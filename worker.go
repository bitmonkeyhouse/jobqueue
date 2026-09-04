package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Handler func(context.Context, []byte) error

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e == nil || e.Err == nil {
		return "permanent job failure"
	}
	return e.Err.Error()
}
func (e *PermanentError) Unwrap() error { return e.Err }
func Permanent(err error) error         { return &PermanentError{Err: err} }

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: make(map[string]Handler)} }
func (r *Registry) Register(command string, handler Handler) error {
	command = strings.TrimSpace(command)
	if command == "" || handler == nil {
		return errors.New("job command and handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[command]; exists {
		return fmt.Errorf("job handler %q is already registered", command)
	}
	r.handlers[command] = handler
	return nil
}
func (r *Registry) Handle(ctx context.Context, command string, arguments []byte) error {
	handler, ok := r.handler(command)
	if !ok {
		return Permanent(Diagnostic("unknown_command", "no handler is registered for the job command", errors.New("unknown job command")))
	}
	return handler(ctx, arguments)
}
func (r *Registry) handler(command string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[command]
	return handler, ok
}

// DefaultNotifyChannel is the Postgres NOTIFY channel both the migration
// trigger and the worker listener use by default.
const DefaultNotifyChannel = "jobs"

type Worker struct {
	DB                *sql.DB
	DatabaseURL       string
	Registry          *Registry
	Queue             string
	NotifyChannel     string
	Concurrency       int
	JobTimeout        time.Duration
	ReservationExpiry time.Duration
	PollInterval      time.Duration
	ShutdownGrace     time.Duration
	SettlementTimeout time.Duration
	Logger            *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()
	wakes := make([]chan struct{}, w.concurrency())
	errCh := make(chan error, len(wakes))
	var wg sync.WaitGroup
	for i := range wakes {
		wakes[i] = make(chan struct{}, 1)
		wg.Add(1)
		go func(wake <-chan struct{}) {
			defer wg.Done()
			if err := w.runConsumer(consumerCtx, ctx, wake); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(wakes[i])
	}

	notifications := w.startListener(consumerCtx)
	broadcastWake(wakes)

	var runErr error
runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case err := <-errCh:
			runErr = err
			break runLoop
		case payload, ok := <-notifications:
			if !ok || notificationRequiresWake(payload, w.queue()) {
				// A reconnect or matching queue notification means work may
				// have arrived while this worker was between polls.
				broadcastWake(wakes)
			}
		}
	}

	// Root cancellation stops new claims. Existing handler contexts are allowed
	// to finish up to the bounded grace period before they are canceled.
	grace := time.NewTimer(w.shutdownGrace())
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		grace.Stop()
	case <-grace.C:
		stopConsumers()
		select {
		case <-done:
		case <-time.After(w.settlementTimeout()):
			if runErr == nil {
				runErr = errors.New("job queue shutdown timed out")
			}
		}
	}
	return runErr
}

// startListener returns a channel of wake payloads, or nil when no DatabaseURL
// is configured (polling only). The goroutine lives until ctx is canceled.
func (w *Worker) startListener(ctx context.Context) <-chan string {
	if strings.TrimSpace(w.DatabaseURL) == "" {
		return nil
	}
	notifications := make(chan string, 1)
	go w.runListener(ctx, strings.TrimSpace(w.DatabaseURL), w.notifyChannel(), notifications)
	return notifications
}

func (w *Worker) runListener(ctx context.Context, dsn, channel string, out chan<- string) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.listenOnce(ctx, dsn, channel, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger().Warn("job queue LISTEN unavailable; retrying", "channel", channel, "error", err)
		}
		if !sleepContext(ctx, listenerReconnectDelay) {
			return
		}
	}
}

const listenerReconnectDelay = time.Second

func (w *Worker) listenOnce(ctx context.Context, dsn, channel string, out chan<- string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	// Rescan on (re)connect because notifications may have been missed while
	// disconnected, mirroring lib/pq's nil-after-reconnect behavior.
	sendNotification(out, "")
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		sendNotification(out, notification.Payload)
	}
}

func sendNotification(out chan<- string, payload string) {
	select {
	case out <- payload:
	default:
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func notificationRequiresWake(payload, queue string) bool {
	return payload == "" || payload == queue
}

func (w *Worker) ProcessNext(ctx context.Context) (bool, error) {
	return w.processNext(ctx, ctx)
}

func (w *Worker) processNext(handlerCtx, lifecycleCtx context.Context) (bool, error) {
	if err := w.validate(); err != nil {
		return false, err
	}
	expired, err := claimExpiredDeadline(lifecycleCtx, w.DB, w.queue(), w.reservationExpiry())
	if err == nil {
		settleCtx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
		defer cancel()
		diagnostic := failureDiagnostic{Code: "retry_window_expired", Message: "job retry window expired before another attempt"}
		return true, w.settle(settleFailure(settleCtx, w.DB, expired, diagnostic, nil), expired)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		if expired.ID == 0 {
			return false, err
		}
		w.logFailure(expired, err)
		settleCtx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
		defer cancel()
		diagnostic := failureDiagnostic{Code: "invalid_payload", Message: "job payload could not be decoded"}
		return true, w.settle(settleFailure(settleCtx, w.DB, expired, diagnostic, nil), expired)
	}
	job, err := claimNext(lifecycleCtx, w.DB, w.queue(), w.reservationExpiry())
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		if job.ID == 0 {
			return false, err
		}
		w.logFailure(job, err)
		settleCtx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
		defer cancel()
		diagnostic := failureDiagnostic{Code: "invalid_payload", Message: "job payload could not be decoded"}
		return true, w.settle(settleFailure(settleCtx, w.DB, job, diagnostic, nil), job)
	}

	handler, ok := w.Registry.handler(job.Command)
	var handlerErr error
	if !ok {
		handlerErr = Permanent(Diagnostic("unknown_command", "no handler is registered for the job command", errors.New("unknown job command")))
	} else {
		jobCtx, cancel := context.WithTimeout(handlerCtx, w.jobTimeout())
		handlerErr = handler(jobCtx, job.Arguments)
		if handlerErr == nil {
			handlerErr = jobCtx.Err()
		}
		cancel()
	}
	settleCtx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
	defer cancel()
	if handlerErr == nil {
		return true, w.settle(completeJob(settleCtx, w.DB, job), job)
	}
	w.logFailure(job, handlerErr)
	diagnostic := diagnosticForError(handlerErr)
	var permanent *PermanentError
	if errors.As(handlerErr, &permanent) {
		return true, w.settle(settleFailure(settleCtx, w.DB, job, diagnostic, nil), job)
	}
	backoff := retryBackoff(job.RetryAttempts + 1)
	return true, w.settle(settleFailure(settleCtx, w.DB, job, diagnostic, &backoff), job)
}

func (w *Worker) logFailure(job Job, err error) {
	w.logger().Error("job attempt failed",
		"job_id", job.ID,
		"queue", job.Queue,
		"command", job.Command,
		"attempt", job.Attempts,
		"retry_attempt", job.RetryAttempts,
		"error", err,
	)
}

func (w *Worker) settle(err error, job Job) error {
	if errors.Is(err, ErrStaleReservation) {
		w.logger().Info("job settlement superseded by a newer reservation", "job_id", job.ID, "queue", job.Queue)
		return nil
	}
	return err
}

func (w *Worker) runConsumer(consumerCtx, rootCtx context.Context, wake <-chan struct{}) error {
	for {
		if rootCtx.Err() != nil {
			return nil
		}
		processed, err := w.processNext(consumerCtx, consumerCtx)
		if err != nil {
			if consumerCtx.Err() != nil {
				return nil
			}
			return err
		}
		if processed {
			continue
		}
		delay := nextAvailableDelay(consumerCtx, w.DB, w.queue(), w.pollInterval())
		timer := time.NewTimer(delay)
		select {
		case <-rootCtx.Done():
			timer.Stop()
			return nil
		case <-consumerCtx.Done():
			timer.Stop()
			return nil
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func broadcastWake(wakes []chan struct{}) {
	for _, wake := range wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (w *Worker) validate() error {
	if w == nil || w.DB == nil {
		return errors.New("job queue database is required")
	}
	if w.Registry == nil {
		return errors.New("job handler registry is required")
	}
	if w.jobTimeout() >= w.reservationExpiry() {
		return errors.New("job timeout must be shorter than reservation expiry")
	}
	return nil
}
func (w *Worker) queue() string {
	if strings.TrimSpace(w.Queue) == "" {
		return DefaultQueue
	}
	return strings.TrimSpace(w.Queue)
}
func (w *Worker) notifyChannel() string {
	if strings.TrimSpace(w.NotifyChannel) == "" {
		return DefaultNotifyChannel
	}
	return strings.TrimSpace(w.NotifyChannel)
}
func (w *Worker) concurrency() int {
	if w.Concurrency < 1 {
		return 2
	}
	return w.Concurrency
}
func (w *Worker) jobTimeout() time.Duration {
	if w.JobTimeout <= 0 {
		return 30 * time.Second
	}
	return w.JobTimeout
}
func (w *Worker) reservationExpiry() time.Duration {
	if w.ReservationExpiry <= 0 {
		return 90 * time.Second
	}
	return w.ReservationExpiry
}
func (w *Worker) pollInterval() time.Duration {
	if w.PollInterval <= 0 {
		return 15 * time.Second
	}
	return w.PollInterval
}
func (w *Worker) shutdownGrace() time.Duration {
	if w.ShutdownGrace <= 0 {
		return 35 * time.Second
	}
	return w.ShutdownGrace
}
func (w *Worker) settlementTimeout() time.Duration {
	if w.SettlementTimeout <= 0 {
		return 5 * time.Second
	}
	return w.SettlementTimeout
}
func (w *Worker) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}

func retryBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return time.Minute
	}
	// Saturate before shifting would overflow time.Duration. The retry deadline
	// will make such a delay terminal rather than allowing an overflowed retry.
	const maxDuration = time.Duration(1<<63 - 1)
	shift := attempt - 1
	if shift >= 63 || time.Minute > maxDuration>>shift {
		return maxDuration
	}
	return time.Minute << shift
}
