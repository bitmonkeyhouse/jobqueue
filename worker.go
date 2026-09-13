package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

// defaultWorkerIdentity is generated once per process: hostname, pid and a
// random suffix, so an operator can tell which process holds a job.
var defaultWorkerIdentity = sync.OnceValue(func() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}
	suffix, err := newReservationID()
	if err != nil {
		suffix = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), suffix)
})

type Worker struct {
	DB                *sql.DB
	DatabaseURL       string
	Registry          *Registry
	Queue             string
	NotifyChannel     string
	Concurrency       int
	JobTimeout        time.Duration
	ReservationExpiry time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	WorkerID          string
	PollInterval      time.Duration
	ShutdownGrace     time.Duration
	SettlementTimeout time.Duration
	Logger            *slog.Logger

	cancelMu   sync.Mutex
	cancelWake chan struct{}
}

// leaseWatch records what happened to a running job's lease so settlement can
// distinguish cancellation from lease loss and unclean shutdown.
type leaseWatch struct {
	cancelRequested atomic.Bool
	leaseLost       atomic.Bool
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
	w.startCancelListener(consumerCtx)
	go w.runReaper(consumerCtx)
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
	job, err := claimNextFor(lifecycleCtx, w.DB, w.queue(), w.leaseDuration(), w.workerID())
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
	var (
		handlerErr error
		watch      *leaseWatch
	)
	if !ok {
		handlerErr = Permanent(Diagnostic("unknown_command", "no handler is registered for the job command", errors.New("unknown job command")))
	} else {
		leaseCtx, leaseWatch, stopHeartbeat := w.startHeartbeat(handlerCtx, job)
		watch = leaseWatch
		jobCtx, cancel := context.WithTimeout(leaseCtx, w.jobTimeout())
		handlerErr = handler(jobCtx, job.Arguments)
		if handlerErr == nil {
			handlerErr = jobCtx.Err()
		}
		cancel()
		stopHeartbeat()
	}
	settleCtx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
	defer cancel()
	if handlerErr == nil {
		return true, w.settle(completeJob(settleCtx, w.DB, job), job)
	}
	if watch != nil && watch.cancelRequested.Load() {
		w.logger().Info("job cancellation propagated to handler", "job_id", job.ID, "queue", job.Queue)
		return true, w.settle(settleCancelled(settleCtx, w.DB, job), job)
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
	if w.LeaseDuration > 0 && w.ReservationExpiry > 0 {
		return errors.New("set LeaseDuration or the deprecated ReservationExpiry, not both")
	}
	if w.heartbeatInterval() >= w.leaseDuration() {
		return errors.New("job heartbeat interval must be shorter than the lease duration")
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
	// Deprecated alias: reservation expiry is now the initial lease duration.
	return w.leaseDuration()
}

func (w *Worker) leaseDuration() time.Duration {
	if w.LeaseDuration > 0 {
		return w.LeaseDuration
	}
	if w.ReservationExpiry > 0 {
		return w.ReservationExpiry
	}
	return time.Minute
}

func (w *Worker) heartbeatInterval() time.Duration {
	if w.HeartbeatInterval > 0 {
		return w.HeartbeatInterval
	}
	return 10 * time.Second
}

func (w *Worker) workerID() string {
	if id := strings.TrimSpace(w.WorkerID); id != "" {
		return id
	}
	return defaultWorkerIdentity()
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

// startHeartbeat renews the claimed job's lease on an interval until the
// returned stop func is called, and wakes immediately when a cancellation
// notification arrives. It reports cancellation and lease loss through the
// returned leaseWatch and cancels the returned context so the handler stops.
// Transient database errors are retried.
func (w *Worker) startHeartbeat(parent context.Context, job Job) (context.Context, *leaseWatch, func()) {
	ctx, cancel := context.WithCancel(parent)
	watch := &leaseWatch{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.heartbeatInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.cancelSignal():
				if w.heartbeatOnce(ctx, job, watch) {
					cancel()
					return
				}
			case <-ticker.C:
				if w.heartbeatOnce(ctx, job, watch) {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, watch, func() {
		cancel()
		<-done
	}
}

// heartbeatOnce renews the lease and reports whether the handler should stop
// (lease lost, or cancellation requested). Transient errors are logged and
// treated as "keep going".
func (w *Worker) heartbeatOnce(ctx context.Context, job Job, watch *leaseWatch) bool {
	cancelRequested, err := w.renewLease(ctx, job)
	if err == nil {
		if cancelRequested {
			watch.cancelRequested.Store(true)
			w.logger().Info("job cancellation requested; cancelling handler",
				"job_id", job.ID, "queue", job.Queue)
			return true
		}
		return false
	}
	if !errors.Is(err, ErrStaleReservation) {
		w.logger().Warn("job lease renewal failed", "job_id", job.ID, "queue", job.Queue, "error", err)
		return false
	}
	watch.leaseLost.Store(true)
	w.logger().Warn("job lease lost; cancelling handler",
		"job_id", job.ID, "queue", job.Queue, "worker_id", w.workerID())
	w.recordLeaseLost(job)
	return true
}

func (w *Worker) renewLease(ctx context.Context, job Job) (bool, error) {
	var cancelRequested bool
	err := w.DB.QueryRowContext(ctx, `
UPDATE jobs
SET lease_expires_at=clock_timestamp() + $3::interval,
    heartbeat_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE id=$1 AND status='reserved' AND reservation_id=$2 AND worker_id=$4
RETURNING cancel_requested_at IS NOT NULL`,
		job.ID, job.ReservationID, durationInterval(w.leaseDuration()), w.workerID()).Scan(&cancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrStaleReservation
	}
	if err != nil {
		return false, err
	}
	if _, err := w.DB.ExecContext(ctx, `
UPDATE job_attempts SET heartbeat_at=clock_timestamp()
WHERE job_id=$1 AND attempt=$2 AND outcome IS NULL`, job.ID, job.Attempts); err != nil {
		return cancelRequested, err
	}
	if err := insertEvent(ctx, w.DB, job.ID, EventLeaseRenewed, map[string]any{
		"attempt":   job.Attempts,
		"worker_id": w.workerID(),
	}); err != nil {
		return cancelRequested, err
	}
	return cancelRequested, nil
}

// cancelSignal returns the current broadcast channel. Closing it wakes every
// waiting heartbeat; broadcastCancel then installs a fresh channel.
func (w *Worker) cancelSignal() <-chan struct{} {
	w.cancelMu.Lock()
	defer w.cancelMu.Unlock()
	if w.cancelWake == nil {
		w.cancelWake = make(chan struct{})
	}
	return w.cancelWake
}

func (w *Worker) broadcastCancel() {
	w.cancelMu.Lock()
	defer w.cancelMu.Unlock()
	if w.cancelWake != nil {
		close(w.cancelWake)
	}
	w.cancelWake = make(chan struct{})
}

func (w *Worker) recordLeaseLost(job Job) {
	ctx, cancel := context.WithTimeout(context.Background(), w.settlementTimeout())
	defer cancel()
	if err := insertEvent(ctx, w.DB, job.ID, EventLeaseLost, map[string]any{
		"attempt":   job.Attempts,
		"worker_id": w.workerID(),
	}); err != nil {
		w.logger().Warn("record job lease loss failed", "job_id", job.ID, "queue", job.Queue, "error", err)
	}
}

// startCancelListener LISTENs on the library-owned cancellation channel and
// broadcasts a wake to every running heartbeat. Cancellation state is durable,
// so this only reduces latency; a missed notification is caught on the next
// heartbeat or by Reap.
func (w *Worker) startCancelListener(ctx context.Context) {
	dsn := strings.TrimSpace(w.DatabaseURL)
	if dsn == "" {
		return
	}
	notifications := make(chan string, 1)
	go w.runListener(ctx, dsn, DefaultCancelChannel, notifications)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-notifications:
				if !ok || notificationRequiresWake(payload, w.queue()) {
					w.broadcastCancel()
				}
			}
		}
	}()
}

// runReaper periodically reconciles expired leases for this worker's queue so
// an abandoned job is handled by policy even when no worker claims again.
func (w *Worker) runReaper(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := Reap(ctx, w.DB, ReapOptions{Queues: []string{w.queue()}}); err != nil && ctx.Err() == nil {
				w.logger().Warn("job queue reap failed", "queue", w.queue(), "error", err)
			}
		}
	}
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
