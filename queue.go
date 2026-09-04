package jobqueue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrStaleReservation = errors.New("job reservation is no longer current")

type QueueDispatcher struct{ db *sql.DB }

func NewDispatcher(db *sql.DB) *QueueDispatcher { return &QueueDispatcher{db: db} }

func (d *QueueDispatcher) Dispatch(ctx context.Context, command string, arguments any, options ...Option) (int64, error) {
	if d == nil || d.db == nil {
		return 0, errors.New("job queue database is required")
	}
	return d.dispatch(ctx, d.db, command, arguments, options...)
}

func (d *QueueDispatcher) DispatchTx(ctx context.Context, tx *sql.Tx, command string, arguments any, options ...Option) (int64, error) {
	if tx == nil {
		return 0, errors.New("job queue transaction is required")
	}
	return d.dispatch(ctx, tx, command, arguments, options...)
}

func (d *QueueDispatcher) dispatch(ctx context.Context, executor interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, command string, arguments any, options ...Option) (int64, error) {
	payload, cfg, err := buildPayload(command, arguments, options...)
	if err != nil {
		return 0, err
	}
	var id int64
	err = executor.QueryRowContext(ctx, `
WITH operation AS (SELECT clock_timestamp() AS now)
INSERT INTO jobs (queue, payload, available_at, retry_until, idempotency_key)
SELECT $1, $2::jsonb,
       operation.now + $3::interval,
       operation.now + $3::interval + $4::interval,
       NULLIF($5, '')
FROM operation
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('available', 'reserved')
DO UPDATE SET updated_at = jobs.updated_at
RETURNING id`, cfg.queue, payload, durationInterval(time.Duration(cfg.delayNanos)), durationInterval(time.Duration(cfg.retryWindowNanos)), cfg.idempotencyKey).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	return id, nil
}

type Job struct {
	ID            int64
	Queue         string
	Payload       json.RawMessage
	Command       string
	Arguments     json.RawMessage
	Attempts      int
	RetryAttempts int
	AvailableAt   time.Time
	RetryUntil    time.Time
	ReservedAt    time.Time
	ReservationID string
}

func claimExpiredDeadline(ctx context.Context, db *sql.DB, queue string, reservationExpiry time.Duration) (Job, error) {
	reservationID, err := newReservationID()
	if err != nil {
		return Job{}, err
	}
	var job Job
	err = db.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE queue=$1 AND retry_until < clock_timestamp()
      AND (
        status='available'
        OR (status='reserved' AND reserved_at <= clock_timestamp() - $2::interval)
      )
    ORDER BY retry_until ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status='reserved', reserved_at=clock_timestamp(), reservation_id=$3,
    updated_at=clock_timestamp()
FROM candidate
WHERE jobs.id=candidate.id
RETURNING jobs.id,jobs.queue,jobs.payload,jobs.attempts,jobs.retry_attempts,
          jobs.available_at,jobs.retry_until,jobs.reserved_at,jobs.reservation_id`, queue, durationInterval(reservationExpiry), reservationID).Scan(
		&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.RetryAttempts,
		&job.AvailableAt, &job.RetryUntil, &job.ReservedAt, &job.ReservationID,
	)
	if err != nil {
		return Job{}, err
	}
	if err := decodeJobPayload(&job); err != nil {
		return job, err
	}
	return job, nil
}

func claimNext(ctx context.Context, db *sql.DB, queue string, reservationExpiry time.Duration) (Job, error) {
	reservationID, err := newReservationID()
	if err != nil {
		return Job{}, err
	}
	var job Job
	err = db.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE queue = $1
      AND retry_until >= clock_timestamp()
      AND (
        (status = 'available' AND available_at <= clock_timestamp())
        OR (status = 'reserved' AND reserved_at <= clock_timestamp() - $2::interval)
      )
    ORDER BY available_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status='reserved', attempts=attempts+1, reserved_at=clock_timestamp(),
    reservation_id=$3, updated_at=clock_timestamp()
FROM candidate
WHERE jobs.id=candidate.id
RETURNING jobs.id,jobs.queue,jobs.payload,jobs.attempts,jobs.retry_attempts,
          jobs.available_at,jobs.retry_until,jobs.reserved_at,jobs.reservation_id`, queue, durationInterval(reservationExpiry), reservationID).Scan(
		&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.RetryAttempts,
		&job.AvailableAt, &job.RetryUntil, &job.ReservedAt, &job.ReservationID,
	)
	if err != nil {
		return Job{}, err
	}
	if err := decodeJobPayload(&job); err != nil {
		return job, err
	}
	return job, nil
}

func decodeJobPayload(job *Job) error {
	job.Command = safelyExtractCommand(job.Payload)
	if err := validatePayload(job.Payload); err != nil {
		return fmt.Errorf("validate claimed job payload: %w", err)
	}
	var payload Payload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode claimed job payload: %w", err)
	}
	job.Command, job.Arguments = payload.Command, payload.Arguments
	return nil
}

func safelyExtractCommand(data []byte) string {
	var document map[string]json.RawMessage
	if json.Unmarshal(data, &document) != nil {
		return ""
	}
	var command string
	if json.Unmarshal(document["command"], &command) != nil {
		return ""
	}
	return strings.TrimSpace(command)
}

func completeJob(ctx context.Context, db *sql.DB, job Job) error {
	redacted, err := redactPayload(job.Payload)
	if err != nil {
		// The side effect already succeeded. Conservatively remove all arguments
		// rather than retrying it because legacy payload redaction failed.
		redacted = terminalRedactionFallback(job.Command)
	}
	result, err := db.ExecContext(ctx, `
UPDATE jobs
SET status='completed', payload=$3::jsonb, completed_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, last_error=NULL, updated_at=clock_timestamp()
WHERE id=$1 AND status='reserved' AND reservation_id=$2`, job.ID, job.ReservationID, redacted)
	return fencedResult(result, err)
}

func settleFailure(ctx context.Context, db *sql.DB, job Job, diagnostic failureDiagnostic, retryAfter *time.Duration) error {
	if db == nil {
		return errors.New("job queue database is required")
	}
	command := strings.TrimSpace(job.Command)
	if command == "" {
		command = "invalid"
	}
	redacted, err := redactPayload(job.Payload)
	if err != nil {
		redacted = terminalRedactionFallback(command)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var terminal bool
	var retryAttempts int
	if retryAfter == nil {
		err = tx.QueryRowContext(ctx, `
UPDATE jobs
SET status='failed', payload=$3::jsonb, failed_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, last_error=$4, updated_at=clock_timestamp()
WHERE id=$1 AND status='reserved' AND reservation_id=$2
RETURNING true,retry_attempts`, job.ID, job.ReservationID, redacted, diagnostic.summary()).Scan(&terminal, &retryAttempts)
	} else {
		err = tx.QueryRowContext(ctx, `
WITH operation AS (SELECT clock_timestamp() AS now)
UPDATE jobs
SET status=CASE WHEN operation.now+$3::interval <= retry_until THEN 'available' ELSE 'failed' END,
    payload=CASE WHEN operation.now+$3::interval <= retry_until THEN payload ELSE $4::jsonb END,
    available_at=CASE WHEN operation.now+$3::interval <= retry_until THEN operation.now+$3::interval ELSE available_at END,
    retry_attempts=retry_attempts+CASE WHEN operation.now+$3::interval <= retry_until THEN 1 ELSE 0 END,
    failed_at=CASE WHEN operation.now+$3::interval <= retry_until THEN NULL ELSE operation.now END,
    reserved_at=NULL, reservation_id=NULL, last_error=$5, updated_at=operation.now
FROM operation
WHERE jobs.id=$1 AND status='reserved' AND reservation_id=$2
RETURNING status='failed',retry_attempts`, job.ID, job.ReservationID, durationInterval(*retryAfter), redacted, diagnostic.summary()).Scan(&terminal, &retryAttempts)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleReservation
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO job_failures (job_id,queue,command,attempts,retry_attempts,error_code,error_message,terminal)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, job.ID, job.Queue, command, job.Attempts, retryAttempts, diagnostic.Code, diagnostic.Message, terminal); err != nil {
		return err
	}
	return tx.Commit()
}

// releaseJob and retryOrFailJob are narrow internal helpers retained for
// boundary-level tests; production worker settlement uses settleFailure.
func releaseJob(ctx context.Context, db *sql.DB, job Job, backoff time.Duration, message string) error {
	diagnostic := failureDiagnostic{Code: "handler_failed", Message: truncateFailureMessage(message)}
	return settleFailure(ctx, db, job, diagnostic, &backoff)
}

func retryOrFailJob(ctx context.Context, db *sql.DB, job Job, backoff time.Duration, message string) error {
	return releaseJob(ctx, db, job, backoff, message)
}

func fencedResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrStaleReservation
	}
	return nil
}

func nextAvailableDelay(ctx context.Context, db *sql.DB, queue string, fallback time.Duration) time.Duration {
	var seconds sql.NullFloat64
	if err := db.QueryRowContext(ctx, `
SELECT EXTRACT(EPOCH FROM (MIN(available_at)-clock_timestamp()))
FROM jobs WHERE queue=$1 AND status='available'`, queue).Scan(&seconds); err != nil || !seconds.Valid {
		return fallback
	}
	delay := time.Duration(seconds.Float64 * float64(time.Second))
	if delay < 0 {
		return 0
	}
	if delay > fallback {
		return fallback
	}
	return delay
}

func durationInterval(duration time.Duration) string {
	return fmt.Sprintf("%f seconds", duration.Seconds())
}

func newReservationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create reservation id: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func terminalRedactionFallback(command string) []byte {
	command = strings.TrimSpace(command)
	if command == "" {
		command = "invalid"
	}
	data, _ := json.Marshal(map[string]any{
		"command": command, "arguments": nil, "sensitive": []string{}, "redacted": true,
	})
	return data
}
