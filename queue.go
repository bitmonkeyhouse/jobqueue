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
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin job dispatch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	id, err := insertJob(ctx, tx, command, arguments, options...)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit job dispatch: %w", err)
	}
	return id, nil
}

func (d *QueueDispatcher) DispatchTx(ctx context.Context, tx *sql.Tx, command string, arguments any, options ...Option) (int64, error) {
	if tx == nil {
		return 0, errors.New("job queue transaction is required")
	}
	return insertJob(ctx, tx, command, arguments, options...)
}

// ensureQueueSQL creates a queue with safe generic defaults on first use. It
// never overwrites an explicitly registered queue.
const ensureQueueSQL = `INSERT INTO job_queues (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`

// insertJobSQL is shared by the database/sql and pgx enqueue paths so both
// drivers have identical semantics. xmax=0 identifies a genuinely new row.
const insertJobSQL = `
WITH operation AS (SELECT clock_timestamp() AS now)
INSERT INTO jobs (queue, payload, available_at, retry_until, idempotency_key,
                  sequence_key, sequence_concurrency, max_attempts)
SELECT $1, $2::jsonb,
       operation.now + $3::interval,
       operation.now + $3::interval + $4::interval,
       NULLIF($5, ''),
       NULLIF($6, ''),
       COALESCE(NULLIF($7, 0), q.sequence_concurrency),
       $8
FROM operation
JOIN job_queues q ON q.name = $1
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('available', 'reserved')
DO UPDATE SET updated_at = jobs.updated_at
RETURNING id, (xmax = 0) AS inserted`

// insertJob writes the job and its enqueued event in the caller's transaction.
// An idempotency conflict returns the existing active job's id without a second
// job or event.
func insertJob(ctx context.Context, tx *sql.Tx, command string, arguments any, options ...Option) (int64, error) {
	payload, cfg, err := buildPayload(command, arguments, options...)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, ensureQueueSQL, cfg.queue); err != nil {
		return 0, fmt.Errorf("ensure job queue: %w", err)
	}
	var id int64
	var inserted bool
	err = tx.QueryRowContext(ctx, insertJobSQL, cfg.queue, payload,
		durationInterval(time.Duration(cfg.delayNanos)),
		durationInterval(time.Duration(cfg.retryWindowNanos)),
		cfg.idempotencyKey, cfg.sequenceKey, cfg.sequenceConcurrency, cfg.maxAttempts).Scan(&id, &inserted)
	if err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	if inserted {
		if err := insertEvent(ctx, tx, id, EventEnqueued, map[string]any{"queue": cfg.queue}); err != nil {
			return 0, err
		}
	}
	return id, nil
}

type Job struct {
	ID                  int64
	Queue               string
	Payload             json.RawMessage
	Command             string
	Arguments           json.RawMessage
	Attempts            int
	RetryAttempts       int
	MaxAttempts         int
	AvailableAt         time.Time
	RetryUntil          time.Time
	ReservedAt          time.Time
	ReservationID       string
	WorkerID            string
	SequenceKey         string
	SequenceConcurrency int
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

func claimNext(ctx context.Context, db *sql.DB, queue string, lease time.Duration) (Job, error) {
	return claimNextFor(ctx, db, queue, lease, "")
}

// claimNextFor claims the oldest eligible job for queue. It only steals a
// reserved row whose lease has expired, and only when the queue's expired-lease
// policy is requeue and the attempt cap is not exhausted. A NULL lease counts as
// expired so no row is unreclaimable. workerID is recorded as the owner and the
// claim is granted a fresh lease.
func claimNextFor(ctx context.Context, db *sql.DB, queue string, lease time.Duration, workerID string) (Job, error) {
	reservationID, err := newReservationID()
	if err != nil {
		return Job{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback() }()
	// Serialise claims per queue so the capacity counts below are accurate. The
	// count is cheap and claims are short; per-key locking is the upgrade path if
	// claim throughput on one queue ever matters.
	// ponytail: queue-wide advisory lock; switch to per-sequence-key locks if throughput matters.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, queueLockKey(queue)); err != nil {
		return Job{}, fmt.Errorf("lock job queue: %w", err)
	}
	var job Job
	err = tx.QueryRowContext(ctx, `
WITH candidate AS (
    SELECT j.id
    FROM jobs j
    JOIN job_queues q ON q.name = j.queue
    WHERE j.queue = $1
      AND j.retry_until >= clock_timestamp()
      AND j.cancel_requested_at IS NULL
      AND (j.max_attempts = 0 OR j.attempts < j.max_attempts)
      AND (
        (j.status = 'available' AND j.available_at <= clock_timestamp())
        OR (j.status = 'reserved'
            AND q.expired_lease_policy = 'requeue'
            AND (j.lease_expires_at IS NULL OR j.lease_expires_at < clock_timestamp()))
      )
      AND (
        q.max_concurrency IS NULL
        OR (SELECT count(*) FROM jobs r
            WHERE r.queue = j.queue AND r.status = 'reserved'
              AND r.lease_expires_at >= clock_timestamp()) < q.max_concurrency
      )
      AND (
        j.sequence_key IS NULL
        OR (SELECT count(*) FROM jobs r
            WHERE r.queue = j.queue AND r.sequence_key = j.sequence_key
              AND r.status = 'reserved'
              AND r.lease_expires_at >= clock_timestamp()) < j.sequence_concurrency
      )
    ORDER BY j.available_at ASC, j.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
),
claimed AS (
    UPDATE jobs
    SET status='reserved', attempts=attempts+1, reserved_at=clock_timestamp(),
        reservation_id=$3, worker_id=NULLIF($4, ''),
        heartbeat_at=clock_timestamp(), lease_expires_at=clock_timestamp() + $2::interval,
        updated_at=clock_timestamp()
    FROM candidate
    WHERE jobs.id=candidate.id
    RETURNING jobs.id, jobs.queue, jobs.payload, jobs.attempts, jobs.retry_attempts,
              jobs.max_attempts, jobs.available_at, jobs.retry_until, jobs.reserved_at,
              jobs.reservation_id, COALESCE(jobs.worker_id, '') AS worker_id, jobs.sequence_key,
              jobs.sequence_concurrency
),
attempt AS (
    INSERT INTO job_attempts (job_id, queue, attempt, worker_id, claimed_at)
    SELECT id, queue, attempts, NULLIF($4, ''), reserved_at FROM claimed
),
event AS (
    INSERT INTO job_events (job_id, type, detail)
    SELECT id, 'claimed', jsonb_strip_nulls(
        jsonb_build_object('attempt', attempts, 'worker_id', NULLIF($4, '')))
    FROM claimed
)
SELECT id, queue, payload, attempts, retry_attempts, max_attempts, available_at,
       retry_until, reserved_at, reservation_id, worker_id,
       COALESCE(sequence_key, ''), sequence_concurrency
FROM claimed`, queue, durationInterval(lease), reservationID, workerID).Scan(
		&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.RetryAttempts,
		&job.MaxAttempts, &job.AvailableAt, &job.RetryUntil, &job.ReservedAt,
		&job.ReservationID, &job.WorkerID, &job.SequenceKey, &job.SequenceConcurrency,
	)
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("commit job claim: %w", err)
	}
	if err := decodeJobPayload(&job); err != nil {
		return job, err
	}
	return job, nil
}

func queueLockKey(queue string) string {
	return "jobqueue.queue:" + queue
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status='completed', payload=$3::jsonb, completed_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, last_error=NULL, updated_at=clock_timestamp()
WHERE id=$1 AND status='reserved' AND reservation_id=$2`, job.ID, job.ReservationID, redacted)
	if err := fencedResult(result, err); err != nil {
		return err
	}
	if err := finishAttempt(ctx, tx, job.ID, job.Attempts, OutcomeCompleted, "", ""); err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, job.ID, EventCompleted, attemptDetail(job, "")); err != nil {
		return err
	}
	return tx.Commit()
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
	// MaxAttempts counts execution claims: once the cap is consumed a handler
	// failure is terminal, never retried.
	if job.MaxAttempts > 0 && job.Attempts >= job.MaxAttempts {
		retryAfter = nil
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
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, last_error=$4, updated_at=clock_timestamp()
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
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, last_error=$5, updated_at=operation.now
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
	if err := finishAttempt(ctx, tx, job.ID, job.Attempts, OutcomeFailed, diagnostic.Code, diagnostic.Message); err != nil {
		return err
	}
	eventType := EventRetryScheduled
	if terminal {
		eventType = EventFailed
	}
	if err := insertEvent(ctx, tx, job.ID, eventType, attemptDetail(job, diagnostic.Code)); err != nil {
		return err
	}
	return tx.Commit()
}

// settleCancelled marks a reserved job terminally cancelled. It is fenced by the
// current reservation, so a job that already completed or failed is left alone.
func settleCancelled(ctx context.Context, db *sql.DB, job Job) error {
	redacted, err := redactPayload(job.Payload)
	if err != nil {
		redacted = terminalRedactionFallback(job.Command)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status='cancelled', payload=$3::jsonb, cancelled_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, last_error=NULL, updated_at=clock_timestamp()
WHERE id=$1 AND status='reserved' AND reservation_id=$2`, job.ID, job.ReservationID, redacted)
	if err := fencedResult(result, err); err != nil {
		return err
	}
	if err := finishAttempt(ctx, tx, job.ID, job.Attempts, OutcomeCancelled,
		"job_cancelled", "job was cancelled while running"); err != nil {
		return err
	}
	if err := insertEvent(ctx, tx, job.ID, EventCancelled, attemptDetail(job, "job_cancelled")); err != nil {
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
