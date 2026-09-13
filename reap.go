package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ReapOptions bounds a reconciliation pass. Zero values mean: 100 jobs, all
// queues.
type ReapOptions struct {
	Limit  int
	Queues []string
}

// ReapResult reports how many expired leases were failed terminally and how many
// were returned to the available pool.
type ReapResult struct {
	Failed    int
	Reclaimed int
}

// Reap reconciles jobs whose owner stopped renewing its lease. It is the
// deterministic, observable counterpart to claim-time stealing: an expired lease
// is either failed terminally (queue policy fail, or the attempt cap is already
// exhausted) or returned to available (policy requeue). The stale owner's
// settlement is fenced by reservation_id and cannot win afterwards.
func Reap(ctx context.Context, db *sql.DB, options ReapOptions) (ReapResult, error) {
	if db == nil {
		return ReapResult{}, errors.New("job queue database is required")
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 100
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ReapResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	query := `
SELECT j.id, j.queue, j.payload, j.attempts, j.retry_attempts, j.max_attempts,
       j.reservation_id, COALESCE(j.worker_id, ''), q.expired_lease_policy
FROM jobs j
JOIN job_queues q ON q.name = j.queue
WHERE j.status = 'reserved'
  AND (j.lease_expires_at IS NULL OR j.lease_expires_at < clock_timestamp())`
	args := []any{limit}
	if len(options.Queues) > 0 {
		query += ` AND j.queue = ANY($2)`
		args = append(args, options.Queues)
	}
	query += ` ORDER BY j.lease_expires_at NULLS FIRST, j.id FOR UPDATE OF j SKIP LOCKED LIMIT $1`

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return ReapResult{}, fmt.Errorf("select expired leases: %w", err)
	}
	type expiredLease struct {
		job     Job
		policy  ExpiredLeasePolicy
		command string
	}
	var expired []expiredLease
	for rows.Next() {
		var lease expiredLease
		if err := rows.Scan(&lease.job.ID, &lease.job.Queue, &lease.job.Payload,
			&lease.job.Attempts, &lease.job.RetryAttempts, &lease.job.MaxAttempts,
			&lease.job.ReservationID, &lease.job.WorkerID, &lease.policy); err != nil {
			_ = rows.Close()
			return ReapResult{}, fmt.Errorf("scan expired lease: %w", err)
		}
		lease.command = safelyExtractCommand(lease.job.Payload)
		expired = append(expired, lease)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return ReapResult{}, err
	}
	if err := rows.Close(); err != nil {
		return ReapResult{}, err
	}

	var result ReapResult
	for _, lease := range expired {
		exhausted := lease.job.MaxAttempts > 0 && lease.job.Attempts >= lease.job.MaxAttempts
		terminal := exhausted || lease.policy == ExpiredLeaseFail
		if terminal {
			if err := reapFail(ctx, tx, lease.job, lease.command); err != nil {
				return ReapResult{}, err
			}
			result.Failed++
			continue
		}
		if err := reapRequeue(ctx, tx, lease.job); err != nil {
			return ReapResult{}, err
		}
		result.Reclaimed++
	}
	if err := tx.Commit(); err != nil {
		return ReapResult{}, fmt.Errorf("commit reap: %w", err)
	}
	return result, nil
}

func reapFail(ctx context.Context, tx *sql.Tx, job Job, command string) error {
	if command == "" {
		command = "invalid"
	}
	redacted, err := redactPayload(job.Payload)
	if err != nil {
		redacted = terminalRedactionFallback(command)
	}
	message := "job lease expired before the owner renewed it"
	if job.MaxAttempts > 0 && job.Attempts >= job.MaxAttempts {
		message = "job lease expired and its attempt cap was already consumed"
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status='failed', payload=$2::jsonb, failed_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, last_error=$3, updated_at=clock_timestamp()
WHERE id=$1`, job.ID, redacted, "lease_expired: "+message); err != nil {
		return fmt.Errorf("fail expired lease: %w", err)
	}
	if err := finishAttempt(ctx, tx, job.ID, job.Attempts, OutcomeLeaseExpired, "lease_expired", message); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO job_failures (job_id, queue, command, attempts, retry_attempts, error_code, error_message, terminal)
VALUES ($1,$2,$3,$4,$5,'lease_expired',$6,true)`,
		job.ID, job.Queue, command, job.Attempts, job.RetryAttempts, message); err != nil {
		return fmt.Errorf("record expired lease failure: %w", err)
	}
	return insertEvent(ctx, tx, job.ID, EventFailed, map[string]any{
		"attempt":    job.Attempts,
		"worker_id":  job.WorkerID,
		"error_code": "lease_expired",
	})
}

func reapRequeue(ctx context.Context, tx *sql.Tx, job Job) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status='available', available_at=clock_timestamp(),
    reserved_at=NULL, reservation_id=NULL, worker_id=NULL, heartbeat_at=NULL,
    lease_expires_at=NULL, updated_at=clock_timestamp()
WHERE id=$1`, job.ID); err != nil {
		return fmt.Errorf("requeue expired lease: %w", err)
	}
	if err := finishAttempt(ctx, tx, job.ID, job.Attempts, OutcomeLeaseExpired,
		"lease_expired", "job lease expired before the owner renewed it"); err != nil {
		return err
	}
	return insertEvent(ctx, tx, job.ID, EventReclaimed, map[string]any{
		"attempt":   job.Attempts,
		"worker_id": job.WorkerID,
		"policy":    string(ExpiredLeaseRequeue),
	})
}
