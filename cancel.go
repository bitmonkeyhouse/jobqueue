package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// DefaultCancelChannel is the library-owned NOTIFY channel used to reduce
// cancellation latency. Cancellation state is durable in PostgreSQL; the
// notification is only an optimisation, and a missed notification is recovered
// by the worker's periodic lease/heartbeat check or by Reap.
const DefaultCancelChannel = "jobqueue_cancel"

// ErrJobNotFound reports a cancellation request for an unknown job id.
var ErrJobNotFound = errors.New("job not found")

// CancellationResult says what a cancellation request did.
type CancellationResult string

const (
	// CancelCancelled means the job was still queued and is now terminally
	// cancelled.
	CancelCancelled CancellationResult = "cancelled"
	// CancelRequested means the job was running; it is marked for cancellation
	// and its owner will cancel the handler context.
	CancelRequested CancellationResult = "requested"
	// CancelAlreadyRequested means the running job already had a pending request.
	CancelAlreadyRequested CancellationResult = "already_requested"
	// CancelAlreadyTerminal means the job had already reached a terminal state;
	// its historical result was not rewritten.
	CancelAlreadyTerminal CancellationResult = "already_terminal"
)

// RequestCancel asks the queue to cancel a job. A queued job is cancelled
// immediately; a running job is marked for cancellation and its owner cancels
// the handler context. A terminal job is never rewritten. Whether the caller was
// allowed to ask is the application's business; this module does no
// authorization.
func (d *QueueDispatcher) RequestCancel(ctx context.Context, jobID int64) (CancellationResult, error) {
	if d == nil || d.db == nil {
		return "", errors.New("job queue database is required")
	}
	return requestCancel(ctx, d.db, jobID)
}

// RequestCancel is the package-level form for callers with a *sql.DB.
func RequestCancel(ctx context.Context, db *sql.DB, jobID int64) (CancellationResult, error) {
	return requestCancel(ctx, db, jobID)
}

func requestCancel(ctx context.Context, db *sql.DB, jobID int64) (CancellationResult, error) {
	if db == nil {
		return "", errors.New("job queue database is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	var status, queue string
	var cancelRequested bool
	err = tx.QueryRowContext(ctx, `
SELECT status, queue, cancel_requested_at IS NOT NULL
FROM jobs WHERE id=$1 FOR UPDATE`, jobID).Scan(&status, &queue, &cancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrJobNotFound
	}
	if err != nil {
		return "", fmt.Errorf("load job for cancellation: %w", err)
	}

	switch status {
	case "available":
		// Queued (including in backoff): terminal cancelled immediately.
		if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET status='cancelled', cancel_requested_at=clock_timestamp(),
    cancelled_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE id=$1`, jobID); err != nil {
			return "", fmt.Errorf("cancel queued job: %w", err)
		}
		if err := insertEvent(ctx, tx, jobID, EventCancelRequested, map[string]any{"from": "available"}); err != nil {
			return "", err
		}
		if err := insertEvent(ctx, tx, jobID, EventCancelled, map[string]any{"from": "available"}); err != nil {
			return "", err
		}
		if err := notifyCancel(ctx, tx, queue); err != nil {
			return "", err
		}
		if err := tx.Commit(); err != nil {
			return "", fmt.Errorf("commit queued cancellation: %w", err)
		}
		return CancelCancelled, nil

	case "reserved":
		if cancelRequested {
			return CancelAlreadyRequested, nil
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE jobs
SET cancel_requested_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE id=$1`, jobID); err != nil {
			return "", fmt.Errorf("request running job cancellation: %w", err)
		}
		if err := insertEvent(ctx, tx, jobID, EventCancelRequested, map[string]any{"from": "reserved"}); err != nil {
			return "", err
		}
		if err := notifyCancel(ctx, tx, queue); err != nil {
			return "", err
		}
		if err := tx.Commit(); err != nil {
			return "", fmt.Errorf("commit cancellation request: %w", err)
		}
		return CancelRequested, nil

	default:
		// completed, failed or cancelled: never retroactive.
		return CancelAlreadyTerminal, nil
	}
}

func notifyCancel(ctx context.Context, tx *sql.Tx, queue string) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_notify($1, $2)`, DefaultCancelChannel, queue); err != nil {
		return fmt.Errorf("notify cancellation: %w", err)
	}
	return nil
}
