package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RecommendedRetention is the documented starting point for disposable jobqueue
// operational history (terminal jobs and their attempts, events and failures).
// It is guidance only: the caller invokes Prune, may choose longer or shorter,
// and the module never deletes anything on its own. It does not apply to a
// consumer's domain records.
const RecommendedRetention = 30 * 24 * time.Hour

// PruneOptions selects terminal jobs to delete. TerminalBefore is required.
type PruneOptions struct {
	TerminalBefore time.Time
	Queues         []string
	Limit          int
}

// Prune deletes terminal jobs (completed, failed or cancelled) whose last update
// predates the cutoff, and cascades their job-owned history: job_attempts,
// job_events and job_failures. It never deletes a non-terminal job and never
// leaves orphan history. Retention is a caller policy; no automatic pruning
// happens.
func Prune(ctx context.Context, db *sql.DB, options PruneOptions) (int, error) {
	if db == nil {
		return 0, errors.New("job queue database is required")
	}
	if options.TerminalBefore.IsZero() {
		return 0, errors.New("prune cutoff time is required")
	}
	limit := options.Limit
	if limit <= 0 {
		limit = 1000
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	query := `
DELETE FROM jobs
WHERE id IN (
    SELECT id FROM jobs
    WHERE status IN ('completed', 'failed', 'cancelled')
      AND updated_at < $2`
	args := []any{limit, options.TerminalBefore}
	if len(options.Queues) > 0 {
		query += ` AND queue = ANY($3)`
		args = append(args, options.Queues)
	}
	query += `
    ORDER BY updated_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)`
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("prune terminal jobs: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune terminal jobs rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit prune: %w", err)
	}
	return int(deleted), nil
}

// Prune is the dispatcher form of the package-level Prune.
func (d *QueueDispatcher) Prune(ctx context.Context, options PruneOptions) (int, error) {
	if d == nil || d.db == nil {
		return 0, errors.New("job queue database is required")
	}
	return Prune(ctx, d.db, options)
}
