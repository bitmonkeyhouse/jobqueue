package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PgxTx is the narrow transaction surface EnqueueTx needs. It is satisfied by
// pgx.Tx and by *pgxpool.Pool, so a pgx consumer can enqueue inside its own
// transaction or directly against its pool. There is no untyped any and no
// runtime method discovery.
type PgxTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

// EnqueueTx enqueues a job from a pgx transaction or pool using exactly the same
// payload construction, insert SQL, idempotency handling and enqueued event as
// Dispatch/DispatchTx. The job and its event are written in the caller's
// transaction; a rollback removes both. It does not require a *sql.DB.
func EnqueueTx(ctx context.Context, tx PgxTx, command string, arguments any, options ...Option) (int64, error) {
	if tx == nil {
		return 0, errors.New("job queue transaction is required")
	}
	payload, cfg, err := buildPayload(command, arguments, options...)
	if err != nil {
		return 0, err
	}
	var id int64
	var inserted bool
	if err := tx.QueryRow(ctx, insertJobSQL, cfg.queue, payload,
		durationInterval(time.Duration(cfg.delayNanos)),
		durationInterval(time.Duration(cfg.retryWindowNanos)),
		cfg.idempotencyKey).Scan(&id, &inserted); err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	if inserted {
		detail, err := encodeEventDetail(map[string]any{"queue": cfg.queue})
		if err != nil {
			return 0, fmt.Errorf("encode job event detail: %w", err)
		}
		if _, err := tx.Exec(ctx, insertEventSQL, id, string(EventEnqueued), detail); err != nil {
			return 0, fmt.Errorf("insert job event %s: %w", EventEnqueued, err)
		}
	}
	return id, nil
}
