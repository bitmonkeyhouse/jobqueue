package jobqueue

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPgxPool returns a pgx pool bound to a freshly migrated isolated schema,
// plus the *sql.DB for independent verification queries.
func testPgxPool(t *testing.T) (*pgxpool.Pool, *sql.DB) {
	t.Helper()
	db, dsn := openIsolatedSchemaWithDSN(t, "test_jobqueue_pgx_")
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate isolated schema: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Exec(`CREATE TABLE domain_rows (id bigserial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create domain table: %v", err)
	}
	return pool, db
}

func TestEnqueueTxCommitsWithDomainRow(t *testing.T) {
	pool, db := testPgxPool(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO domain_rows (name) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	}
	id, err := EnqueueTx(ctx, tx, "pgx.command", map[string]any{"n": 1}, IdempotencyKey("pgx-k1"))
	if err != nil {
		t.Fatal(err)
	}

	// A separate connection must not see the uncommitted job or domain row.
	var visibleJobs, visibleRows int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM domain_rows)`).Scan(&visibleJobs, &visibleRows); err != nil {
		t.Fatal(err)
	}
	if visibleJobs != 0 || visibleRows != 0 {
		t.Fatalf("pre-commit visibility = jobs:%d rows:%d, want 0/0", visibleJobs, visibleRows)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE id=$1), (SELECT count(*) FROM domain_rows)`, id).Scan(&visibleJobs, &visibleRows); err != nil {
		t.Fatal(err)
	}
	if visibleJobs != 1 || visibleRows != 1 {
		t.Fatalf("post-commit visibility = jobs:%d rows:%d, want 1/1", visibleJobs, visibleRows)
	}
}

func TestEnqueueTxRollbackLeavesNothing(t *testing.T) {
	pool, db := testPgxPool(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO domain_rows (name) VALUES ('rolled-back')`); err != nil {
		t.Fatal(err)
	}
	if _, err := EnqueueTx(ctx, tx, "pgx.command", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var jobs, rows, events int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM domain_rows), (SELECT count(*) FROM job_events)`).
		Scan(&jobs, &rows, &events); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || rows != 0 || events != 0 {
		t.Fatalf("after rollback = jobs:%d rows:%d events:%d, want 0/0/0", jobs, rows, events)
	}
}

func TestEnqueueTxIdempotent(t *testing.T) {
	pool, db := testPgxPool(t)
	ctx := context.Background()

	first, err := EnqueueTx(ctx, pool, "pgx.command", map[string]any{"n": 1}, IdempotencyKey("pgx-dup"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := EnqueueTx(ctx, pool, "pgx.command", map[string]any{"n": 2}, IdempotencyKey("pgx-dup"))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent ids = %d, %d", first, second)
	}
	var jobs, events int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_events)`).Scan(&jobs, &events); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || events != 1 {
		t.Fatalf("jobs/events = %d/%d, want 1/1", jobs, events)
	}
}

func TestEnqueueTxMatchesDatabaseSQLSemantics(t *testing.T) {
	pool, db := testPgxPool(t)
	ctx := context.Background()

	pgxID, err := EnqueueTx(ctx, pool, "pgx.command", map[string]any{"path": "pgx"}, Queue("shared"))
	if err != nil {
		t.Fatal(err)
	}
	sqlID, err := NewDispatcher(db).Dispatch(ctx, "pgx.command", map[string]any{"path": "sql"}, Queue("shared"))
	if err != nil {
		t.Fatal(err)
	}
	if pgxID == sqlID {
		t.Fatalf("expected distinct jobs, got %d", pgxID)
	}
	var payloads int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE queue='shared' AND payload->>'command'='pgx.command'`).Scan(&payloads); err != nil {
		t.Fatal(err)
	}
	if payloads != 2 {
		t.Fatalf("shared command jobs = %d, want 2", payloads)
	}
}
