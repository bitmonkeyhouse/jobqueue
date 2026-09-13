package jobqueue

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func insertPruneJob(t *testing.T, db *sql.DB, queue, status string, updatedAt time.Time) int64 {
	t.Helper()
	payload := `{"command":"prune.command","arguments":{},"sensitive":[]}`
	var column string
	switch status {
	case "completed":
		column = "completed_at"
	case "failed":
		column = "failed_at"
	case "cancelled":
		column = "cancelled_at"
	default:
		t.Fatalf("unsupported status %q", status)
	}
	var id int64
	err := db.QueryRow(`
INSERT INTO jobs (queue, payload, status, `+column+`, created_at, updated_at)
VALUES ($1, $2::jsonb, $3, clock_timestamp(), $4, $4)
RETURNING id`, queue, payload, status, updatedAt).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_attempts (job_id, queue, attempt, outcome, finished_at, duration_ms)
VALUES ($1, $2, 1, 'completed', clock_timestamp(), 5)`, id, queue); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_events (job_id, type, detail) VALUES ($1, 'completed', '{}')`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_failures (job_id, queue, command, attempts, retry_attempts, error_code, error_message, terminal)
VALUES ($1, $2, 'prune.command', 1, 0, 'prune_failed', 'pruned failure', true)`, id, queue); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPruneDeletesTerminalJobsAndHistory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	cutoff := time.Now().Add(time.Second)
	completed := insertPruneJob(t, db, "prune.q", "completed", time.Now().Add(-time.Hour))
	failed := insertPruneJob(t, db, "prune.q", "failed", time.Now().Add(-time.Hour))
	cancelled := insertPruneJob(t, db, "prune.q", "cancelled", time.Now().Add(-time.Hour))

	deleted, err := Prune(ctx, db, PruneOptions{TerminalBefore: cutoff})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("pruned %d, want 3", deleted)
	}
	for _, id := range []int64{completed, failed, cancelled} {
		var jobs, attempts, events, failures int
		if err := db.QueryRow(`
SELECT (SELECT count(*) FROM jobs WHERE id=$1),
       (SELECT count(*) FROM job_attempts WHERE job_id=$1),
       (SELECT count(*) FROM job_events WHERE job_id=$1),
       (SELECT count(*) FROM job_failures WHERE job_id=$1)`, id).
			Scan(&jobs, &attempts, &events, &failures); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 || attempts != 0 || events != 0 || failures != 0 {
			t.Fatalf("job %d left orphans: jobs=%d attempts=%d events=%d failures=%d", id, jobs, attempts, events, failures)
		}
	}
}

func TestPruneRespectsCutoffAndNonTerminal(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	recent := insertPruneJob(t, db, "prune.q", "completed", time.Now())
	// A non-terminal job with an old update must never be pruned.
	payload := `{"command":"prune.command","arguments":{},"sensitive":[]}`
	var available int64
	if err := db.QueryRow(`
INSERT INTO jobs (queue, payload, status, created_at, updated_at)
VALUES ('prune.q', $1::jsonb, 'available', clock_timestamp()-interval '2 hours', clock_timestamp()-interval '2 hours')
RETURNING id`, payload).Scan(&available); err != nil {
		t.Fatal(err)
	}
	deleted, err := Prune(ctx, db, PruneOptions{TerminalBefore: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("pruned %d jobs past the cutoff boundary", deleted)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE id IN ($1,$2)`, recent, available).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("remaining = %d, want 2", remaining)
	}
}

func TestPruneQueueFilterAndLimit(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	insertPruneJob(t, db, "queue.a", "completed", time.Now().Add(-time.Hour))
	insertPruneJob(t, db, "queue.a", "completed", time.Now().Add(-time.Hour))
	bKeep := insertPruneJob(t, db, "queue.b", "completed", time.Now().Add(-time.Hour))

	deleted, err := Prune(ctx, db, PruneOptions{TerminalBefore: time.Now(), Queues: []string{"queue.a"}, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("pruned %d, want 1 (limit)", deleted)
	}
	var remainingA, remainingB int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE queue='queue.a'), (SELECT count(*) FROM jobs WHERE id=$1)`, bKeep).
		Scan(&remainingA, &remainingB); err != nil {
		t.Fatal(err)
	}
	if remainingA != 1 || remainingB != 1 {
		t.Fatalf("remaining a=%d b=%d, want 1/1", remainingA, remainingB)
	}
}

func TestPruneRequiresCutoff(t *testing.T) {
	db := testDB(t)
	if _, err := Prune(context.Background(), db, PruneOptions{}); err == nil {
		t.Fatal("Prune accepted a zero cutoff")
	}
}
