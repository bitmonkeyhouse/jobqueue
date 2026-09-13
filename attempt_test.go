package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestMaxAttemptsOneRunsOnce(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	calls := 0
	registry := NewRegistry()
	if err := registry.Register("attempts.one", func(context.Context, []byte) error {
		calls++
		return Diagnostic("provider_error", "provider rejected the request", errors.New("boom"))
	}); err != nil {
		t.Fatal(err)
	}
	id, err := NewDispatcher(db).Dispatch(ctx, "attempts.one", map[string]any{}, MaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, jitter: func(d time.Duration) time.Duration { return d }}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM jobs WHERE id=$1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 1 {
		t.Fatalf("status=%q attempts=%d, want failed/1", status, attempts)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	// The cap is exhausted, so the job is not claimable again.
	if processed, err := worker.ProcessNext(ctx); err != nil || processed {
		t.Fatalf("exhausted job ran again: %v, %v", processed, err)
	}
	var attemptsRows, terminal int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM job_attempts WHERE job_id=$1), (SELECT count(*) FROM job_failures WHERE job_id=$1 AND terminal)`, id).
		Scan(&attemptsRows, &terminal); err != nil {
		t.Fatal(err)
	}
	if attemptsRows != 1 || terminal != 1 {
		t.Fatalf("attempts=%d terminalFailures=%d", attemptsRows, terminal)
	}
}

func TestMaxAttemptsTwoRunsTwice(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	calls := 0
	registry := NewRegistry()
	if err := registry.Register("attempts.two", func(context.Context, []byte) error {
		calls++
		return errors.New("still failing")
	}); err != nil {
		t.Fatal(err)
	}
	id, err := NewDispatcher(db).Dispatch(ctx, "attempts.two", map[string]any{}, MaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, jitter: func(d time.Duration) time.Duration { return d }}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("first ProcessNext = %v, %v", processed, err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status, attempts FROM jobs WHERE id=$1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "available" || attempts != 1 {
		t.Fatalf("after first failure status=%q attempts=%d, want available/1", status, attempts)
	}
	// Skip the backoff window.
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("second ProcessNext = %v, %v", processed, err)
	}
	if err := db.QueryRow(`SELECT status, attempts FROM jobs WHERE id=$1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 2 {
		t.Fatalf("after second failure status=%q attempts=%d, want failed/2", status, attempts)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestMaxAttemptsOneKilledWorkerNeverReexecutes(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "attempts.killed", map[string]any{}, MaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	job, err := claimNextFor(ctx, db, DefaultQueue, time.Minute, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if job.MaxAttempts != 1 || job.Attempts != 1 {
		t.Fatalf("claim max=%d attempts=%d", job.MaxAttempts, job.Attempts)
	}
	// The worker dies: its lease expires. The cap is consumed, so this must fail
	// terminally rather than run a second attempt.
	if _, err := db.Exec(`UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	result, err := Reap(ctx, db, ReapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("reap result = %+v, want one terminal failure", result)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("status = %q, want failed", status)
	}
	if _, err := claimNext(ctx, db, DefaultQueue, time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("one-attempt job was re-executed: %v", err)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	const base = time.Minute
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 200; i++ {
		got := defaultJitter(base)
		if got < 48*time.Second || got > 72*time.Second {
			t.Fatalf("jittered backoff %s outside ±20%% of %s", got, base)
		}
		seen[got] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("jitter produced no variation: %v", seen)
	}
}
