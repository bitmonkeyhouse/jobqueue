package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func intPtr(value int) *int { return &value }

func openSecondConn(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEnqueueCreatesQueueWithDefaults(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "queue.command", map[string]any{}, Queue("auto.queue")); err != nil {
		t.Fatal(err)
	}
	var maxConcurrency sql.NullInt64
	var sequenceConcurrency int
	var policy string
	if err := db.QueryRow(`SELECT max_concurrency, sequence_concurrency, expired_lease_policy FROM job_queues WHERE name='auto.queue'`).
		Scan(&maxConcurrency, &sequenceConcurrency, &policy); err != nil {
		t.Fatal(err)
	}
	if maxConcurrency.Valid || sequenceConcurrency != 1 || policy != string(ExpiredLeaseRequeue) {
		t.Fatalf("defaults = max:%v seq:%d policy:%q", maxConcurrency, sequenceConcurrency, policy)
	}
}

func TestEnsureQueueCreatesAndNeverMutates(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	cfg := QueueConfig{Name: "ensure.queue", MaxConcurrency: intPtr(3), ExpiredLeasePolicy: ExpiredLeaseFail}

	created, err := EnsureQueue(ctx, db, cfg)
	if err != nil || !created {
		t.Fatalf("EnsureQueue create = %v, %v", created, err)
	}
	created, err = EnsureQueue(ctx, db, QueueConfig{Name: "ensure.queue", MaxConcurrency: intPtr(9), ExpiredLeasePolicy: ExpiredLeaseRequeue})
	if err != nil || created {
		t.Fatalf("EnsureQueue second = %v, %v", created, err)
	}
	var maxConcurrency int
	var policy string
	if err := db.QueryRow(`SELECT max_concurrency, expired_lease_policy FROM job_queues WHERE name='ensure.queue'`).
		Scan(&maxConcurrency, &policy); err != nil {
		t.Fatal(err)
	}
	if maxConcurrency != 3 || policy != string(ExpiredLeaseFail) {
		t.Fatalf("EnsureQueue mutated existing row: max=%d policy=%q", maxConcurrency, policy)
	}
}

func TestRegisterQueueExactUpsert(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	state, err := RegisterQueue(ctx, db, QueueConfig{Name: "register.queue", MaxConcurrency: intPtr(5)})
	if err != nil || state != QueueCreated {
		t.Fatalf("RegisterQueue create = %v, %v", state, err)
	}
	state, err = RegisterQueue(ctx, db, QueueConfig{Name: "register.queue", MaxConcurrency: intPtr(5)})
	if err != nil || state != QueueUnchanged {
		t.Fatalf("RegisterQueue unchanged = %v, %v", state, err)
	}
	state, err = RegisterQueue(ctx, db, QueueConfig{Name: "register.queue", MaxConcurrency: intPtr(2)})
	if err != nil || state != QueueUpdated {
		t.Fatalf("RegisterQueue tighten = %v, %v", state, err)
	}
	// Exact upsert may loosen back to unlimited.
	state, err = RegisterQueue(ctx, db, QueueConfig{Name: "register.queue"})
	if err != nil || state != QueueUpdated {
		t.Fatalf("RegisterQueue loosen = %v, %v", state, err)
	}
	var maxConcurrency sql.NullInt64
	if err := db.QueryRow(`SELECT max_concurrency FROM job_queues WHERE name='register.queue'`).Scan(&maxConcurrency); err != nil {
		t.Fatal(err)
	}
	if maxConcurrency.Valid {
		t.Fatalf("max_concurrency = %v, want NULL (unlimited)", maxConcurrency.Int64)
	}
}

func TestQueueMaxConcurrencyAcrossConnections(t *testing.T) {
	db, dsn := openIsolatedSchemaWithDSN(t, "test_jobqueue_capacity_")
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	other := openSecondConn(t, dsn)
	ctx := context.Background()

	if _, err := RegisterQueue(ctx, db, QueueConfig{Name: "limited", MaxConcurrency: intPtr(1)}); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(db)
	first, err := dispatcher.Dispatch(ctx, "capacity.command", map[string]any{"n": 1}, Queue("limited"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Dispatch(ctx, "capacity.command", map[string]any{"n": 2}, Queue("limited"))
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := claimNext(ctx, db, "limited", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != first && claimed.ID != second {
		t.Fatalf("claimed unexpected job %d", claimed.ID)
	}
	if _, err := claimNext(ctx, other, "limited", time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second connection claimed beyond max_concurrency: %v", err)
	}
	if err := completeJob(ctx, db, claimed); err != nil {
		t.Fatal(err)
	}
	next, err := claimNext(ctx, other, "limited", time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if next.ID == claimed.ID {
		t.Fatalf("reclaimed completed job %d", next.ID)
	}
}

func TestSequenceKeyIsSequential(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{"n": 1}, Queue("seq"), SequenceKey("project:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{"n": 2}, Queue("seq"), SequenceKey("project:1")); err != nil {
		t.Fatal(err)
	}
	first, err := claimNext(ctx, db, "seq", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(ctx, db, "seq", time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("same sequence key ran concurrently: %v", err)
	}
	if err := completeJob(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(ctx, db, "seq", time.Minute); err != nil {
		t.Fatalf("claim after sequence released: %v", err)
	}
}

func TestDifferentSequenceKeysRunConcurrently(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{}, Queue("seq2"), SequenceKey("project:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{}, Queue("seq2"), SequenceKey("project:2")); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(ctx, db, "seq2", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(ctx, db, "seq2", time.Minute); err != nil {
		t.Fatalf("different sequence keys blocked each other: %v", err)
	}
}

func TestBlockedCandidateIsSkipped(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	blocking, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{}, Queue("seq3"), SequenceKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{}, Queue("seq3"), SequenceKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	eligible, err := dispatcher.Dispatch(ctx, "seq.command", map[string]any{}, Queue("seq3"), SequenceKey("k2"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := claimNext(ctx, db, "seq3", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != blocking {
		t.Fatalf("first claim = %d, want %d", claimed.ID, blocking)
	}
	// The older blocked job (id=blocked) must be skipped for the eligible one.
	next, err := claimNext(ctx, db, "seq3", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != eligible {
		t.Fatalf("second claim = %d, want eligible %d (blocked %d)", next.ID, eligible, blocked)
	}
}
