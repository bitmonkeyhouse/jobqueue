package jobqueue

import (
	"context"
	"database/sql"
	"testing"
)

// v01Dispatcher mirrors the consumer-declared interface indifeed uses
// (indifeed/internal/feedback/queued_email.go:17). v0.2.0 must keep satisfying
// it unchanged: DispatchTx keeps its concrete *sql.Tx parameter.
type v01Dispatcher interface {
	Dispatch(context.Context, string, any, ...Option) (int64, error)
	DispatchTx(context.Context, *sql.Tx, string, any, ...Option) (int64, error)
}

var _ v01Dispatcher = (*QueueDispatcher)(nil)

// TestV01CompatibilitySurface exercises the v0.1.0 public surface an existing
// consumer uses end to end: dispatcher construction, Dispatch, DispatchTx,
// Registry/Register, Worker.ProcessNext and the reaper-style settlement path.
func TestV01CompatibilitySurface(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	registry := NewRegistry()
	if err := registry.Register("compat.command", func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	var dispatcher v01Dispatcher = NewDispatcher(db)
	if _, err := dispatcher.Dispatch(ctx, "compat.command", map[string]any{"x": 1},
		Queue(DefaultQueue), Metadata(map[string]any{"kind": "compat"})); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := dispatcher.DispatchTx(ctx, tx, "compat.command", map[string]any{"x": 2}); err != nil {
		t.Fatalf("DispatchTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	worker := &Worker{DB: db, Registry: registry, Queue: DefaultQueue}
	for i := 0; i < 2; i++ {
		processed, err := worker.ProcessNext(ctx)
		if err != nil {
			t.Fatalf("ProcessNext: %v", err)
		}
		if !processed {
			t.Fatalf("ProcessNext returned no work on iteration %d", i)
		}
	}
	var completed int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status='completed'`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 2 {
		t.Fatalf("completed jobs = %d, want 2", completed)
	}
}
