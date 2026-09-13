package jobqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCancelQueuedJob(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, err := NewDispatcher(db).Dispatch(ctx, "cancel.command", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewDispatcher(db).RequestCancel(ctx, id)
	if err != nil || result != CancelCancelled {
		t.Fatalf("RequestCancel = %v, %v", result, err)
	}
	var status string
	var cancelled, cancelRequested bool
	if err := db.QueryRow(`SELECT status, cancelled_at IS NOT NULL, cancel_requested_at IS NOT NULL FROM jobs WHERE id=$1`, id).
		Scan(&status, &cancelled, &cancelRequested); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" || !cancelled || !cancelRequested {
		t.Fatalf("status=%q cancelled=%v requested=%v", status, cancelled, cancelRequested)
	}
	assertEventTypes(t, db, id, string(EventEnqueued), string(EventCancelRequested), string(EventCancelled))

	// A cancelled job must never run.
	worker := &Worker{DB: db, Registry: NewRegistry()}
	if processed, err := worker.ProcessNext(ctx); err != nil || processed {
		t.Fatalf("cancelled job was processed: %v, %v", processed, err)
	}
}

func TestCancelRunningJobPropagatesAndNeverRetries(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	started := make(chan struct{})
	registry := NewRegistry()
	if err := registry.Register("cancel.running", func(handlerCtx context.Context, _ []byte) error {
		close(started)
		<-handlerCtx.Done()
		return handlerCtx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	id, err := NewDispatcher(db).Dispatch(ctx, "cancel.running", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, LeaseDuration: 30 * time.Second, HeartbeatInterval: 25 * time.Millisecond}
	processed := make(chan error, 1)
	go func() {
		_, err := worker.ProcessNext(ctx)
		processed <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	result, err := NewDispatcher(db).RequestCancel(ctx, id)
	if err != nil || result != CancelRequested {
		t.Fatalf("RequestCancel = %v, %v", result, err)
	}
	select {
	case err := <-processed:
		if err != nil {
			t.Fatalf("ProcessNext: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation was not detected (no notification listener configured)")
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("status = %q, want cancelled (not failed/available)", status)
	}
	var outcome string
	if err := db.QueryRow(`SELECT outcome FROM job_attempts WHERE job_id=$1 AND attempt=1`, id).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != string(OutcomeCancelled) {
		t.Fatalf("attempt outcome = %q, want cancelled", outcome)
	}
	var failures int
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1`, id).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 0 {
		t.Fatalf("cancelled job recorded %d failure rows", failures)
	}
	// No retry: nothing left to run.
	if processed, err := worker.ProcessNext(ctx); err != nil || processed {
		t.Fatalf("cancelled job retried: %v, %v", processed, err)
	}
}

func TestCancelRunningJobWithoutNotificationListener(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	started := make(chan struct{})
	registry := NewRegistry()
	if err := registry.Register("cancel.poll", func(handlerCtx context.Context, _ []byte) error {
		close(started)
		<-handlerCtx.Done()
		return handlerCtx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	id, err := NewDispatcher(db).Dispatch(ctx, "cancel.poll", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// DatabaseURL is empty, so there is no LISTEN: detection must come from the
	// durable state on the heartbeat.
	worker := &Worker{DB: db, Registry: registry, LeaseDuration: 30 * time.Second, HeartbeatInterval: 25 * time.Millisecond}
	processed := make(chan error, 1)
	go func() { _, err := worker.ProcessNext(ctx); processed <- err }()
	<-started
	if _, err := NewDispatcher(db).RequestCancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-processed:
		if err != nil {
			t.Fatalf("ProcessNext: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("durable cancellation was not detected on the heartbeat")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("status = %q, want cancelled", status)
	}
}

func TestCancelAlreadyTerminalIsNotRetroactive(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	registry := NewRegistry()
	_ = registry.Register("cancel.done", func(context.Context, []byte) error { return nil })
	id, err := NewDispatcher(db).Dispatch(ctx, "cancel.done", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	result, err := NewDispatcher(db).RequestCancel(ctx, id)
	if err != nil || result != CancelAlreadyTerminal {
		t.Fatalf("RequestCancel = %v, %v", result, err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("terminal job rewritten to %q", status)
	}
}

func TestCancelAlreadyRequested(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	payload := `{"command":"cancel.command","arguments":{},"sensitive":[]}`
	var id int64
	if err := db.QueryRow(`
INSERT INTO jobs (payload, status, reserved_at, reservation_id, worker_id, heartbeat_at, lease_expires_at)
VALUES ($1::jsonb, 'reserved', clock_timestamp(), 'r1', 'w1', clock_timestamp(), clock_timestamp()+interval '5 minutes')
RETURNING id`, payload).Scan(&id); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(db)
	if result, err := dispatcher.RequestCancel(ctx, id); err != nil || result != CancelRequested {
		t.Fatalf("first RequestCancel = %v, %v", result, err)
	}
	if result, err := dispatcher.RequestCancel(ctx, id); err != nil || result != CancelAlreadyRequested {
		t.Fatalf("second RequestCancel = %v, %v", result, err)
	}
}

func TestCancelUnknownJob(t *testing.T) {
	db := testDB(t)
	if _, err := NewDispatcher(db).RequestCancel(context.Background(), 999999); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("RequestCancel unknown = %v", err)
	}
}

func TestCancellationRaceCompletionWins(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "cancel.race", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	job, err := claimNextFor(ctx, db, DefaultQueue, time.Minute, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if err := completeJob(ctx, db, job); err != nil {
		t.Fatal(err)
	}
	// A cancellation that arrives after completion is fenced out.
	if err := settleCancelled(ctx, db, job); !errors.Is(err, ErrStaleReservation) {
		t.Fatalf("settleCancelled after completion = %v, want ErrStaleReservation", err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("status = %q, want completed", status)
	}
}

func TestCancellationWithExpiredLeaseIsSettledCancelled(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := EnsureQueue(ctx, db, QueueConfig{Name: DefaultQueue}); err != nil {
		t.Fatal(err)
	}
	payload := `{"command":"cancel.command","arguments":{},"sensitive":[]}`
	var id int64
	if err := db.QueryRow(`
INSERT INTO jobs (payload, status, reserved_at, reservation_id, worker_id, heartbeat_at,
                  lease_expires_at, cancel_requested_at)
VALUES ($1::jsonb, 'reserved', clock_timestamp(), 'r1', 'w1', clock_timestamp(),
        clock_timestamp()-interval '1 second', clock_timestamp())
RETURNING id`, payload).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_attempts (job_id, queue, attempt, worker_id) VALUES ($1,'default',1,'w1')`, id); err != nil {
		t.Fatal(err)
	}
	result, err := Reap(ctx, db, ReapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Cancelled != 1 || result.Reclaimed != 0 || result.Failed != 0 {
		t.Fatalf("reap result = %+v", result)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("status = %q, want cancelled", status)
	}
}
