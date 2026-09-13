package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestClaimSetsWorkerIdentityAndLease(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	job, err := claimNextFor(ctx, db, DefaultQueue, 2*time.Minute, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	var workerID string
	var leaseFuture, heartbeatSet bool
	if err := db.QueryRow(`SELECT worker_id, lease_expires_at > clock_timestamp(), heartbeat_at IS NOT NULL FROM jobs WHERE id=$1`, job.ID).
		Scan(&workerID, &leaseFuture, &heartbeatSet); err != nil {
		t.Fatal(err)
	}
	if workerID != "worker-1" || !leaseFuture || !heartbeatSet {
		t.Fatalf("job owner=%q leaseFuture=%v heartbeat=%v", workerID, leaseFuture, heartbeatSet)
	}
	var attemptWorker string
	if err := db.QueryRow(`SELECT worker_id FROM job_attempts WHERE job_id=$1 AND attempt=1`, job.ID).Scan(&attemptWorker); err != nil {
		t.Fatal(err)
	}
	if attemptWorker != "worker-1" {
		t.Fatalf("attempt worker = %q", attemptWorker)
	}
	var eventWorker string
	if err := db.QueryRow(`SELECT detail->>'worker_id' FROM job_events WHERE job_id=$1 AND type='claimed'`, job.ID).Scan(&eventWorker); err != nil {
		t.Fatal(err)
	}
	if eventWorker != "worker-1" {
		t.Fatalf("claimed event worker = %q", eventWorker)
	}
}

func TestHeartbeatPreventsTheft(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: NewRegistry(), WorkerID: "heartbeat-1",
		LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond}
	job, err := claimNextFor(ctx, db, DefaultQueue, 300*time.Millisecond, worker.workerID())
	if err != nil {
		t.Fatal(err)
	}
	_, _, stop := worker.startHeartbeat(ctx, job)
	time.Sleep(500 * time.Millisecond) // longer than the initial lease
	stop()
	if _, err := claimNext(ctx, db, DefaultQueue, 300*time.Millisecond); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("healthy heartbeating job was stolen: %v", err)
	}
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	first, err := claimNextFor(ctx, db, DefaultQueue, 200*time.Millisecond, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	second, err := claimNext(ctx, db, DefaultQueue, time.Minute)
	if err != nil {
		t.Fatalf("expired lease not reclaimed: %v", err)
	}
	if second.ID != first.ID || second.ReservationID == first.ReservationID || second.Attempts != 2 {
		t.Fatalf("reclaim first=%+v second=%+v", first, second)
	}
}

func TestStaleOwnerCannotRenewReclaimedLease(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: NewRegistry(), WorkerID: "stale-owner"}
	job, err := claimNextFor(ctx, db, DefaultQueue, time.Minute, worker.workerID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := Reap(ctx, db, ReapOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.renewLease(ctx, job); !errors.Is(err, ErrStaleReservation) {
		t.Fatalf("stale renewal error = %v, want ErrStaleReservation", err)
	}
}

func TestReapFailPolicy(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := RegisterQueue(ctx, db, QueueConfig{Name: "fail.queue", ExpiredLeasePolicy: ExpiredLeaseFail}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}, Queue("fail.queue")); err != nil {
		t.Fatal(err)
	}
	job, err := claimNextFor(ctx, db, "fail.queue", time.Minute, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	result, err := Reap(ctx, db, ReapOptions{Queues: []string{"fail.queue"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 || result.Reclaimed != 0 {
		t.Fatalf("reap result = %+v", result)
	}
	var status, code string
	if err := db.QueryRow(`SELECT status, last_error FROM jobs WHERE id=$1`, job.ID).Scan(&status, &code); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("status = %q, want failed", status)
	}
	var outcome, attemptCode string
	if err := db.QueryRow(`SELECT outcome, error_code FROM job_attempts WHERE job_id=$1 AND attempt=1`, job.ID).Scan(&outcome, &attemptCode); err != nil {
		t.Fatal(err)
	}
	if outcome != string(OutcomeLeaseExpired) || attemptCode != "lease_expired" {
		t.Fatalf("attempt outcome/code = %q/%q", outcome, attemptCode)
	}
	var failures int
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1 AND terminal`, job.ID).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Fatalf("terminal failure history = %d", failures)
	}
	assertEventTypes(t, db, job.ID, string(EventEnqueued), string(EventClaimed), string(EventFailed))
}

func TestReapRequeuePolicy(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	job, err := claimNextFor(ctx, db, DefaultQueue, time.Minute, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	result, err := Reap(ctx, db, ReapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Reclaimed != 1 || result.Failed != 0 {
		t.Fatalf("reap result = %+v", result)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "available" {
		t.Fatalf("status = %q, want available", status)
	}
	var outcome string
	if err := db.QueryRow(`SELECT outcome FROM job_attempts WHERE job_id=$1 AND attempt=1`, job.ID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != string(OutcomeLeaseExpired) {
		t.Fatalf("attempt outcome = %q", outcome)
	}
	assertEventTypes(t, db, job.ID, string(EventEnqueued), string(EventClaimed), string(EventReclaimed))
}

func TestNullLeaseCountsAsExpired(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, err := NewDispatcher(db).Dispatch(ctx, "lease.command", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a row claimed by a pre-lease worker: reserved with no lease.
	if _, err := db.Exec(`
UPDATE jobs SET status='reserved', reserved_at=clock_timestamp(), reservation_id='legacy' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	result, err := Reap(ctx, db, ReapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Reclaimed != 1 {
		t.Fatalf("reap result = %+v, want one reclaim", result)
	}
	if _, err := claimNext(ctx, db, DefaultQueue, time.Minute); err != nil {
		t.Fatalf("NULL-lease job not claimable after reap: %v", err)
	}
}

func TestMigrationGuardBlocksReservedJobs(t *testing.T) {
	db := testDBAtVersion(t, 3)
	ctx := context.Background()
	payload := `{"command":"guard.command","arguments":{},"sensitive":[]}`
	if _, err := db.Exec(`INSERT INTO jobs (payload,status,reserved_at,reservation_id) VALUES ($1::jsonb,'reserved',clock_timestamp(),'r1')`, payload); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, db)
	if !errors.Is(err, ErrActiveReservations) {
		t.Fatalf("Migrate error = %v, want ErrActiveReservations", err)
	}
	var safety *MigrationSafetyError
	if !errors.As(err, &safety) || safety.ReservedJobs != 1 {
		t.Fatalf("typed error = %#v", err)
	}
	var hasLease bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='jobs' AND column_name='lease_expires_at')`).Scan(&hasLease); err != nil {
		t.Fatal(err)
	}
	if hasLease {
		t.Fatal("guard returned an error but the lease migration was applied")
	}
}

func TestMigrationGuardProceedsWhenDrained(t *testing.T) {
	db := testDBAtVersion(t, 3)
	ctx := context.Background()
	payload := `{"command":"guard.command","arguments":{},"sensitive":[]}`
	if _, err := db.Exec(`INSERT INTO jobs (payload) VALUES ($1::jsonb)`, payload); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate with only available jobs: %v", err)
	}
	var hasLease bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='jobs' AND column_name='lease_expires_at')`).Scan(&hasLease); err != nil {
		t.Fatal(err)
	}
	if !hasLease {
		t.Fatal("Migrate did not apply the lease migration")
	}
}

func TestMigrateAfterLeaseMigrationIgnoresRunningJobs(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, err := NewDispatcher(db).Dispatch(ctx, "guard.command", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
UPDATE jobs SET status='reserved', reserved_at=clock_timestamp(), reservation_id='r1',
    worker_id='w1', heartbeat_at=clock_timestamp(), lease_expires_at=clock_timestamp()+interval '10 minutes'
WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	// The lease migration is already applied, so Migrate must be unconditional.
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate after the lease migration rejected startup: %v", err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" {
		t.Fatalf("running job status changed to %q", status)
	}
}

func TestJobTimeoutMayExceedLease(t *testing.T) {
	db := testDB(t)
	worker := &Worker{DB: db, Registry: NewRegistry(), JobTimeout: 30 * time.Minute,
		LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second}
	if err := worker.validate(); err != nil {
		t.Fatalf("lease now outlives the handler, but validate failed: %v", err)
	}
}

func TestWorkerIdentityIsStable(t *testing.T) {
	worker := &Worker{}
	first := worker.workerID()
	if first == "" || worker.workerID() != first {
		t.Fatalf("worker identity not stable: %q then %q", first, worker.workerID())
	}
}
