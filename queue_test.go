package jobqueue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://localhost:5432/postgres?sslmode=disable"
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		if os.Getenv("TEST_DATABASE_URL") != "" {
			t.Fatalf("configured postgres unavailable: %v", err)
		}
		t.Skipf("postgres unavailable: %v", err)
	}
	schema := "test_jobqueue_" + newTestID(t)
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema + `, public`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(embeddedMigrationUpSQL(t)); err != nil {
		_ = db.Close()
		t.Fatalf("create isolated jobs schema: %v", err)
	}
	adminDB := db
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-csearch_path="+schema+",public")
	parsed.RawQuery = query.Encode()
	db, err = sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = adminDB.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = adminDB.Close()
	})
	return db
}

func embeddedMigrationUpSQL(t *testing.T) string {
	t.Helper()
	data, err := migrationsFS.ReadFile("migrations/00001_create_jobs.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.Split(string(data), "-- +goose Down")[0]
	return strings.Replace(up, "-- +goose Up", "", 1)
}

func newTestID(t *testing.T) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())))
	return fmt.Sprintf("%x", sum[:8])
}

func TestDispatchAndDispatchTxCommitRollback(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "direct", map[string]string{"value": "one"}); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	if _, err := dispatcher.DispatchTx(context.Background(), tx, "rolled-back", map[string]string{"value": "two"}); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	tx, _ = db.Begin()
	if _, err := dispatcher.DispatchTx(context.Background(), tx, "committed", map[string]string{"value": "three"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var commands []string
	rows, _ := db.Query(`SELECT payload->>'command' FROM jobs ORDER BY id`)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var command string
		_ = rows.Scan(&command)
		commands = append(commands, command)
	}
	if strings.Join(commands, ",") != "direct,committed" {
		t.Fatalf("commands = %v", commands)
	}
}

func TestDispatchRetryDeadlinesUseFirstAvailability(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	before := time.Now()
	if _, err := dispatcher.Dispatch(context.Background(), "default-window", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), "custom-window", map[string]string{"value": "x"}, RetryWindow(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), "delayed-window", map[string]string{"value": "x"}, Delay(3*time.Hour), RetryWindow(4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	for _, test := range []struct {
		command string
		delay   time.Duration
		window  time.Duration
	}{
		{"default-window", 0, 24 * time.Hour},
		{"custom-window", 0, 2 * time.Hour},
		{"delayed-window", 3 * time.Hour, 4 * time.Hour},
	} {
		var availableAt, retryUntil time.Time
		if err := db.QueryRow(`SELECT available_at,retry_until FROM jobs WHERE payload->>'command'=$1`, test.command).Scan(&availableAt, &retryUntil); err != nil {
			t.Fatal(err)
		}
		if retryUntil.Sub(availableAt) != test.window {
			t.Fatalf("%s retry window = %s, want exactly %s", test.command, retryUntil.Sub(availableAt), test.window)
		}
		if availableAt.Before(before.Add(test.delay-time.Second)) || availableAt.After(after.Add(test.delay+time.Second)) {
			t.Fatalf("%s availability = %s outside dispatch tolerance", test.command, availableAt)
		}
	}
}

func TestClaimSkipsRowLockedByAnotherTransaction(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "first", map[string]string{"value": "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), "second", map[string]string{"value": "two"}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var lockedID int64
	if err := tx.QueryRow(`SELECT id FROM jobs WHERE payload->>'command'='first' FOR UPDATE`).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	job, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID == lockedID || job.Command != "second" {
		t.Fatalf("claimed locked/wrong job: %+v locked=%d", job, lockedID)
	}
}

func TestClaimExclusivityExpiryDelayAndStaleFencing(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "delayed", map[string]string{"secret": "x"}, Delay(time.Hour), SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("early delayed claim error = %v", err)
	}
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	first, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("simultaneous second claim error = %v", err)
	}
	if _, err := db.Exec(`UPDATE jobs SET reserved_at=clock_timestamp()-interval '91 seconds'`); err != nil {
		t.Fatal(err)
	}
	second, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.ReservationID == second.ReservationID || second.Attempts != 2 {
		t.Fatalf("reclaimed job first=%+v second=%+v", first, second)
	}
	if err := completeJob(context.Background(), db, first); !errors.Is(err, ErrStaleReservation) {
		t.Fatalf("stale completion error = %v", err)
	}
	if err := completeJob(context.Background(), db, second); err != nil {
		t.Fatal(err)
	}
}

func TestReservationReclaimsDoNotAdvanceHandlerFailureBackoff(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("reclaimed-failure", func(context.Context, []byte) error { return errors.New("down") })
	if _, err := dispatcher.Dispatch(context.Background(), "reclaimed-failure", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET reserved_at=clock_timestamp()-interval '91 seconds'`); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	processed, err := (&Worker{DB: db, Registry: registry}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var attempts, retryAttempts int
	var availableAt time.Time
	if err := db.QueryRow(`SELECT attempts,retry_attempts,available_at FROM jobs`).Scan(&attempts, &retryAttempts, &availableAt); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || retryAttempts != 1 {
		t.Fatalf("attempts=%d retryAttempts=%d", attempts, retryAttempts)
	}
	if delay := availableAt.Sub(before); delay < 59*time.Second || delay > 61*time.Second {
		t.Fatalf("first handler failure delay after reclaim = %s", delay)
	}
}

func TestExpiredAvailableJobFailsWithoutInvokingHandler(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "expired", map[string]string{"secret": "value"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()-interval '2 seconds', retry_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	var calls atomic.Int32
	_ = registry.Register("expired", func(context.Context, []byte) error { calls.Add(1); return nil })
	processed, err := (&Worker{DB: db, Registry: registry}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status, payload string
	if err := db.QueryRow(`SELECT status,payload::text FROM jobs`).Scan(&status, &payload); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || calls.Load() != 0 || strings.Contains(payload, "value") {
		t.Fatalf("status=%s calls=%d payload=%s", status, calls.Load(), payload)
	}
}

func TestExpiredCurrentReservationWaitsForReservationExpiry(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "running", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()-interval '2 seconds', retry_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	processed, err := (&Worker{DB: db, Registry: NewRegistry()}).ProcessNext(context.Background())
	if err != nil || processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" {
		t.Fatalf("status=%s, want active reservation untouched", status)
	}
}

func TestWorkerTreatsStaleSettlementAsSuperseded(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("stale", func(context.Context, []byte) error {
		_, err := db.Exec(`UPDATE jobs SET reservation_id='newer' WHERE payload->>'command'='stale'`)
		return err
	})
	if _, err := dispatcher.Dispatch(context.Background(), "stale", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	processed, err := (&Worker{DB: db, Registry: registry}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status string
	var failures int
	if err := db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM job_failures`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" || failures != 0 {
		t.Fatalf("status = %q failures=%d, want newer reservation untouched", status, failures)
	}
}

func TestExpiredStaleReservationBecomesTerminalWithoutHandler(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "success", map[string]string{"token": "crash-secret"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	if _, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()-interval '2 seconds', reserved_at=clock_timestamp()-interval '91 seconds', retry_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	var calls atomic.Int32
	_ = registry.Register("success", func(context.Context, []byte) error { calls.Add(1); return nil })
	worker := &Worker{DB: db, Registry: registry}
	processed, err := worker.ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status, payload, lastError string
	if err := db.QueryRow(`SELECT status,payload::text,last_error FROM jobs`).Scan(&status, &payload, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || calls.Load() != 0 || strings.Contains(payload, "crash-secret") || lastError != "retry_window_expired: job retry window expired before another attempt" {
		t.Fatalf("terminal crashed row status=%s calls=%d payload=%s error=%s", status, calls.Load(), payload, lastError)
	}
}

func TestCorruptExpiredReservationFailsWithConservativeRedaction(t *testing.T) {
	db := testDB(t)
	if _, err := db.Exec(`ALTER TABLE jobs DROP CONSTRAINT jobs_payload_check`); err != nil {
		t.Fatal(err)
	}
	payload := `{"command":"broken","arguments":{"secret":"expired-secret"},"sensitive":[42]}`
	if _, err := db.Exec(`INSERT INTO jobs(payload,status,attempts,available_at,retry_until,reserved_at,reservation_id) VALUES($1,'reserved',1,clock_timestamp()-interval '2 seconds',clock_timestamp()-interval '1 second',clock_timestamp()-interval '91 seconds','old')`, payload); err != nil {
		t.Fatal(err)
	}
	processed, err := (&Worker{DB: db, Registry: NewRegistry()}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status, terminal string
	if err := db.QueryRow(`SELECT status,payload::text FROM jobs`).Scan(&status, &terminal); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || strings.Contains(terminal, "expired-secret") || !strings.Contains(terminal, `"arguments": null`) {
		t.Fatalf("status=%s payload=%s", status, terminal)
	}
}

func TestJobsPayloadConstraintRejectsMissingAndNonStringSensitiveValues(t *testing.T) {
	db := testDB(t)
	invalid := []string{
		`{"arguments":{},"sensitive":[]}`,
		`{"command":"broken","sensitive":[]}`,
		`{"command":"broken","arguments":{}}`,
		`{"command":"broken","arguments":{},"sensitive":[42]}`,
	}
	for _, payload := range invalid {
		if _, err := db.Exec(`INSERT INTO jobs(payload) VALUES($1)`, payload); err == nil {
			t.Fatalf("invalid payload accepted: %s", payload)
		}
	}
}

func TestCorruptClaimedPayloadFailsAndNextValidJobProcesses(t *testing.T) {
	db := testDB(t)
	// Temporarily remove the new constraint to simulate a legacy/corrupt row.
	if _, err := db.Exec(`ALTER TABLE jobs DROP CONSTRAINT jobs_payload_check`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs(payload) VALUES ('{"command":"broken","arguments":{},"sensitive":[42]}')`); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "valid", map[string]string{"value": "ok"}); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	var calls atomic.Int32
	_ = registry.Register("valid", func(context.Context, []byte) error { calls.Add(1); return nil })
	worker := &Worker{DB: db, Registry: registry}
	processed, err := worker.ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("corrupt ProcessNext = %v, %v", processed, err)
	}
	processed, err = worker.ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("valid ProcessNext = %v, %v", processed, err)
	}
	var corruptStatus, corruptPayload, corruptError, validStatus string
	if err := db.QueryRow(`SELECT status,payload::text,last_error FROM jobs WHERE id=1`).Scan(&corruptStatus, &corruptPayload, &corruptError); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM jobs WHERE id=2`).Scan(&validStatus); err != nil {
		t.Fatal(err)
	}
	if corruptStatus != "failed" || strings.Contains(corruptPayload, "42") || corruptError != "invalid_payload: job payload could not be decoded" || validStatus != "completed" || calls.Load() != 1 {
		t.Fatalf("corrupt status=%s payload=%s error=%s valid=%s calls=%d", corruptStatus, corruptPayload, corruptError, validStatus, calls.Load())
	}
}

func TestWorkerSuccessRetryDeadlineAndUnknownCommand(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	var successCalls atomic.Int32
	_ = registry.Register("success", func(context.Context, []byte) error { successCalls.Add(1); return nil })
	_ = registry.Register("retry", func(context.Context, []byte) error { return errors.New("provider secret body") })
	worker := &Worker{DB: db, Registry: registry}
	if _, err := dispatcher.Dispatch(context.Background(), "success", map[string]string{"token": "secret-token"}, SensitiveArguments(), Metadata(map[string]any{"kind": "safe"})); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), "retry", map[string]string{"token": "retry-secret"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), "missing", map[string]string{"token": "missing-secret"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		processed, err := worker.ProcessNext(context.Background())
		if err != nil || !processed {
			t.Fatalf("ProcessNext %d = %v, %v", i, processed, err)
		}
	}
	var status, payload, lastError string
	if err := db.QueryRow(`SELECT status,payload::text,COALESCE(last_error,'') FROM jobs WHERE payload->>'command'='success'`).Scan(&status, &payload, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || strings.Contains(payload, "secret-token") || !strings.Contains(payload, `"kind": "safe"`) || successCalls.Load() != 1 {
		t.Fatalf("success row status=%s payload=%s calls=%d", status, payload, successCalls.Load())
	}
	if err := db.QueryRow(`SELECT status,payload::text,last_error FROM jobs WHERE payload->>'command'='missing'`).Scan(&status, &payload, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || strings.Contains(payload, "missing-secret") || lastError != "unknown_command: no handler is registered for the job command" {
		t.Fatalf("unknown row status=%s payload=%s error=%s", status, payload, lastError)
	}
	var availableAt time.Time
	if err := db.QueryRow(`SELECT status,available_at,last_error FROM jobs WHERE payload->>'command'='retry'`).Scan(&status, &availableAt, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "available" || lastError != "handler_failed: job handler returned an error" {
		t.Fatalf("retry row status=%s error=%s", status, lastError)
	}
	if delay := time.Until(availableAt); delay < 59*time.Second || delay > 61*time.Second {
		t.Fatalf("first retry delay = %s, want about 1 minute", delay)
	}
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp(), retry_until=clock_timestamp()+interval '30 seconds' WHERE payload->>'command'='retry'`); err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
		t.Fatalf("retry terminal process = %v, %v", processed, err)
	}
	if err := db.QueryRow(`SELECT status,payload::text,last_error FROM jobs WHERE payload->>'command'='retry'`).Scan(&status, &payload, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || strings.Contains(payload, "retry-secret") || lastError != "handler_failed: job handler returned an error" {
		t.Fatalf("terminal retry row status=%s payload=%s error=%s", status, payload, lastError)
	}
}

func TestReleaseUsesOneTimestampAtRetryBoundary(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "boundary", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	job, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET retry_until=clock_timestamp()+interval '60 seconds' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	// A zero delay at a very near boundary must either release consistently or
	// fail consistently; it must never violate retry_until >= available_at.
	if err := releaseJob(context.Background(), db, job, 0, "retry"); err != nil {
		t.Fatal(err)
	}
	var exact bool
	if err := db.QueryRow(`SELECT available_at <= retry_until AND retry_attempts=1 FROM jobs WHERE id=$1`, job.ID).Scan(&exact); err != nil {
		t.Fatal(err)
	}
	if !exact {
		t.Fatal("released job crossed its retry deadline")
	}
}

func TestReleaseNearBoundaryFailsTerminallyWithoutConstraintRace(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.Dispatch(context.Background(), "near-boundary", map[string]string{"secret": "x"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	job, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET retry_until=clock_timestamp()+interval '59.999 seconds' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := retryOrFailJob(context.Background(), db, job, time.Minute, "failed"); err != nil {
		t.Fatal(err)
	}
	var status, payload string
	var retryAttempts int
	if err := db.QueryRow(`SELECT status,payload::text,retry_attempts FROM jobs WHERE id=$1`, job.ID).Scan(&status, &payload, &retryAttempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || retryAttempts != 0 || strings.Contains(payload, `"secret":"x"`) {
		t.Fatalf("status=%s retryAttempts=%d payload=%s", status, retryAttempts, payload)
	}
}

func TestWorkerPollingInitialDrainAndCancellation(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	done := make(chan struct{})
	_ = registry.Register("poll", func(context.Context, []byte) error { close(done); return nil })
	if _, err := dispatcher.Dispatch(context.Background(), "poll", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &Worker{DB: db, Registry: registry, PollInterval: 10 * time.Millisecond}
	exited := make(chan error, 1)
	go func() { exited <- worker.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not initially drain queued job")
	}
	cancel()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop on context cancellation")
	}
}

func TestCompleteFallsBackToConservativeRedaction(t *testing.T) {
	db := testDB(t)
	payload := `{"command":"legacy","arguments":{"secret":"value"},"sensitive":["/arguments/missing"]}`
	var job Job
	if err := db.QueryRow(`INSERT INTO jobs(payload,status,attempts,reserved_at,reservation_id) VALUES($1,'reserved',1,clock_timestamp(),'reservation') RETURNING id,queue,payload,attempts,available_at,retry_until,reserved_at,reservation_id`, payload).Scan(&job.ID, &job.Queue, &job.Payload, &job.Attempts, &job.AvailableAt, &job.RetryUntil, &job.ReservedAt, &job.ReservationID); err != nil {
		t.Fatal(err)
	}
	job.Command = "legacy"
	if err := completeJob(context.Background(), db, job); err != nil {
		t.Fatal(err)
	}
	var status, terminal string
	if err := db.QueryRow(`SELECT status,payload::text FROM jobs WHERE id=$1`, job.ID).Scan(&status, &terminal); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || strings.Contains(terminal, "value") || !strings.Contains(terminal, `"arguments": null`) {
		t.Fatalf("completed legacy row status=%s payload=%s", status, terminal)
	}
}

func TestAttemptsAreAuditOnly(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	var calls atomic.Int32
	_ = registry.Register("many-attempts", func(context.Context, []byte) error { calls.Add(1); return nil })
	if _, err := dispatcher.Dispatch(context.Background(), "many-attempts", map[string]string{"value": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE jobs SET attempts=100`); err != nil {
		t.Fatal(err)
	}
	processed, err := (&Worker{DB: db, Registry: registry}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status,attempts FROM jobs`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attempts != 101 || calls.Load() != 1 {
		t.Fatalf("status=%s attempts=%d calls=%d", status, attempts, calls.Load())
	}
}

func TestPermanentHandlerFailsWithoutRetry(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("permanent", func(context.Context, []byte) error { return Permanent(errors.New("private detail")) })
	if _, err := dispatcher.Dispatch(context.Background(), "permanent", map[string]string{"secret": "x"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	processed, err := (&Worker{DB: db, Registry: registry}).ProcessNext(context.Background())
	if err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(`SELECT status,attempts FROM jobs`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 1 {
		t.Fatalf("status=%s attempts=%d", status, attempts)
	}
}

func TestFailureHistoryPreservesSafeDiagnosticsAcrossRetries(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	secretCause := errors.New("provider response contained secret-token")
	_ = registry.Register("history", func(context.Context, []byte) error {
		return Diagnostic("provider_unavailable", "upstream provider is temporarily unavailable", secretCause)
	})
	jobID, err := dispatcher.Dispatch(context.Background(), "history", map[string]string{"secret": "payload-secret"}, SensitiveArguments())
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	for attempt := 1; attempt <= 2; attempt++ {
		if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
			t.Fatalf("ProcessNext %d = %v, %v", attempt, processed, err)
		}
		if attempt == 1 {
			if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp() WHERE id=$1`, jobID); err != nil {
				t.Fatal(err)
			}
		}
	}
	rows, err := db.Query(`SELECT attempts,retry_attempts,error_code,error_message,terminal FROM job_failures WHERE job_id=$1 ORDER BY id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
		var attempts, retryAttempts int
		var code, message string
		var terminal bool
		if err := rows.Scan(&attempts, &retryAttempts, &code, &message, &terminal); err != nil {
			t.Fatal(err)
		}
		if attempts != count || retryAttempts != count || code != "provider_unavailable" || message != "upstream provider is temporarily unavailable" || terminal {
			t.Fatalf("failure %d = attempts=%d retries=%d code=%q message=%q terminal=%v", count, attempts, retryAttempts, code, message, terminal)
		}
		if strings.Contains(message, "secret-token") {
			t.Fatalf("raw cause persisted: %q", message)
		}
	}
	if count != 2 {
		t.Fatalf("failure history count = %d", count)
	}
}

func TestTerminalFailureHistoryAndSuccessHistory(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("permanent-history", func(context.Context, []byte) error {
		return Permanent(Diagnostic("invalid_request", "job request cannot be processed", errors.New("private-secret")))
	})
	_ = registry.Register("successful-history", func(context.Context, []byte) error { return nil })
	permanentID, err := dispatcher.Dispatch(context.Background(), "permanent-history", map[string]string{"secret": "payload-secret"}, SensitiveArguments())
	if err != nil {
		t.Fatal(err)
	}
	successID, err := dispatcher.Dispatch(context.Background(), "successful-history", map[string]string{"value": "ok"})
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	for range 2 {
		if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
			t.Fatalf("ProcessNext = %v, %v", processed, err)
		}
	}
	var code, message, payload string
	var terminal bool
	if err := db.QueryRow(`SELECT f.error_code,f.error_message,f.terminal,j.payload::text FROM job_failures f JOIN jobs j ON j.id=f.job_id WHERE f.job_id=$1`, permanentID).Scan(&code, &message, &terminal, &payload); err != nil {
		t.Fatal(err)
	}
	if code != "invalid_request" || message != "job request cannot be processed" || !terminal || strings.Contains(payload, "payload-secret") || strings.Contains(message, "private-secret") {
		t.Fatalf("terminal failure code=%q message=%q terminal=%v payload=%s", code, message, terminal, payload)
	}
	var successFailures int
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1`, successID).Scan(&successFailures); err != nil {
		t.Fatal(err)
	}
	if successFailures != 0 {
		t.Fatalf("successful job failure history = %d", successFailures)
	}
}

func TestTimeoutUnknownMalformedAndExpiredFailureCodes(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("timeout-history", func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	})
	timeoutID, _ := dispatcher.Dispatch(context.Background(), "timeout-history", map[string]string{"value": "x"})
	unknownID, _ := dispatcher.Dispatch(context.Background(), "unknown-history", map[string]string{"value": "x"})
	expiredID, _ := dispatcher.Dispatch(context.Background(), "expired-history", map[string]string{"value": "x"})
	if _, err := db.Exec(`UPDATE jobs SET available_at=clock_timestamp()-interval '2 seconds',retry_until=clock_timestamp()-interval '1 second' WHERE id=$1`, expiredID); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, JobTimeout: time.Millisecond, ReservationExpiry: time.Second}
	for range 3 {
		if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
			t.Fatalf("ProcessNext = %v, %v", processed, err)
		}
	}
	canceledID, _ := dispatcher.Dispatch(context.Background(), "canceled-history", map[string]string{"value": "x"})
	_ = registry.Register("canceled-history", func(context.Context, []byte) error { return context.Canceled })
	if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
		t.Fatalf("canceled ProcessNext = %v, %v", processed, err)
	}
	for id, want := range map[int64]string{timeoutID: "job_timeout", unknownID: "unknown_command", expiredID: "retry_window_expired", canceledID: "job_canceled"} {
		var got string
		if err := db.QueryRow(`SELECT error_code FROM job_failures WHERE job_id=$1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("job %d code=%q want %q", id, got, want)
		}
	}

	if _, err := db.Exec(`ALTER TABLE jobs DROP CONSTRAINT jobs_payload_check`); err != nil {
		t.Fatal(err)
	}
	var malformedID int64
	if err := db.QueryRow(`INSERT INTO jobs(payload) VALUES ('{"command":"malformed","arguments":{},"sensitive":[42]}') RETURNING id`).Scan(&malformedID); err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.ProcessNext(context.Background()); err != nil || !processed {
		t.Fatalf("malformed ProcessNext = %v, %v", processed, err)
	}
	var malformedCode string
	if err := db.QueryRow(`SELECT error_code FROM job_failures WHERE job_id=$1`, malformedID).Scan(&malformedCode); err != nil {
		t.Fatal(err)
	}
	if malformedCode != "invalid_payload" {
		t.Fatalf("malformed code=%q", malformedCode)
	}
}

func TestFailureSettlementIsAtomicAndStaleOwnerWritesNoHistory(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	jobID, err := dispatcher.Dispatch(context.Background(), "atomic", map[string]string{"secret": "x"}, SensitiveArguments())
	if err != nil {
		t.Fatal(err)
	}
	job, err := claimNext(context.Background(), db, DefaultQueue, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	invalid := failureDiagnostic{Code: "INVALID CODE", Message: "safe"}
	if err := settleFailure(context.Background(), db, job, invalid, nil); err == nil {
		t.Fatal("invalid failure history unexpectedly committed")
	}
	var status, reservationID string
	var failures int
	if err := db.QueryRow(`SELECT status,reservation_id FROM jobs WHERE id=$1`, jobID).Scan(&status, &reservationID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1`, jobID).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if status != "reserved" || reservationID != job.ReservationID || failures != 0 {
		t.Fatalf("atomic rollback status=%s reservation=%s failures=%d", status, reservationID, failures)
	}
	if _, err := db.Exec(`UPDATE jobs SET reservation_id='new-owner' WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	valid := failureDiagnostic{Code: "handler_failed", Message: "job handler returned an error"}
	if err := settleFailure(context.Background(), db, job, valid, nil); !errors.Is(err, ErrStaleReservation) {
		t.Fatalf("stale settlement error=%v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1`, jobID).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 0 {
		t.Fatalf("stale owner wrote %d failures", failures)
	}
}

func TestDiagnosticAndPermanentPreserveCauseIdentity(t *testing.T) {
	cause := errors.New("private provider detail")
	err := Permanent(Diagnostic("provider_failed", "provider operation failed", cause))
	if !errors.Is(err, cause) {
		t.Fatal("wrapped cause identity was lost")
	}
	var permanent *PermanentError
	var diagnostic *DiagnosticError
	if !errors.As(err, &permanent) || !errors.As(err, &diagnostic) {
		t.Fatalf("composition failed: permanent=%#v diagnostic=%#v", permanent, diagnostic)
	}
	got := diagnosticForError(err)
	if got.Code != "provider_failed" || got.Message != "provider operation failed" {
		t.Fatalf("diagnostic=%+v", got)
	}
	fallback := diagnosticForError(errors.New("secret raw error"))
	if fallback.Code != "handler_failed" || strings.Contains(fallback.Message, "secret") {
		t.Fatalf("unsafe fallback=%+v", fallback)
	}
}

func TestRetryBackoffDoublesAndSaturates(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    time.Duration
	}{{1, time.Minute}, {2, 2 * time.Minute}, {3, 4 * time.Minute}, {4, 8 * time.Minute}, {100, time.Duration(1<<63 - 1)}} {
		if got := retryBackoff(test.attempt); got != test.want {
			t.Fatalf("retryBackoff(%d)=%s want %s", test.attempt, got, test.want)
		}
	}
}

func TestListenerWakeFilteringIncludesReconnectNil(t *testing.T) {
	if !notificationRequiresWake("", DefaultQueue) {
		t.Fatal("empty reconnect notification must wake consumers")
	}
	if !notificationRequiresWake(DefaultQueue, DefaultQueue) {
		t.Fatal("matching queue notification must wake consumers")
	}
	if notificationRequiresWake("other", DefaultQueue) {
		t.Fatal("other queue notification must not wake consumers")
	}
}

func TestBroadcastWakeReachesEveryConsumer(t *testing.T) {
	wakes := []chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)}
	broadcastWake(wakes)
	for i, wake := range wakes {
		select {
		case <-wake:
		default:
			t.Fatalf("consumer %d was not woken", i)
		}
	}
}

func TestWorkerShutdownLetsInflightSettleAndStopsNewClaims(t *testing.T) {
	db := testDB(t)
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	_ = registry.Register("shutdown", func(context.Context, []byte) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	for range 2 {
		if _, err := dispatcher.Dispatch(context.Background(), "shutdown", map[string]string{"value": "x"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	worker := &Worker{DB: db, Registry: registry, Concurrency: 1, PollInterval: time.Millisecond, ShutdownGrace: time.Second, SettlementTimeout: 200 * time.Millisecond}
	go func() { exited <- worker.Run(ctx) }()
	<-started
	cancel()
	close(release)
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker shutdown timed out")
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls=%d, claimed new work after cancellation", calls.Load())
	}
	var completed, available int
	if err := db.QueryRow(`SELECT COUNT(*) FILTER(WHERE status='completed'),COUNT(*) FILTER(WHERE status='available') FROM jobs`).Scan(&completed, &available); err != nil {
		t.Fatal(err)
	}
	if completed != 1 || available != 1 {
		t.Fatalf("completed=%d available=%d", completed, available)
	}
}

func TestTerminalPayloadRemainsValidJSON(t *testing.T) {
	data := terminalRedactionFallback("broken")
	if !json.Valid(data) {
		t.Fatalf("fallback is invalid JSON: %s", data)
	}
}
