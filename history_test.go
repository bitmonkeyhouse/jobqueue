package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func eventTypes(t *testing.T, db *sql.DB, jobID int64) []string {
	t.Helper()
	rows, err := db.Query(`SELECT type FROM job_events WHERE job_id=$1 ORDER BY seq`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var types []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		types = append(types, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types
}

func assertEventTypes(t *testing.T, db *sql.DB, jobID int64, want ...string) {
	t.Helper()
	got := eventTypes(t, db, jobID)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("job %d events = %v, want %v", jobID, got, want)
	}
}

func TestEnqueueWritesOneEventPerJob(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)

	first, err := dispatcher.Dispatch(ctx, "history.command", map[string]any{"n": 1}, IdempotencyKey("history-key"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Dispatch(ctx, "history.command", map[string]any{"n": 2}, IdempotencyKey("history-key"))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent dispatch ids = %d, %d", first, second)
	}
	assertEventTypes(t, db, first, string(EventEnqueued))

	var jobs, events int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_events)`).Scan(&jobs, &events); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || events != 1 {
		t.Fatalf("jobs/events = %d/%d, want 1/1", jobs, events)
	}
}

func TestSuccessfulAttemptHistory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	registry := NewRegistry()
	if err := registry.Register("history.ok", func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "history.ok", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}

	var jobID int64
	if err := db.QueryRow(`SELECT id FROM jobs`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, db, jobID, string(EventEnqueued), string(EventClaimed), string(EventCompleted))

	var attempt, duration int
	var outcome, workerID sql.NullString
	var finished sql.NullTime
	if err := db.QueryRow(`SELECT attempt, outcome, finished_at, duration_ms, worker_id FROM job_attempts WHERE job_id=$1`, jobID).
		Scan(&attempt, &outcome, &finished, &duration, &workerID); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || outcome.String != string(OutcomeCompleted) || !finished.Valid || duration < 0 {
		t.Fatalf("attempt row = attempt:%d outcome:%q finished:%v duration:%d", attempt, outcome.String, finished.Valid, duration)
	}
}

func TestRetriedAttemptHistory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	registry := NewRegistry()
	if err := registry.Register("history.retry", func(context.Context, []byte) error {
		return Diagnostic("transient_failure", "provider was unavailable", errors.New("boom"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "history.retry", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var jobID int64
	if err := db.QueryRow(`SELECT id FROM jobs`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, db, jobID, string(EventEnqueued), string(EventClaimed), string(EventRetryScheduled))

	var outcome, code string
	if err := db.QueryRow(`SELECT outcome, error_code FROM job_attempts WHERE job_id=$1 AND attempt=1`, jobID).Scan(&outcome, &code); err != nil {
		t.Fatal(err)
	}
	if outcome != string(OutcomeFailed) || code != "transient_failure" {
		t.Fatalf("attempt outcome/code = %q/%q", outcome, code)
	}
}

func TestDispatchTxRollbackLeavesNoHistory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(db)
	if _, err := dispatcher.DispatchTx(ctx, tx, "history.rollback", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var jobs, events int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_events)`).Scan(&jobs, &events); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || events != 0 {
		t.Fatalf("jobs/events after rollback = %d/%d, want 0/0", jobs, events)
	}
}

func TestHistoryDetailOmitsSensitiveValues(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	registry := NewRegistry()
	if err := registry.Register("history.secret", func(context.Context, []byte) error {
		return Diagnostic("secret_failure", "handler could not use the credential", errors.New("denied"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "history.secret",
		map[string]any{"token": "super-secret-value"}, SensitiveArguments()); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var detail string
	if err := db.QueryRow(`SELECT string_agg(detail::text, ' ') FROM job_events`).Scan(&detail); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(detail, "super-secret-value") {
		t.Fatalf("event detail leaked sensitive value: %s", detail)
	}
	var messages string
	if err := db.QueryRow(`SELECT COALESCE(string_agg(COALESCE(error_message,''), ' '), '') FROM job_attempts`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(messages, "super-secret-value") {
		t.Fatalf("attempt history leaked sensitive value: %s", messages)
	}
}

func TestJobHistoryCascadesWithJob(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	registry := NewRegistry()
	if err := registry.Register("history.cascade", func(context.Context, []byte) error {
		return Permanent(Diagnostic("permanent_failure", "job cannot succeed", errors.New("nope")))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "history.cascade", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	var jobID int64
	if err := db.QueryRow(`SELECT id FROM jobs`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM jobs WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	var attempts, events, failures int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM job_attempts), (SELECT count(*) FROM job_events), (SELECT count(*) FROM job_failures)`).
		Scan(&attempts, &events, &failures); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || events != 0 || failures != 0 {
		t.Fatalf("history after cascade = attempts:%d events:%d failures:%d, want all 0", attempts, events, failures)
	}
}
