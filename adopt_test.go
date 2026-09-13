package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

const legacyPayload = `{"command":"legacy.command","arguments":{},"sensitive":[]}`

func execMigrationSQL(t *testing.T, db *sql.DB, sqlText string) {
	t.Helper()
	up := strings.Split(sqlText, "-- +goose Down")[0]
	up = strings.Replace(up, "-- +goose Up", "", 1)
	up = strings.ReplaceAll(up, "-- +goose StatementBegin", "")
	up = strings.ReplaceAll(up, "-- +goose StatementEnd", "")
	if strings.TrimSpace(up) == "" {
		t.Fatal("migration has no Up section")
	}
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("apply migration SQL: %v", err)
	}
}

func applyEmbeddedMigration(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	data, err := migrationsFS.ReadFile("migrations/" + name)
	if err != nil {
		t.Fatal(err)
	}
	execMigrationSQL(t, db, string(data))
}

// testDBLegacyV01 builds the recognised v0.1.0 schema without any module
// migration history, as a consumer that copied the schema would have.
func testDBLegacyV01(t *testing.T) *sql.DB {
	t.Helper()
	db := openIsolatedSchema(t, "test_jobqueue_legacy_")
	applyEmbeddedMigration(t, db, "00001_create_jobs.sql")
	return db
}

func relationExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func assertGooseVersion(t *testing.T, db *sql.DB, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(`SELECT max(version_id) FROM ` + MigrationTable + ` WHERE is_applied`).Scan(&got); err != nil {
		t.Fatalf("read migration version: %v", err)
	}
	if got != want {
		t.Fatalf("migration version = %d, want %d", got, want)
	}
}

func TestAdoptLegacyV01Schema(t *testing.T) {
	db := testDBLegacyV01(t)
	ctx := context.Background()
	if relationExists(t, db, MigrationTable) {
		t.Fatal("legacy fixture unexpectedly has module migration history")
	}
	if _, err := db.Exec(`INSERT INTO jobs (queue, payload) VALUES ('legacy.queue', $1::jsonb)`, legacyPayload); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate adopted legacy schema: %v", err)
	}
	assertGooseVersion(t, db, 4)
	if !relationExists(t, db, "job_queues") {
		t.Fatal("job_queues was not created")
	}
	var hasLease bool
	if err := db.QueryRow(`SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema=current_schema() AND table_name='jobs' AND column_name='lease_expires_at')`).Scan(&hasLease); err != nil {
		t.Fatal(err)
	}
	if !hasLease {
		t.Fatal("lease migration was not applied after adoption")
	}
	var jobs int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE queue='legacy.queue'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("legacy job count = %d, want 1", jobs)
	}

	fresh := testDB(t)
	if diff := compareSchemas(t, db, fresh); diff != "" {
		t.Fatalf("adopted schema differs from a fresh v0.2.0 install: %s", diff)
	}
}

func TestAdoptLegacyV01PreservesData(t *testing.T) {
	db := testDBLegacyV01(t)
	ctx := context.Background()

	var availableID, completedID, failedID int64
	if err := db.QueryRow(`INSERT INTO jobs (queue, payload) VALUES ('p.a', $1::jsonb) RETURNING id`, legacyPayload).Scan(&availableID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`INSERT INTO jobs (queue, payload, status, completed_at)
VALUES ('p.b', $1::jsonb, 'completed', clock_timestamp()) RETURNING id`, legacyPayload).Scan(&completedID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`INSERT INTO jobs (queue, payload, status, failed_at, last_error)
VALUES ('p.c', $1::jsonb, 'failed', clock_timestamp(), 'legacy_failure: boom') RETURNING id`, legacyPayload).Scan(&failedID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_failures (job_id, queue, command, attempts, retry_attempts, error_code, error_message, terminal)
VALUES ($1, 'p.c', 'legacy.command', 1, 0, 'legacy_failure', 'boom', true)`, failedID); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	assertJob := func(id int64, wantStatus string) {
		t.Helper()
		var status, queue, payload string
		if err := db.QueryRow(`SELECT status, queue, payload::text FROM jobs WHERE id=$1`, id).Scan(&status, &queue, &payload); err != nil {
			t.Fatal(err)
		}
		if status != wantStatus {
			t.Fatalf("job %d status = %q, want %q", id, status, wantStatus)
		}
		if !strings.Contains(payload, "legacy.command") {
			t.Fatalf("job %d payload changed: %s", id, payload)
		}
	}
	assertJob(availableID, "available")
	assertJob(completedID, "completed")
	assertJob(failedID, "failed")

	var failures int
	if err := db.QueryRow(`SELECT count(*) FROM job_failures WHERE job_id=$1`, failedID).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Fatalf("failure history rows = %d, want 1", failures)
	}
	var lastError string
	if err := db.QueryRow(`SELECT last_error FROM jobs WHERE id=$1`, failedID).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError != "legacy_failure: boom" {
		t.Fatalf("last_error changed to %q", lastError)
	}
}

func TestAdoptLegacyUnsafeReservationFailsClosed(t *testing.T) {
	db := testDBLegacyV01(t)
	ctx := context.Background()
	var id int64
	if err := db.QueryRow(`INSERT INTO jobs (queue, payload, status, reserved_at, reservation_id)
VALUES ('legacy.r', $1::jsonb, 'reserved', clock_timestamp(), 'legacy-reservation') RETURNING id`, legacyPayload).Scan(&id); err != nil {
		t.Fatal(err)
	}

	err := Migrate(ctx, db)
	if !errors.Is(err, ErrActiveReservations) {
		t.Fatalf("Migrate with a reserved job = %v, want ErrActiveReservations", err)
	}
	// Adoption stamped 00001, but no unsafe migration was applied.
	assertGooseVersion(t, db, 1)
	if relationExists(t, db, "job_attempts") {
		t.Fatal("00002 was partially applied despite the safety guard")
	}
	var hasLease bool
	if err := db.QueryRow(`SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema=current_schema() AND table_name='jobs' AND column_name='lease_expires_at')`).Scan(&hasLease); err != nil {
		t.Fatal(err)
	}
	if hasLease {
		t.Fatal("lease migration was applied despite the safety guard")
	}

	// Drain the queue, then the coordinated upgrade completes.
	if _, err := db.Exec(`UPDATE jobs SET status='failed', failed_at=clock_timestamp(), reserved_at=NULL, reservation_id=NULL WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate after draining: %v", err)
	}
	assertGooseVersion(t, db, 4)
}

func TestAdoptLegacyUnrecognisedFailsClosed(t *testing.T) {
	db := testDBLegacyV01(t)
	ctx := context.Background()
	if _, err := db.Exec(`ALTER TABLE jobs DROP COLUMN last_error`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, db)
	if !errors.Is(err, ErrLegacySchemaUnrecognised) {
		t.Fatalf("Migrate on an unrecognised schema = %v, want ErrLegacySchemaUnrecognised", err)
	}
	var adoption *LegacyAdoptionError
	if !errors.As(err, &adoption) || len(adoption.Reasons) == 0 {
		t.Fatalf("expected a LegacyAdoptionError with reasons, got %#v", err)
	}
	if relationExists(t, db, MigrationTable) {
		t.Fatal("unrecognised schema was stamped with migration 00001")
	}
}

func TestAdoptLegacyUnrecognisedConstraintFailsClosed(t *testing.T) {
	db := testDBLegacyV01(t)
	ctx := context.Background()
	if _, err := db.Exec(`ALTER TABLE jobs DROP CONSTRAINT jobs_status_check`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('available','reserved','completed','failed','cancelled'))`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, db)
	if !errors.Is(err, ErrLegacySchemaUnrecognised) {
		t.Fatalf("Migrate with an altered status constraint = %v, want ErrLegacySchemaUnrecognised", err)
	}
	if relationExists(t, db, MigrationTable) {
		t.Fatal("altered schema was stamped with migration 00001")
	}
}

func TestMigrateFreshDatabaseIsUnaffected(t *testing.T) {
	db := openIsolatedSchema(t, "test_jobqueue_fresh_")
	ctx := context.Background()
	if relationExists(t, db, "jobs") || relationExists(t, db, MigrationTable) {
		t.Fatal("fresh schema is not empty")
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertGooseVersion(t, db, 4)
}

func TestAdoptIndifeedDeployedFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/indifeed_20260818000000_create_jobs.sql")
	if err != nil {
		t.Fatal(err)
	}
	db := openIsolatedSchema(t, "test_jobqueue_indifeed_")
	ctx := context.Background()
	execMigrationSQL(t, db, string(data))
	if relationExists(t, db, MigrationTable) {
		t.Fatal("indifeed fixture unexpectedly has module migration history")
	}
	if _, err := db.Exec(`INSERT INTO jobs (queue, payload) VALUES ('indifeed.queue', $1::jsonb)`, legacyPayload); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("adopt indifeed deployed schema: %v", err)
	}
	assertGooseVersion(t, db, 4)

	// The consumer-local trigger/channel is replaced by the canonical one.
	var function string
	if err := db.QueryRow(`
SELECT p.proname
FROM pg_trigger t
JOIN pg_proc p ON p.oid = t.tgfoid
WHERE t.tgname = 'jobs_notify_after_insert' AND t.tgrelid = 'jobs'::regclass`).Scan(&function); err != nil {
		t.Fatalf("inspect trigger function: %v", err)
	}
	if function != "notify_job" {
		t.Fatalf("notification trigger uses %q, want notify_job", function)
	}
	var definition string
	if err := db.QueryRow(`SELECT pg_get_functiondef('notify_job'::regproc)`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(definition, "pg_notify('jobs'") {
		t.Fatalf("canonical trigger does not notify the library channel: %s", definition)
	}
	var jobs int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE queue='indifeed.queue'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("indifeed job count = %d, want 1", jobs)
	}
}

// compareSchemas returns a human-readable difference between two databases'
// queue tables, or "" when they match in columns, indexes and constraints.
func compareSchemas(t *testing.T, a, b *sql.DB) string {
	t.Helper()
	left := schemaSnapshot(t, a)
	right := schemaSnapshot(t, b)
	if reflect.DeepEqual(left, right) {
		return ""
	}
	var differences []string
	for key, want := range right {
		got := left[key]
		if !reflect.DeepEqual(got, want) {
			differences = append(differences, key)
		}
	}
	return "differing " + strings.Join(differences, ", ")
}

func schemaSnapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	snapshot := make(map[string][]string)
	for _, table := range []string{"jobs", "job_failures", "job_attempts", "job_events", "job_queues"} {
		snapshot[table+".columns"] = queryStrings(t, db, `
SELECT column_name || ':' || data_type || ':' || is_nullable
FROM information_schema.columns
WHERE table_schema=current_schema() AND table_name=$1
ORDER BY column_name`, table)
		snapshot[table+".indexes"] = queryStrings(t, db, `
SELECT indexname FROM pg_indexes
WHERE schemaname=current_schema() AND tablename=$1
ORDER BY indexname`, table)
		snapshot[table+".constraints"] = queryStrings(t, db, `
SELECT conname || ':' || pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = $1::regclass
ORDER BY conname`, table)
	}
	return snapshot
}

func queryStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}
