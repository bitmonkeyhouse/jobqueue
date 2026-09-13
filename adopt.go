package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// ErrLegacySchemaUnrecognised reports that an existing jobqueue-like schema
// without module migration history does not match the recognised v0.1.0
// baseline, so it cannot be adopted automatically.
var ErrLegacySchemaUnrecognised = errors.New("jobqueue: existing schema is not a recognised v0.1.0 jobqueue schema")

// LegacyAdoptionError lists every reason a legacy schema failed adoption checks.
// The operator must resolve an unrecognised schema manually; the module never
// guesses, stamps optimistically or recreates conflicting objects.
type LegacyAdoptionError struct {
	Reasons []string
}

func (e *LegacyAdoptionError) Error() string {
	return fmt.Sprintf("jobqueue: cannot adopt the existing schema as v0.1.0: %s", strings.Join(e.Reasons, "; "))
}

func (e *LegacyAdoptionError) Unwrap() error { return ErrLegacySchemaUnrecognised }

// adoptLegacySchema gives a database that already has the v0.1.0 jobqueue schema
// but no jobqueue_goose_db_version a module-owned migration history. It verifies
// the schema against the recognised baseline, then records migration 00001 as
// already applied without executing its DDL and without touching existing data.
//
// Fresh databases and already module-managed databases are left untouched.
func adoptLegacySchema(ctx context.Context, db *sql.DB) error {
	store, err := database.NewStore(goose.DialectPostgres, MigrationTable)
	if err != nil {
		return fmt.Errorf("create job queue migration store: %w", err)
	}
	ext, ok := store.(database.StoreExtender)
	if !ok {
		return errors.New("job queue migration store does not support adoption checks")
	}
	historyExists, err := ext.TableExists(ctx, db)
	if err != nil {
		return fmt.Errorf("inspect job queue migration history: %w", err)
	}
	if historyExists {
		return nil
	}
	hasJobs, err := schemaTableExists(ctx, db, "jobs")
	if err != nil {
		return err
	}
	if !hasJobs {
		return nil
	}
	if err := verifyLegacyV01Baseline(ctx, db); err != nil {
		return err
	}
	if err := store.CreateVersionTable(ctx, db); err != nil {
		return fmt.Errorf("create job queue migration history: %w", err)
	}
	if err := store.Insert(ctx, db, database.InsertRequest{Version: 1}); err != nil {
		return fmt.Errorf("adopt legacy v0.1.0 schema: %w", err)
	}
	return nil
}

type columnSpec struct {
	dataType string
	nullable bool
}

var legacyJobsColumns = map[string]columnSpec{
	"id":              {"bigint", false},
	"queue":           {"text", false},
	"payload":         {"jsonb", false},
	"idempotency_key": {"text", true},
	"status":          {"text", false},
	"attempts":        {"integer", false},
	"retry_attempts":  {"integer", false},
	"available_at":    {"timestamp with time zone", false},
	"retry_until":     {"timestamp with time zone", false},
	"reserved_at":     {"timestamp with time zone", true},
	"reservation_id":  {"text", true},
	"completed_at":    {"timestamp with time zone", true},
	"failed_at":       {"timestamp with time zone", true},
	"last_error":      {"text", true},
	"created_at":      {"timestamp with time zone", false},
	"updated_at":      {"timestamp with time zone", false},
}

var legacyJobFailuresColumns = map[string]columnSpec{
	"id":             {"bigint", false},
	"job_id":         {"bigint", false},
	"queue":          {"text", false},
	"command":        {"text", false},
	"attempts":       {"integer", false},
	"retry_attempts": {"integer", false},
	"error_code":     {"text", false},
	"error_message":  {"text", false},
	"terminal":       {"boolean", false},
	"occurred_at":    {"timestamp with time zone", false},
}

// v2ExclusiveJobsColumns must be absent from a clean v0.1.0 baseline; if any is
// present the schema is a partial/unrecognised state this module will not touch.
var v2ExclusiveJobsColumns = []string{
	"sequence_key", "sequence_concurrency", "max_attempts", "worker_id",
	"heartbeat_at", "lease_expires_at", "cancel_requested_at", "cancelled_at",
}

var legacyRequiredIndexes = []string{
	"idx_jobs_available",
	"idx_jobs_expired_reservations",
	"idx_jobs_active_idempotency",
	"idx_jobs_terminal_history",
	"idx_job_failures_job_history",
	"idx_job_failures_operations",
}

var legacyRequiredConstraints = []string{
	"jobs_queue_nonempty",
	"jobs_idempotency_key_nonempty",
	"jobs_status_check",
	"jobs_attempts_check",
	"jobs_retry_attempts_check",
	"jobs_retry_window_check",
	"jobs_payload_check",
	"jobs_reservation_check",
	"jobs_terminal_check",
	"job_failures_queue_nonempty",
	"job_failures_command_nonempty",
	"job_failures_attempts_check",
	"job_failures_error_code_check",
	"job_failures_error_message_check",
}

// Triggers and notification functions are deliberately not verified: a copied
// v0.1.0 schema may carry a consumer-local channel, and migration 00002 replaces
// it with the canonical library trigger.

func verifyLegacyV01Baseline(ctx context.Context, db *sql.DB) error {
	var reasons []string
	for _, table := range []string{"jobs", "job_failures"} {
		exists, err := schemaTableExists(ctx, db, table)
		if err != nil {
			return err
		}
		if !exists {
			reasons = append(reasons, fmt.Sprintf("table %q is missing", table))
		}
	}
	if len(reasons) > 0 {
		return &LegacyAdoptionError{Reasons: reasons}
	}

	jobsColumns, err := tableColumns(ctx, db, "jobs")
	if err != nil {
		return err
	}
	failureColumns, err := tableColumns(ctx, db, "job_failures")
	if err != nil {
		return err
	}
	reasons = append(reasons, checkColumns("jobs", jobsColumns, legacyJobsColumns)...)
	reasons = append(reasons, checkColumns("job_failures", failureColumns, legacyJobFailuresColumns)...)
	for _, column := range v2ExclusiveJobsColumns {
		if _, exists := jobsColumns[column]; exists {
			reasons = append(reasons, fmt.Sprintf("jobs.%s already exists; schema is not a clean v0.1.0 baseline", column))
		}
	}

	indexes, err := tableIndexes(ctx, db, "jobs")
	if err != nil {
		return err
	}
	failureIndexes, err := tableIndexes(ctx, db, "job_failures")
	if err != nil {
		return err
	}
	for _, name := range legacyRequiredIndexes {
		if !indexes[name] && !failureIndexes[name] {
			reasons = append(reasons, fmt.Sprintf("index %q is missing", name))
		}
	}

	constraints, err := tableConstraints(ctx, db, "jobs")
	if err != nil {
		return err
	}
	failureConstraints, err := tableConstraints(ctx, db, "job_failures")
	if err != nil {
		return err
	}
	for _, name := range legacyRequiredConstraints {
		if _, ok := constraints[name]; !ok {
			if _, ok := failureConstraints[name]; !ok {
				reasons = append(reasons, fmt.Sprintf("constraint %q is missing", name))
			}
		}
	}

	if definition, ok := constraints["jobs_status_check"]; ok {
		for _, status := range []string{"available", "reserved", "completed", "failed"} {
			if !strings.Contains(definition, "'"+status+"'") {
				reasons = append(reasons, fmt.Sprintf("jobs_status_check does not allow status %q", status))
			}
		}
		if strings.Contains(definition, "cancelled") {
			reasons = append(reasons, "jobs_status_check already allows cancelled; schema is not a clean v0.1.0 baseline")
		}
	}
	if definition, ok := constraints["jobs_reservation_check"]; ok {
		if !strings.Contains(definition, "reserved_at") || !strings.Contains(definition, "reservation_id") {
			reasons = append(reasons, "jobs_reservation_check does not tie reserved_at and reservation_id to the reserved status")
		}
	}
	if definition, ok := constraints["jobs_terminal_check"]; ok {
		if !strings.Contains(definition, "completed_at") || !strings.Contains(definition, "failed_at") {
			reasons = append(reasons, "jobs_terminal_check does not tie completed_at and failed_at to terminal statuses")
		}
	}
	if definition, ok := constraints["jobs_payload_check"]; ok {
		if !strings.Contains(definition, "payload") {
			reasons = append(reasons, "jobs_payload_check does not constrain the payload")
		}
	}

	fk, err := foreignKeyInfo(ctx, db, "job_failures_job_id_fkey")
	if err != nil {
		return err
	}
	switch {
	case !fk.exists:
		reasons = append(reasons, "foreign key job_failures_job_id_fkey is missing")
	case !fk.referencesJobs:
		reasons = append(reasons, "foreign key job_failures_job_id_fkey does not reference jobs(id)")
	case fk.deleteRule != "r":
		reasons = append(reasons, "foreign key job_failures_job_id_fkey is not ON DELETE RESTRICT")
	}

	if len(reasons) > 0 {
		sort.Strings(reasons)
		return &LegacyAdoptionError{Reasons: reasons}
	}
	return nil
}

func checkColumns(table string, actual map[string]columnSpec, expected map[string]columnSpec) []string {
	var reasons []string
	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			reasons = append(reasons, fmt.Sprintf("%s.%s is missing", table, name))
			continue
		}
		if got.dataType != want.dataType {
			reasons = append(reasons, fmt.Sprintf("%s.%s type is %q, want %q", table, name, got.dataType, want.dataType))
		}
		if got.nullable != want.nullable {
			reasons = append(reasons, fmt.Sprintf("%s.%s nullability differs from v0.1.0", table, name))
		}
	}
	return reasons
}

func schemaTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = current_schema() AND table_name = $1
)`, table).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect schema table %q: %w", table, err)
	}
	return exists, nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]columnSpec, error) {
	rows, err := db.QueryContext(ctx, `
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = $1`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]columnSpec)
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, err
		}
		columns[name] = columnSpec{dataType: dataType, nullable: nullable == "YES"}
	}
	return columns, rows.Err()
}

func tableIndexes(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
SELECT indexname FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = $1`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s indexes: %w", table, err)
	}
	defer rows.Close()
	indexes := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		indexes[name] = true
	}
	return indexes, rows.Err()
}

func tableConstraints(ctx context.Context, db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = $1::regclass`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s constraints: %w", table, err)
	}
	defer rows.Close()
	constraints := make(map[string]string)
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return nil, err
		}
		constraints[name] = definition
	}
	return constraints, rows.Err()
}

type foreignKey struct {
	exists         bool
	referencesJobs bool
	deleteRule     string
}

func foreignKeyInfo(ctx context.Context, db *sql.DB, name string) (foreignKey, error) {
	var referencesJobs bool
	var deleteRule string
	err := db.QueryRowContext(ctx, `
SELECT confrelid = 'jobs'::regclass, confdeltype
FROM pg_constraint
WHERE conname = $1 AND contype = 'f' AND conrelid = 'job_failures'::regclass`, name).Scan(&referencesJobs, &deleteRule)
	if errors.Is(err, sql.ErrNoRows) {
		return foreignKey{}, nil
	}
	if err != nil {
		return foreignKey{}, fmt.Errorf("inspect foreign key %q: %w", name, err)
	}
	return foreignKey{exists: true, referencesJobs: referencesJobs, deleteRule: deleteRule}, nil
}
