package jobqueue

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrationTable is the goose version table this module uses so its migration
// history never collides with the consuming application's own goose table.
const MigrationTable = "jobqueue_goose_db_version"

// ErrActiveReservations reports that the lease migration cannot be applied
// safely because reserved jobs still exist.
var ErrActiveReservations = errors.New("jobqueue: reserved jobs present; drain the queue before upgrading")

// MigrationSafetyError is returned by Migrate when crossing the v0.1.0 -> v0.2.0
// lease boundary is unsafe. The guard applies only while the lease migration is
// pending; once lease_expires_at exists, Migrate is unconditional.
type MigrationSafetyError struct {
	ReservedJobs int64
}

func (e *MigrationSafetyError) Error() string {
	return fmt.Sprintf(
		"jobqueue: cannot apply the reservation-to-lease migration while %d reserved job(s) exist; drain the queue and stop old workers before retrying",
		e.ReservedJobs)
}

func (e *MigrationSafetyError) Unwrap() error { return ErrActiveReservations }

// Migrate applies this module's schema to db using an isolated goose history
// table. Call it once at startup, before any Worker or Dispatcher runs.
//
// It fails closed with a MigrationSafetyError when, and only when, the pending
// lease migration cannot be applied safely because reserved jobs exist. Once
// that migration has been applied, later calls never reject startup just because
// jobs are running.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("job queue database is required")
	}
	if err := checkLeaseMigrationSafety(ctx, db); err != nil {
		return err
	}
	fsys, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded job queue migrations: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys,
		goose.WithTableName(MigrationTable))
	if err != nil {
		return fmt.Errorf("create job queue migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply job queue migrations: %w", err)
	}
	return nil
}

// checkLeaseMigrationSafety is scoped to exactly the pending lease migration: a
// jobs table that does not yet have lease_expires_at. Once the column exists the
// upgrade has happened and running jobs are irrelevant.
func checkLeaseMigrationSafety(ctx context.Context, db *sql.DB) error {
	var hasJobs bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('jobs') IS NOT NULL`).Scan(&hasJobs); err != nil {
		return fmt.Errorf("inspect job queue schema: %w", err)
	}
	if !hasJobs {
		return nil
	}
	var hasLeaseColumn bool
	if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'jobs' AND column_name = 'lease_expires_at'
)`).Scan(&hasLeaseColumn); err != nil {
		return fmt.Errorf("inspect job queue schema: %w", err)
	}
	if hasLeaseColumn {
		return nil
	}
	var reserved int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE status='reserved'`).Scan(&reserved); err != nil {
		return fmt.Errorf("count reserved jobs: %w", err)
	}
	if reserved > 0 {
		return &MigrationSafetyError{ReservedJobs: reserved}
	}
	return nil
}
