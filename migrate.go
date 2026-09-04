package jobqueue

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrationTable is the goose version table this module uses so its migration
// history never collides with the consuming application's own goose table.
const MigrationTable = "jobqueue_goose_db_version"

// Migrate applies this module's schema to db using an isolated goose history
// table. Call it once at startup, before any Worker or Dispatcher runs.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("job queue database is required")
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
