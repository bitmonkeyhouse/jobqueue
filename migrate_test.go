package jobqueue

import (
	"context"
	"testing"
)

func TestMigrateAppliesJobsSchema(t *testing.T) {
	db := openIsolatedSchema(t, "test_jobqueue_migrate_")
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass('jobs') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Migrate did not create the jobs table")
	}
}
