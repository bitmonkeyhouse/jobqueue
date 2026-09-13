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

func TestMigrateUpgradesFromV01(t *testing.T) {
	db := testDBAtVersion(t, 1)
	// A v0.1.0 job in a queue that predates the registry must stay claimable.
	legacyPayload := `{"command":"legacy.command","arguments":{},"sensitive":[]}`
	if _, err := db.Exec(`INSERT INTO jobs (queue, payload) VALUES ('legacy.queue', $1::jsonb)`, legacyPayload); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("upgrade from v0.1.0: %v", err)
	}
	for _, table := range []string{"job_attempts", "job_events"} {
		var exists bool
		if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("upgrade did not create %s", table)
		}
	}
	var backfilled bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM job_queues WHERE name='legacy.queue')`).Scan(&backfilled); err != nil {
		t.Fatal(err)
	}
	if !backfilled {
		t.Fatal("upgrade did not backfill the legacy queue into job_queues")
	}
	// The failure-history FK must now cascade so Prune can remove a job's history.
	var deleteRule string
	if err := db.QueryRow(`
SELECT confdeltype FROM pg_constraint
WHERE conname = 'job_failures_job_id_fkey'`).Scan(&deleteRule); err != nil {
		t.Fatal(err)
	}
	if deleteRule != "c" {
		t.Fatalf("job_failures delete rule = %q, want cascade", deleteRule)
	}
}
