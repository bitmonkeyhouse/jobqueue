# jobqueue v0.2.0 — release notes

**Module path:** `git.bit-monkey.io/bitmonkey/jobqueue` (unchanged; no `/v2`).
"jobqueue v2" is the architecture name only.

This release makes worker ownership explicit (leases), adds durable
cancellation, per-claim attempt history, an append-only event timeline, a queue
registry with sequencing/concurrency limits, attempt caps, pruning, and a read
API. The v0.1.0 public API keeps compiling unchanged.

---

## Source compatibility

No consumer source change is required to compile. Verified against the live
consumers at this release:

- **indifeed** — `go build ./...` and `go vet ./...` pass unchanged against the
  local v0.2.0 module.
- **steeev/api** — `go build ./...` passes unchanged.

### Preserved v0.1.0 surface

```
Migrate(*sql.DB)
NewDispatcher(*sql.DB) *QueueDispatcher
QueueDispatcher.Dispatch / .DispatchTx(ctx, *sql.Tx, ...)
NewRegistry() *Registry / Registry.Register / Registry.Handle
Worker.Run / Worker.ProcessNext
Handler, Worker, Registry, Option, Payload
Permanent, Diagnostic, SafeDiagnostic
Queue, Delay, RetryWindow, IdempotencyKey, Sensitive, SensitiveArguments, Metadata
ErrStaleReservation
DefaultQueue, DefaultRetryWindow, DefaultNotifyChannel, MigrationTable
```

`DispatchTx(context.Context, *sql.Tx, string, any, ...Option) (int64, error)` is
**unchanged**. pgx support is a separate entry point, `jobqueue.EnqueueTx`, and
does not replace it.

### New (additive)

`EnqueueTx` + `PgxTx`; `EnsureQueue`/`RegisterQueue` + `QueueConfig`;
`SequenceKey`, `SequenceConcurrency`, `MaxAttempts` options; `Worker.WorkerID`,
`LeaseDuration`, `HeartbeatInterval`; `Reap`; `Prune`; `RequestCancel` +
`CancellationResult`; `JobFromContext` + `JobInfo`; `GetJob`, `ListJobs`,
`CountJobs`, `ListAttempts`, `ListEvents`, `QueueStats`; `JobAttempt`,
`JobEvent`, `JobView`; `RecommendedRetention`.

---

## Behavioural and operational changes

1. **Leases replace fixed reservation expiry.** A claim is owned while its lease
   is renewed. The startup rule "`JobTimeout` must be shorter than
   `ReservationExpiry`" is removed; the lease now outlives the handler.
   `ReservationExpiry` is retained as a deprecated alias for the initial
   `LeaseDuration` (setting both is an error).
2. **`cancelled` is a new terminal status.** Any predicate treating
   `completed OR failed` as terminal is now incomplete.
3. **Mixed worker versions are unsupported.** A v0.1.0 worker and a v0.2.0 worker
   must never run against the same database (see the upgrade procedure).
4. **`MaxAttempts`** counts execution claims; `MaxAttempts(1)` means one run.
5. **`NotifyChannel` is deprecated and ignored.** The job and cancellation
   channels are library-owned; a non-default value logs a startup warning.
6. **Retry backoff gains ±20% jitter.**

---

## Required upgrade procedure (v0.1.0 → v0.2.0)

1. Stop accepting/creating new work where necessary.
2. Drain queued/reserved work, or otherwise bring the queue to a clean state.
3. Stop every old worker.
4. Verify there are no `reserved` jobs.
5. Apply the jobqueue schema migration.
6. Deploy/start the new workers.
7. Resume work.

`Migrate` fails closed with a typed `MigrationSafetyError`
(`errors.Is(err, ErrActiveReservations)`) **only while the lease migration is
still pending** (the `jobs` table exists without `lease_expires_at`) and reserved
jobs exist. Once the lease migration is applied, `Migrate` is unconditional and
never rejects startup because jobs are running. `available` jobs do not block
migration: they are unowned and the new workers pick them up.

The module ships migrations `00002` (attempts/events, cascading failure history),
`00003` (queue registry, sequencing, attempt caps, legacy-queue backfill) and
`00004` (leases, cancellation, constraint/index rewrites).

---

## Retention

`Prune` deletes terminal jobs before a caller-supplied cutoff and cascades their
`job_attempts`, `job_events` and `job_failures`. There is no automatic pruning.
`RecommendedRetention` (30 days) is documented guidance for disposable queue
history only; it does not apply to any consumer's domain records.

---

## Consumer audit (2026-09-13)

Searched for terminal-state assumptions equivalent to
`status = 'completed' OR status = 'failed'`,
`status IN ('completed','failed')`, or
`completed_at IS NOT NULL OR failed_at IS NOT NULL`.

### indifeed

- **One assumption, in a test only:** `internal/feedback/dns_provisioning_test.go:541`
  counts `status='completed'`; it stays valid but does not prove terminality.
- No production code branches on the four-status set.
- **`NotifyChannel: "indifeed_jobs"`** (`cmd/server/main.go:268`) is now
  deprecated and ignored. It was already ineffective: indifeed's own trigger
  notifies `'shipworth_jobs'` and the module's default is `'jobs'`, so the
  listener never matched. indifeed has been running on the polling fallback.

### steeev/api

- No terminal-state assumptions found.
- Calls `jobqueue.Migrate`, so it follows the module-owned migration model.

### Other consumers

No other module in the workspace depends on `jobqueue`.

---

## Legacy schema adoption (v0.1.0 → module-owned migrations)

**From v0.2.0 the module owns its schema and migrations.** Consumer-owned copied
jobqueue migrations are legacy/deprecated; `jobqueue.Migrate` is authoritative.

`Migrate` is safe in three cases:

- **Fresh database** — no module history and no queue schema: migrations apply
  from version 1 normally.
- **Module-managed database** — `jobqueue_goose_db_version` exists: normal
  migrations.
- **Legacy v0.1.0 database** — the jobqueue schema exists but
  `jobqueue_goose_db_version` does not: the schema is verified against the
  recognised v0.1.0 baseline, then migration `00001` is stamped as applied
  without executing its DDL, and migrations `00002`–`00004` run through the
  ordinary path (including the pending-lease safety guard). Existing data is not
  modified to establish ownership.

The verification checks tables, required columns and types, primary keys, the
`job_failures` foreign key (including `ON DELETE RESTRICT`), the status,
reservation and terminal constraints, and the indexes v0.1.0 behaviour requires,
and requires the v0.2.0 columns to be absent. Triggers and notification channels
are deliberately not verified: a copied schema may carry a consumer-local
channel, and migration `00002` replaces it with the library trigger.

If an existing jobqueue-like schema does not match the baseline, `Migrate` fails
closed with a typed `LegacyAdoptionError` (`errors.Is(err,
ErrLegacySchemaUnrecognised)`) listing every mismatch. The module never stamps
optimistically, partially migrates, guesses a version or recreates conflicting
objects; the operator resolves an unrecognised schema manually.

### IndiFeed adoption

IndiFeed carries a copy of the v0.1.0 schema in
`indifeed/db/migrations/20260818000000_create_jobs.sql`, applied under IndiFeed's
own `goose_db_version`, and does not currently call `jobqueue.Migrate`. Its
deployed schema matches the canonical v0.1.0 baseline except for the notification
trigger (`notify_shipworth_job` → `shipworth_jobs`), which adoption deliberately
ignores and migration `00002` replaces. `testdata/indifeed_20260818000000_create_jobs.sql`
is a fixture of that exact deployed migration and is exercised by the adoption
tests.

Supported IndiFeed upgrade:

```text
stop accepting new queue work
→ drain queued/reserved work as required
→ stop every v0.1.0 worker
→ verify no reserved jobs
→ deploy version containing jobqueue v0.2.0
→ jobqueue.Migrate adopts the recognised v0.1.0 schema
→ pending lease migration guard passes
→ migrations 00002–00004 apply
→ start v0.2.0 workers
→ resume work
```

v0.1.0 and v0.2.0 workers must never operate concurrently.

After adoption:

1. IndiFeed calls `jobqueue.Migrate` at startup.
2. `20260818000000_create_jobs.sql` is historical only; it was already deployed
   and must not be erased or rewritten in IndiFeed's goose history.
3. Future jobqueue schema changes come **only** from `jobqueue.Migrate`; migrations
   `00002`–`00004` are not copied into IndiFeed.
4. The obsolete `NotifyChannel` setting is removed, since the module owns
   notification channels.
