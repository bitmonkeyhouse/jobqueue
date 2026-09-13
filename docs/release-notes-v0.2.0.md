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

## OPEN — blocking indifeed's schema upgrade

**This needs a decision; it changes the documented migration-ownership contract
and must not be resolved silently.**

The v2 plan states that the module owns its schema migrations and consumers call
`jobqueue.Migrate`. That is true for **steeev/api** but **not for indifeed**:

- indifeed does **not** call `jobqueue.Migrate`. It carries a full copy of the
  v0.1.0 schema in its own application migration
  (`indifeed/db/migrations/20260818000000_create_jobs.sql` — `jobs`,
  `job_failures`, trigger) applied by indifeed's own goose history
  (`goose_db_version`).
- indifeed documents this as intentional:
  `indifeed/docs/async-jobs.md` — "indifeed's existing local `db/migrations`
  remain authoritative for this schema and are applied by indifeed, not
  `jobqueue.Migrate`."
- indifeed's copied trigger notifies `'shipworth_jobs'`, not `'jobs'`.

Consequences for v0.2.0:

1. indifeed will not receive migrations `00002`–`00004` from `Migrate`. Without
   equivalent migrations its `jobs` table lacks `job_attempts`, `job_events`,
   `job_queues`, `worker_id`, `lease_expires_at`, `cancel_requested_at`,
   `cancelled_at`, `sequence_key` and `max_attempts`, so v0.2.0 runtime code
   fails on the first claim.
2. indifeed cannot simply start calling `jobqueue.Migrate`: its `jobs` table was
   created under `goose_db_version`, while the module tracks
   `jobqueue_goose_db_version`. The module would try to apply `00001` and fail on
   the pre-existing objects. The two migration histories must be reconciled
   first.
3. The drain-before-upgrade guard lives in `jobqueue.Migrate`, which indifeed
   never calls, so indifeed gets no automatic safety check.

### Options

**A. indifeed keeps owning its schema; copy migrations `00002`–`00004` into
`indifeed/db/migrations`.**
Smallest operational change, preserves indifeed's stated model. Cost: two
schemas kept in sync by hand forever, and indifeed must reimplement the
upgrade guard (or rely on the documented manual drain).

**B. indifeed adopts `jobqueue.Migrate` and retires its copy.**
Single source of truth. Cost: reconcile the two goose histories — seed
`jobqueue_goose_db_version` with version 1 (mark `00001` applied) and neutralize
the already-applied local `create_jobs` migration, or add module support for
adopting an existing v0.1.0 schema. This is the only option that makes the
module's guard effective for indifeed.

**C. Add a module-supported adoption path** (e.g. `Migrate` detects an existing
v0.1.0 `jobs` table with no module history and stamps `00001` as applied before
applying `00002`+). Makes option B safe and repeatable for any consumer that
copied v0.1.0. Cost: new behaviour in `Migrate` and a new test surface.

Until this is decided, indifeed stays on v0.1.0 and its dependency must not be
bumped. The reusable module (WP0–WP10) is complete and green; only indifeed's
upgrade steps (WP11) and Morpheus integration (WP12) depend on this decision.
