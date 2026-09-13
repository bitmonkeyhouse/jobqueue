# jobqueue v2 — Implementation plan

This is an incremental plan from the current `git.bit-monkey.io/bitmonkey/jobqueue`
tree to **v0.2.0**, not a greenfield queue and not a Morpheus queue hidden in a
shared package. The module remains generic. Morpheus is the first demanding consumer
and exercises its durable execution, cancellation, retry, lease and sequencing
guarantees.

The current module is v0.1.0. It already has a real PostgreSQL schema, atomic
`FOR UPDATE SKIP LOCKED` claiming, fenced settlement, retries, failure history,
redaction, LISTEN/NOTIFY and a real PostgreSQL test harness. Preserve those
strengths while making the state needed by workers and future admin tooling first
class.

Specification: `docs/jobqueue-v2-spec.md`. That document is normative and
self-contained; this plan implements it. External audits are background only.

**Release identity.** The implementation ships as v0.2.0 on the existing module path
`git.bit-monkey.io/bitmonkey/jobqueue`. "jobqueue v2" is the architecture name, not a
Go semantic-major release. No `/v2` import path is introduced.

## 1. Non-goals and compatibility contract

- Do not add `audit_runs`, projects, reports, findings, analysis modes or any other
  Morpheus table to this module.
- Do not make the job payload a copy of domain configuration. Morpheus enqueues
  `{run_id}` and the worker loads the run from PostgreSQL.
- Do not replace PostgreSQL with SQLite, an in-memory broker or a second service.
- Do not replace the module wholesale. Existing indifeed code uses v0.1.0's
  `NewDispatcher`, `Dispatch`, `DispatchTx(*sql.Tx)`, `NewRegistry`, `Register`,
  `Worker.Run`, `ProcessNext`, `Permanent`, `Diagnostic`, `SafeDiagnostic`,
  `Metadata`, `IdempotencyKey`, `Queue`, `DefaultQueue` and `Option`.
- **Preserve `DispatchTx(context.Context, *sql.Tx, string, any, ...jobqueue.Option)
  (int64, error)` exactly.** Do not widen its `*sql.Tx` parameter and do not replace
  it with a generic transaction interface. indifeed declares its own interface over
  this method (`internal/feedback/queued_email.go:17`) and must keep compiling.
- Add pgx support as a **separate** package-level entry point, `EnqueueTx`, that needs
  no dummy `*sql.DB` and works with `pgx.Tx` and `*pgxpool.Pool`.
- Keep `*sql.DB` and `*sql.Tx` support. Do not force indifeed to migrate to pgx
  before it needs to.
- **Do not attempt mixed-version interoperability.** v0.1.0 (reservation-expiry) and
  v0.2.0 (lease) workers must never run against the same database. The upgrade is
  coordinated and the migration guards it.
- Run all queue concurrency and lifecycle tests against real PostgreSQL. The existing
  isolated-schema harness in `queue_test.go` is the starting point.

## 2. Migration ownership and numbering

`migrate.go` embeds `migrations/*.sql`, runs goose with the PostgreSQL dialect, and
uses the separate `jobqueue_goose_db_version` table. The module owns its schema
migrations; consumers do not copy the SQL into application migrations. A shared
database uses one agreed queue schema and globally namespaced queue names; a consumer
using its own `search_path` keeps that behaviour.

Rules:

1. Keep migrations embedded and monotonic. Never edit an applied migration.
2. Keep `jobqueue_goose_db_version` separate from application migration history.
3. Constraint and index rewrites are explicit migration steps, not incidental to
   adding a nullable column.
4. `Migrate` runs the pre-flight guard (§ WP4) before applying the breaking migration
   and fails clearly when `reserved` jobs exist.

Migrations introduced by this plan (all part of the unshipped v0.2.0, so all are
finalised before the release):

| Migration | Contents | Work package |
| --- | --- | --- |
| `00002_job_attempts_events.sql` | `job_attempts`, `job_events`, cascade FKs, `job_failures` FK changed from `ON DELETE RESTRICT` to `ON DELETE CASCADE`, indexes | WP1 |
| `00003_queue_registry.sql` | `job_queues`, `jobs.sequence_key`, `jobs.sequence_concurrency`, `jobs.max_attempts`, supporting indexes | WP3 |
| `00004_leases_and_cancellation.sql` | **Breaking.** `jobs.worker_id`, `heartbeat_at`, `lease_expires_at`, `cancel_requested_at`, `cancelled_at`; `jobs_status_check` extended with `cancelled`; terminal/reservation `CHECK` rewrites; partial-index rewrites; lease and cancellation indexes. Guarded by the migration pre-flight check. | WP4, WP5 |

`00004` is the single guarded breaking step: one drain covers lease and cancellation
semantics together.

## 3. Work packages

Final implementation order: WP0 → WP1 → WP2 → WP3 → WP4 → WP5 → WP6 → WP7 → WP8 →
WP9 → WP10 → WP11 → WP12.

WP1 and WP2 may proceed in parallel only if their migration and transaction
boundaries are coordinated; do not land event writes that can commit outside the
caller transaction. WP4 must precede any long-running Morpheus job. WP3 should land
before per-project sequencing is enabled. WP4 and WP5 share `00004` and are developed
in order, then validated together.

### WP0 — Freeze the v0.1.0 baseline

**Current implementation:** tagged v0.1.0, indifeed a live consumer, tests
PostgreSQL-only, most production queries private helpers.

**Required change:** record the v0.1.0 public API and run the full suite against the
supported PostgreSQL versions. Add a compatibility consumer test or compile-only
example using the indifeed call patterns, including the `DispatchTx(*sql.Tx)`
interface shape.

**Schema change:** none.
**Public API change:** none.
**Compatibility impact:** none; this is the baseline later packages must preserve.

**Tests:** `go test ./...`; compile the existing indifeed module at its pinned v0.1.0
API; migration test on a fresh isolated schema.
**Acceptance:** baseline green; public symbols listed in release notes; upgrade
database fixture exists.

---

### WP1 — First-class attempts and generic job events

**Current implementation:** `jobs.attempts`/`retry_attempts` are counters;
`job_failures` records failed settlements only; no successful-attempt record, claim
worker, duration, lease history or append-only event sequence.

**Required change:** create generic operational history for every consumer:

- `job_attempts`, one row per claim: claim/finish/heartbeat timestamps, worker id,
  attempt number, outcome, safe error code/message, duration;
- `job_events`, append-only, monotonic `seq`, job id, timestamp, bounded event type
  and bounded structured JSON detail;
- emit events transactionally with enqueue, claim, retry, cancellation, reclaim and
  terminal settlement;
- retain `job_failures` as a compatibility failure history with its existing rows and
  meaning; do not silently remove it.

Events are generic queue facts (`enqueued`, `claimed`, `lease_renewed`, `lease_lost`,
`retry_scheduled`, `cancel_requested`, `cancelled`, `completed`, `failed`,
`reclaimed`), not domain progress (§4 of the spec). Event detail stores no raw
handler errors and no sensitive payloads.

**Schema change:** `00002`. Add `job_attempts` and `job_events` with `ON DELETE
CASCADE` FKs to `jobs`; indexes on `(job_id, seq)` and `(job_id, attempt)` plus
operational query indexes. Change `job_failures.job_id` from `ON DELETE RESTRICT` to
`ON DELETE CASCADE` so WP7 pruning can remove a job's history without orphans.
Use an identity column for `job_events.seq`; document that sequences are not
gap-free and clients resume with `seq > last_seen`.

**Public API change:** internal writes first. Add `JobAttempt`, `JobEvent` and
outcome types; read models land in WP8.
**Compatibility impact:** additive. Existing queries keep working; consumers get
extra writes only.

**Tests:** fresh and v0.1.0-upgraded schemas; one event per required transition;
rollback leaves no enqueue/attempt/event rows; concurrent readers resume from a
sequence; sensitive payload values never appear in event detail; cascade delete
removes attempts/events/failures with the job.
**Acceptance:** a successful, failed, retried and abandoned job each has inspectable
attempt history and an ordered, resumable event timeline.

---

### WP2 — pgx-native transactional enqueue

**Current implementation:** `Dispatch` accepts `*sql.DB`; `DispatchTx` accepts only
`*sql.Tx`. Atomicity works for `database/sql`; it cannot join Morpheus's `pgx.Tx`.

**Required change:** factor payload construction and the insert so both drivers use
the same semantics. **Keep `DispatchTx(*sql.Tx)` exactly as it is.** Add a separate
package-level entry point:

```go
type PgxTx interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func EnqueueTx(ctx context.Context, tx PgxTx, command string, arguments any, options ...Option) (int64, error)
```

`PgxTx` is satisfied by `pgx.Tx` and `*pgxpool.Pool`; no untyped `any` and no runtime
method discovery. Do not require a `*sql.DB` to use it. The insert and the `enqueued`
event run in the caller-supplied transaction. Idempotency conflict returns the
existing active job id without a second job or event.

**Schema change:** none in this package (queue-row creation belongs to WP3).
**Public API change:** add `EnqueueTx` and `PgxTx`; `Dispatch`, `DispatchTx`
unchanged.
**Compatibility impact:** additive. If an interface is introduced anywhere, keep the
concrete `DispatchTx(*sql.Tx)` method so indifeed's interface compiles.

**Tests:** real PostgreSQL with a pgx pool and `pgx.Tx`: commit, rollback,
domain-row-plus-job atomicity, idempotent duplicate dispatch, context cancellation
before commit, two independent connections observing only committed jobs, and
`*pgxpool.Pool` use.
**Acceptance:** Morpheus can call `createAuditRun(tx)` then
`EnqueueTx(tx, "morpheus.audit", {run_id})` and cannot produce either half through
rollback or process failure.

---

### WP3 — Queue registry, sequencing and concurrency

**Current implementation:** `Worker.Concurrency` limits goroutines inside one
process; multiple processes multiply it. No database-backed queue limit, no sequence
key. indifeed approximates priority with separate queues.

**Required change:** add generic claim predicates and queue configuration:

- `job_queues` with `max_concurrency` (NULL = unlimited), `sequence_concurrency`
  (default 1) and `expired_lease_policy` (default `requeue`);
- nullable `sequence_key` and per-job `sequence_concurrency` (default 1);
- enqueue into an unknown queue atomically creates the row with defaults using an
  idempotent `INSERT … ON CONFLICT (name) DO NOTHING`;
- explicit registry API with deterministic semantics:
  - `EnsureQueue(ctx, db, cfg) (bool, error)` — create-if-absent, never mutates an
    existing row (the operation enqueue uses implicitly);
  - `RegisterQueue(ctx, db, cfg) (QueueRegistration, error)` — create-or-update to
    exactly `cfg`, returning created/updated/unchanged. It may both tighten and
    loosen configuration, including restoring `max_concurrency = NULL` (unlimited);
    "only tighten" semantics are deliberately not implemented.
- a blocked candidate is skipped, not claimed and released; FIFO ordering is
  retained;
- count only live, unexpired claims, and serialise the candidate query per queue (or
  per queue and key) with a session advisory lock or a lockable `job_queues` row so
  two workers cannot both pass the count check.

Morpheus uses `queue="morpheus.audit"`, `sequence_key="project:<id>"`,
`sequence_concurrency=1`, `expired_lease_policy=fail`, plus a chosen
`max_concurrency`; it calls `RegisterQueue` before its first enqueue. The module
treats `sequence_key` as opaque text.

**Schema change:** `00003`. `job_queues`; `jobs.sequence_key`,
`jobs.sequence_concurrency`, `jobs.max_attempts` (default 0 = unlimited/window-only);
indexes supporting queue/key/live-claim counts.
**Public API change:** `SequenceKey`, `SequenceConcurrency`, `MaxAttempts`,
`EnsureQueue`, `RegisterQueue`, `QueueConfig`, `QueueRegistration`; queue-wide fields
in query models later. `Queue(name)` and `Worker.Concurrency` unchanged.
**Compatibility impact:** additive. Existing queues get unlimited/default behaviour;
existing jobs have a null sequence key and are never accidentally serialised;
auto-create never overwrites explicit configuration.

**Tests:** real PostgreSQL with multiple pools: two workers cannot claim one job;
queue-wide limit holds across workers; same key is sequential; different keys run
concurrently; blocked old jobs do not prevent eligible work; concurrent first enqueue
creates exactly one row with defaults; `RegisterQueue` updates an existing row
deterministically; `EnsureQueue` does not mutate.
**Acceptance:** a queue limit is global, not per process, and one key's jobs never run
concurrently at `sequence_concurrency = 1`.

---

### WP4 — Leases, worker identity and the coordinated-upgrade guard

**Current implementation:** claim sets `status='reserved'`, `reserved_at` and a
random `reservation_id`; another worker may steal after a fixed `ReservationExpiry`;
no worker identity, heartbeat or renewal. Settlement is fenced by reservation id.
`JobTimeout` must be shorter than the expiry, which is unsafe for long audits.

**Required change:** keep the fenced claim and add explicit ownership:

- retain `reservation_id` (fencing token), `reserved_at` (claim start) and add
  `lease_expires_at` (liveness); do not rename for conceptual cleanup;
- add stable `worker_id` per process with a deployment override, and `heartbeat_at`;
- renew only when `job_id + reservation_id + worker_id` still match;
- renew on `HeartbeatInterval` (default ~10s), well below `LeaseDuration`
  (default ~60s), and stop on handler exit;
- reclaim only when `lease_expires_at < clock_timestamp()`, treating
  `lease_expires_at IS NULL` as expired so no row is unreclaimable;
- settlement continues to fence on `status='reserved' AND reservation_id=$n`;
- add explicit `Reap`/`ReclaimExpired` rather than relying only on the next claim;
- per-queue `expired_lease_policy` (`fail`/`requeue`) applies on expiry, subordinate
  to `MaxAttempts` (if the cap is consumed, settle terminally — see WP6);
- remove the `JobTimeout < ReservationExpiry` startup rule; keep `ReservationExpiry`
  as a deprecated alias for the initial `LeaseDuration` for one release, and reject
  supplying both;
- **add a pre-flight guard in `Migrate`:** the guard is scoped to exactly the
  pending lease migration. While `jobs` exists and does not yet have
  `lease_expires_at`, a non-zero `SELECT count(*) FROM jobs WHERE status='reserved'`
  returns a typed error (for example `ErrActiveReservations` carrying the count) and
  applies nothing. Fresh installs and drained databases proceed, and **once the
  lease migration is applied `Migrate` is unconditional**: a v0.2.0 database with
  live `reserved` jobs restarts normally and `Migrate` never rejects startup because
  a job is running. `available` jobs do not block.

A lease expiry is an ownership fact, not proof the prior handler stopped; keep the
at-least-once warning. Document the required coordinated upgrade (stop work → drain →
stop old workers → verify no `reserved` jobs → migrate → start new workers → resume).

**Schema change:** `00004` adds `worker_id`, `heartbeat_at`, `lease_expires_at` and
the expired-lease/worker indexes. No backfill of already-`reserved` rows is needed
because the guard requires zero `reserved` rows before migrating. Add the guard's
query path.
**Public API change:** `WorkerID`, `LeaseDuration`, `HeartbeatInterval`,
expired-lease policy, `Reap`. `ReservationExpiry` deprecated.
**Compatibility impact:** **not additive.** The meaning of a `reserved` row changes;
the startup rule changes. Mixed v0.1.0/v0.2.0 workers are unsupported and the guard
enforces the drain. Existing workers compile.

**Tests:** two workers cannot settle the same claim; a killed worker is reclaimable
only after lease expiry; a healthy heartbeat prevents theft; a stale owner cannot
renew or settle a reclaimed claim; explicit reaper handles expiry; worker identity is
persisted; graceful shutdown settles or leaves a recoverable lease; **the migration
guard fails with `reserved` rows present on a v0.1.0 schema and applies nothing, then
succeeds after settlement; a fresh install migrates; a v0.2.0 schema with a live
reserved job migrates without error (guard not applied once `lease_expires_at`
exists)**; `NULL` lease is treated as expired.
**Acceptance:** a long-running handler remains owned while heartbeats are healthy, an
abandoned handler is detectable and recoverable without process-start heuristics or
double terminal writes, and an unsafe upgrade fails loudly.

---

### WP5 — Cancellation and the `cancelled` terminal state

**Current implementation:** no cancellation column, status, API, watcher or
cross-process signal; a handler observes only worker/job-timeout cancellation.

**Required change:** queue-owned cancellation mechanics per spec §5.4:

- extend the status set with `cancelled` and **explicitly rewrite** every affected
  constraint and partial index from `00001_create_jobs.sql`: `jobs_status_check`
  (:22), `jobs_reservation_check` (:38), `jobs_terminal_check` (:42),
  `idx_jobs_terminal_history` (:53); add cancellation columns and a pending-cancel
  index;
- `RequestCancel(ctx, jobID)` returns applied / already terminal / already requested:
  - `available` → terminal `cancelled` immediately;
  - `reserved` → persist `cancel_requested_at`, notify, and the owner cancels the
    handler context;
  - terminal → report and do not touch the row;
- durable state first, notification only for latency: persist and notify in the same
  transaction; workers also detect cancellation during heartbeat/lease activity, so a
  lost/disconnected `LISTEN` cannot lose a cancellation;
- the owning worker watches its running jobs and cancels the handler context; the job
  settles `cancelled`, not `failed`; shutdown/lifecycle cancellation is settled
  distinctly and is not reported as a user cancellation;
- cancellation never retries;
- first fenced terminal write wins; a later request reports the result without
  rewriting history;
- authorization stays in the application.

**Schema change:** part of `00004` (with WP4): `cancel_requested_at`, `cancelled_at`;
constraint and index rewrites listed above.
**Public API change:** `RequestCancel`, `CancellationResult`, `JobFromContext` for
attempt/worker identity.
**Compatibility impact:** additive API and migration, but the status set is broader.
Existing consumers must not assume only four statuses. `cancelled` is terminal.
Handlers that ignore context do not stop immediately; the lease and settlement fence
still protect the row, but cancellation cannot interrupt code that ignores context.

**Tests:** queued cancellation never invokes the handler; running cancellation reaches
a handler in another worker/process; cancellation survives a lost notification;
cancellation is not retroactive; races cancellation↔claim, cancellation↔success,
cancellation↔failure, cancellation↔lease expiry each have one terminal winner;
cancelled jobs do not retry; a completed job stays completed.
**Acceptance:** API cancellation works for queued and running jobs across process
boundaries, is durable and observable, and never changes a completed/failed job.

---

### WP6 — Maximum attempts and backoff jitter

**Current implementation:** all non-`Permanent` handler errors retry with exponential
backoff until `retry_until`; no maximum attempt count. `attempts` counts claims
(including re-claims); `retry_attempts` counts released handler failures. No jitter.

**Required change:**

- `MaxAttempts(n)` counts **execution claims**. Increment on a successful claim that
  permits handler execution; `MaxAttempts(1)` means the handler runs at most once;
- on lease expiry, if `max_attempts > 0` and `attempts >= max_attempts`, settle
  terminally as `failed` regardless of the queue's `expired_lease_policy`; a killed
  worker's one-attempt job is never silently re-executed;
- `MaxAttempts(0)`/unset retains window-bounded behaviour and the expired-lease
  policy;
- exhausted jobs settle terminally, never left claimable and invisible;
- retain `retry_attempts` for compatibility and diagnostics only; it drives backoff
  and the retry-window comparison, not `MaxAttempts`; document the distinction on the
  type;
- add bounded jitter (±20%) to retry delays with injectable/random-safe test control;
  keep the saturating bound and the retry-window deadline as a second independent
  bound;
- emit attempt/event records for retry scheduling and terminal exhaustion.

**Schema change:** `jobs.max_attempts` already added in WP3.
**Public API change:** `MaxAttempts(n)`; keep `RetryWindow`, `Permanent`,
`Diagnostic`, `SafeDiagnostic` compatible.
**Compatibility impact:** default behaviour unchanged; consumers opting into a cap
get terminal behaviour. indifeed needs no source change.

**Tests:** backoff timing with a clock or bounded tolerance; jitter bounds;
max-attempts 1 and N; retry-window boundary; permanent error; failure history across
retries; no second handler call after exhaustion; killed worker + `MaxAttempts(1)` is
terminally failed after lease expiry and never re-executed; stale owner writes no
failure history.
**Acceptance:** Morpheus guarantees one execution attempt without wrapping every error
in a domain trick, while other consumers keep generic retries.

---

### WP7 — Pruning, cascading history and retention

**Current implementation:** no prune path. `job_failures.job_id` is `ON DELETE
RESTRICT`, which blocks deleting a job with failure history.

**Required change:** add `Prune(ctx, db, PruneOptions)` that deletes only terminal
jobs (`completed | failed | cancelled`) matching the caller's cutoff/queue filter and
cascades purely job-owned history: `job_attempts`, `job_events`, `job_failures`, and
any future job-owned queue history. Pruning never deletes a non-terminal job and
never leaves orphan history. Retention is a caller policy, separate from correctness:

- no automatic pruning and no default cutoff enforced by the module;
- **30 days is the documented recommended retention window** for disposable jobqueue
  operational history; it is guidance, not an enforced default. The caller invokes
  `Prune`, may choose longer or shorter, and the module never deletes on its own. It
  does **not** apply to Morpheus audit/report history or other consumer domain
  records, which have their own retention policies;
- `PruneOptions` carries cutoff time, optional queue filter and a bounded batch limit;
  `Prune(ctx, db, PruneOptions{TerminalBefore: time.Now().Add(-30*24*time.Hour)})` is
  the documented starting point.

**Schema change:** cascade FKs already in `00002`; add an index supporting
terminal-job prune scans in `00003`/`00004` migrations (covered by the existing
terminal-history index where possible).
**Public API change:** `Prune`, `PruneOptions`.
**Compatibility impact:** additive.

**Tests:** prune removes the job and cascades attempts/events/failures; no orphans
remain; non-terminal jobs are refused; batch limit and cutoff respected; concurrent
settlement during prune is safe.
**Acceptance:** retention is enforceable by an operator call, and pruning cannot
orphan queue history.

---

### WP8 — Read / admin query surface with redaction

**Current implementation:** operators query `jobs`/`job_failures` directly; no
`GetJob`, list/filter, counts, attempt or event API.

**Required change:** small read-side methods/models, not a web UI:

- `GetJob`; `ListJobs` with queue/status/time/worker/sequence filters and bounded
  pagination; `CountJobs` grouped by queue and lifecycle state;
  `ListAttempts(jobID)`; `ListEvents(jobID, afterSeq, limit)`; optional `QueueStats`;
- explicit ordering and pagination; no unbounded result methods;
- **apply `Sensitive` payload redaction for every status**, including `available` and
  `reserved`, using the stored pointers, with the existing conservative fallback on
  failure; return the `JobView` representation from spec §5.11;
- no unrestricted raw-payload API; consumers needing raw arguments use their own
  database access under their own trust boundary.

**Schema change:** add covering/filter indexes only after `EXPLAIN` against realistic
rows: active queue/status/available ordering, worker/lease, attempts by job/time,
events by job/sequence and recent terminal history.
**Public API change:** new query types and methods; `JobView`. Direct SQL remains
possible.
**Compatibility impact:** additive.

**Tests:** filters, stable pagination, counts, event resume from a sequence, attempt
duration/wait calculations, terminal and cancellation fields, redaction of
`available`/`reserved`/`completed` payloads, concurrent writes while reading, and a
test proving no raw payload leaks through the read API.
**Acceptance:** a future reusable admin UI can render queues, counts, jobs, attempts,
worker/lease, retries, cancellation, failures and event timelines through the module
API without private SQL, and never sees unredacted sensitive arguments.

---

### WP9 — Notification cleanup

**Current implementation:** the insert trigger hardcodes `pg_notify('jobs', …)` while
`Worker.NotifyChannel` is read by the listener. A custom channel value is silently
ignored today (indifeed sets `indifeed_jobs` and relies on polling).

**Required change:** make the wake-up channel **library-owned and fixed** for this
phase and stop pretending it is configurable:

- keep `jobs` as the fixed job channel to minimise migration churn; add a
  library-owned cancellation channel for WP5;
- retain `Worker.NotifyChannel` in the struct so existing consumers compile, but mark
  it deprecated and stop reading it for the job channel;
- a non-empty `NotifyChannel` that differs from the library channel logs a startup
  warning naming it as deprecated and ignored;
- document the library-owned channel in README and package docs; schedule removal for
  a later release.

**Schema change:** none beyond the channel used by WP5's notification.
**Public API change:** `NotifyChannel` deprecated (no signature change).
**Compatibility impact:** no break. indifeed keeps compiling and starts receiving
notifications on the library channel (an improvement over v0.1.0's silent polling).

**Tests:** wake-ups delivered on the library channel with `NotifyChannel` unset and
set; a lost `LISTEN` still loses no work and no cancellation; startup warning emitted
for a non-default value.
**Acceptance:** no configuration field appears functional while being ignored.

---

### WP10 — Worker lifecycle, shutdown and observability

**Current implementation:** `Worker.Run` starts `Concurrency` consumers, wakes via
LISTEN/NOTIFY or polling, waits `ShutdownGrace` before cancelling in-flight handlers;
`ProcessNext` serves tests; `slog` only, no metrics hooks.

**Required change:** integrate heartbeat, cancellation watching and queue limits
without hiding lifecycle errors. Define the lifecycle precisely:

1. validate configuration and identify the worker;
2. stop new claims on root cancellation;
3. let active handlers finish during grace;
4. cancel active handler contexts after grace;
5. attempt bounded settlement/release; leave an explicit expired lease if the DB is
   unavailable;
6. return a shutdown/settlement error when work could not be durably accounted for.

Keep `ProcessNext` as a deterministic one-job API. Add structured log fields for
worker, job, attempt, lease and outcome. Defer a Prometheus/OTel hook until the
lifecycle is stable.

**Schema change:** none beyond WP1–WP7.
**Public API change:** worker fields for worker id, lease, heartbeat, expired-lease
policy and cancellation polling; `JobFromContext`. Registry and Handler signatures
unchanged.
**Compatibility impact:** existing handlers still receive `context.Context` and raw
JSON. Defaults documented. `ReservationExpiry` must not silently create an unsafe
mixed-version deployment.

**Tests:** graceful shutdown; no new claims after root cancellation; in-flight
completion; handler cancellation after grace; settlement timeout; LISTEN disconnect
plus polling fallback; multiple independent workers; worker identity in logs and
query results.
**Acceptance:** a worker shuts down without silently losing a claim, and operators can
distinguish normal cancellation, timeout, retry, lease expiry and permanent failure.

---

### WP11 — Documentation, consumer audit, migration upgrade and release

**Current implementation:** README describes v0.1.0 (fixed reservations, retry
windows, `DispatchTx(*sql.Tx)`, direct SQL troubleshooting, at-least-once). No v2
spec or plan in the repo before this change.

**Required change:**

- update README and package docs with the final API and state machine; migration
  guide from v0.1.0; the required coordinated upgrade procedure and the migration
  guard; mixed-version rules; worker shutdown rules; cancellation race semantics;
  lease sizing; idempotency; transactional pgx usage; safe payload diagnostics;
  library-owned notification channels; the `cancelled` terminal state; and the
  attempts/`retry_attempts` distinction;
- **search current consumers** for terminal assumptions equivalent to
  `status = 'completed' OR status = 'failed'`,
  `status IN ('completed','failed')`, or
  `completed_at IS NOT NULL OR failed_at IS NOT NULL`, and document every affected
  location. Indifeed's only such assumptions are in tests; confirm and record;
- keep `docs/jobqueue-v2-spec.md` and `docs/jobqueue-v2-plan.md` in this repository;
  do not depend on a cross-repository audit document for normative requirements;
- release notes call out `cancelled` as a new terminal state, `NotifyChannel` as
  deprecated, and the coordinated-upgrade requirement.

**Schema change:** run every migration upgrade check appropriate to release policy.
Do not promise down migrations that can destroy live history without an explicit
operator action.
**Public API change:** publish under v0.2.0 on the existing module path, with the
indifeed compatibility check.
**Compatibility impact:** announce the coordinated worker upgrade. Update indifeed's
dependency and tests. Morpheus adopts the pgx and lease APIs after the release.

**Tests:** fresh install; v0.1.0 upgrade with the guard (blocked and unblocked);
concurrent `Migrate` calls; indifeed compile and test suite; package examples; race
test where practical; CI PostgreSQL service.
**Acceptance:** a new consumer can understand and operate the queue from the README,
and existing indifeed deployments have a documented, tested upgrade path.

---

### WP12 — Integrate Morpheus after the module release

**Current implementation:** Morpheus creates and executes audit runs in its own
process flow. Its PostgreSQL store uses pgx (on the `postgres` branch); `Execute(runID)`
loads the durable run and refuses an unknown persisted `analysis_mode` rather than
falling back to the worker's process default. Reports are canonical in
`audit_runs.report_markdown`.

**Required change:** in one pgx transaction:

1. create the `audit_run` and persist all immutable execution choices;
2. `jobqueue.EnqueueTx(ctx, tx, "morpheus.audit", {run_id}, Queue, SequenceKey,
   MaxAttempts(1))`;
3. commit both.

Register a generic handler that loads the run by id and calls `Execute(runID)`; it
reads analysis mode, project configuration and report settings from PostgreSQL and
does not trust copied payload configuration. Call `RegisterQueue` for
`morpheus.audit` with `expired_lease_policy = fail`, `sequence_concurrency = 1` and a
bounded `max_concurrency`. Add Morpheus `audit_run_events` as an append-only domain
stream with a per-run sequence and query/resume API. Queue events remain generic
operational history; domain events are not duplicated into them beyond the lifecycle
boundary (spec §4).

**Schema change:** Morpheus-only migration for `audit_run_events`, job-id/attempt
correlation fields, and optional `retry_of_run_id`. No Morpheus fields in jobqueue.
**Public API change:** Morpheus adapter/handler only.
**Compatibility impact:** direct CLI/TUI execution remains possible during staged
migration; both paths write the same domain progress stream; the worker executes
entirely from PostgreSQL.

**Tests:** real PostgreSQL commit/rollback; handler loads from another process;
one-attempt failure; per-project sequential audits; cancellation while queued and
running; unknown analysis mode fails clearly; queue and domain event streams are
independently resumable; canonical report stays database-backed.
**Acceptance:** the architecture is exactly `audit_run + enqueue` in one transaction,
then `jobqueue worker → Execute(run_id)`, with no queue mechanics duplicated in
Morpheus.

## 4. PostgreSQL test matrix

The harness stays PostgreSQL-only and uses two or more independent pools/connections
in the same isolated schema. A second process is represented by a separate worker and
pool; add a subprocess test for real crash/lease behaviour where the harness allows.
Use deterministic barriers/channels around claim, heartbeat, cancellation and
settlement. Do not replace race tests with sleeps except for bounded lease-expiry
checks with generous margins.

| Area | Required real-PostgreSQL test |
| --- | --- |
| Claiming | Two workers cannot claim one job; `SKIP LOCKED` skips a row held by another transaction; FIFO ordering is stable |
| Ownership | Crash leaves a lease; not reclaimed before expiry; healthy heartbeat prevents theft; stale owner cannot settle or renew a reclaimed claim; `NULL` lease is expired; explicit reaper |
| Enqueue | Transactional rollback leaves no domain/job/event rows; commit leaves all; pgx and `database/sql` paths have the same semantics; `DispatchTx(*sql.Tx)` signature unchanged; idempotency returns one active id |
| Queues | Concurrent first enqueue creates one row with defaults; `EnsureQueue` never mutates; `RegisterQueue` creates/updates deterministically; queue-wide limit across independent workers; same key sequential; different keys concurrent; lease expiry frees capacity |
| Cancellation | Queued cancellation; running cross-process cancellation; lost-notification fallback; cancellation/claim, cancellation/success, cancellation/failure and cancellation/lease-expiry races; terminal rows unchanged |
| Retry | Retryable failure/backoff; jitter bounds; retry-window cutoff; permanent error; max attempts N and 1; killed worker + `MaxAttempts(1)` terminal and never re-executed; failure and attempt history preserved |
| Shutdown | No claims after shutdown begins; in-flight handler gets grace; handler cancellation after grace; settlement timeout and lease recovery are visible |
| History | Event sequence resume; ordering; successful and failed attempt duration; cascade prune; counts and filters; safe diagnostics/redaction |
| Deployment | Fresh migration; v0.1.0 upgrade; migration guard blocks with `reserved` jobs and applies nothing; guard passes when drained; concurrent migration calls; LISTEN disconnect plus polling; at least two PostgreSQL versions in CI |
| Read API | Filters, pagination, counts; redaction for available/reserved/completed; no raw-payload leak |

## 5. Deliberately deferred

- **Batch/group implementation:** later as `batch_id`/parent plus a `job_batches`
  aggregate model. Stable job ids, per-job attempts/events and generic query APIs make
  this additive; no speculative parent semantics now.
- **Priority/weighted scheduling:** separate queues express the current need; add a
  priority column/index only when a consumer needs it.
- **Recurring scheduling:** delayed jobs exist; recurring work is a scheduler concern.
- **Admin UI:** build after the query API is exercised by Morpheus and indifeed.
- **Metrics exporter:** durable attempts/events and structured logs come first.
- **Typed payload registry:** handlers own their argument types; the queue validates
  the envelope only.
- **Automatic retention/background pruning:** retention is a caller policy; a
  scheduler is additive.
- **`NotifyChannel` removal:** deprecated in v0.2.0; remove in a later release with a
  proper deprecation cycle.

These are deferred features, not holes in the ownership model. None requires putting
Morpheus concepts into the shared module or rewriting the claim/settlement core.
