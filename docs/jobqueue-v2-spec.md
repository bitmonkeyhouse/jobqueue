# jobqueue v2 — Specification

Target behaviour for the shared queue at `git.bit-monkey.io/bitmonkey/jobqueue`,
currently v0.1.0, as it becomes infrastructure for Morpheus and the other Bit
Monkey products.

**Release identity.** "jobqueue v2" is the name of this architecture and
specification, not a Go semantic-major release. The implementation ships as
**v0.2.0** on the existing module path:

```text
git.bit-monkey.io/bitmonkey/jobqueue
```

There is no `/v2` import path and none will be introduced. Consumers keep
importing the module path they use today.

Companion documents:

- `docs/jobqueue-v2-plan.md` — how v0.1.0 becomes v0.2.0, work package by work package.
- `README.md` — the v0.1.0 user guide, which this spec extends rather than replaces.

This specification is normative and self-contained. Any external audit,
including `morpheus/docs/jobqueue-audit.md`, is **background only**: it may
explain motivation and may be cited for context, but it must not carry a
requirement that is absent from this document. Where an external document and
this specification disagree, this specification wins.

Written 2026-09-13 against the tree at `d748e60`.

---

## 1. What v0.1.0 already is

Everything below is fact, not intent, and each line has a citation. The plan builds
on this rather than replacing it: the schema, the claim statement, the settlement
fencing and the failure history are all sound and already in use.

| Area | v0.1.0 |
| --- | --- |
| Persistence | PostgreSQL only. `jobs` and `job_failures` (`migrations/00001_create_jobs.sql`), created by `Migrate` with an isolated goose history table `jobqueue_goose_db_version` (`migrate.go:18,22`) so a consumer's own migrations never collide |
| Payload | `jsonb` with a database-level shape constraint: `command`, `arguments` (object/array/null), `sensitive` (JSON pointers), optional `metadata` (`00001_create_jobs.sql:23-37`) |
| Enqueue | `Dispatch` and `DispatchTx(ctx, *sql.Tx, …)` (`queue.go:23,28`), the latter inside the caller's `database/sql` transaction, so enqueue commits and rolls back with the caller's work |
| Idempotency | `IdempotencyKey` (`payload.go:71`) with a partial unique index over active jobs; a conflicting dispatch returns the existing job's id (`queue.go:52-59`) |
| Claim | One statement: `FOR UPDATE SKIP LOCKED` over a candidate CTE, then `UPDATE … SET status='reserved', attempts=attempts+1, reserved_at, reservation_id` (`queue.go:112-145`). Ordering is `available_at, id` — FIFO, no priority |
| Settlement fencing | `completeJob` and `settleFailure` match `status='reserved' AND reservation_id=$n`; a superseded worker gets `ErrStaleReservation` and the worker logs it as superseded (`queue.go:176-195,257`, `worker.go:290-296`) |
| Retries | Handler error → back to `available` with backoff `1m << (retry_attempts-1)`, saturating (`worker.go:412`), bounded by `retry_until` (the retry window, default 24h from first availability); `Permanent(err)` fails immediately (`worker.go:18-27`) |
| Failure history | `job_failures` rows per failure with a bounded, operator-safe code and message (`errors.go:49-90`), `terminal` flag, and attempts at failure |
| Secrets | `Sensitive` JSON pointers; the payload is redacted at settlement (`payload.go:270`), with a conservative fallback if redaction fails |
| Worker | `Registry` of command → handler (`worker.go:29`), `Concurrency` goroutines per process, `JobTimeout` (handler deadline), `ReservationExpiry`, `PollInterval`, `ShutdownGrace`, `SettlementTimeout`, `slog` logging (`worker.go:66-81`) |
| Wake-ups | An `AFTER INSERT` trigger `pg_notify`s the queue name; the worker LISTENs with a pgx connection and polls as a fallback, with the poll delay derived from the next `available_at` (`worker.go:148-190`, `queue.go:271`) |
| Shutdown | Root cancellation stops new claims; in-flight handlers get `ShutdownGrace` before their contexts are cancelled; settlement gets `SettlementTimeout` (`worker.go:104-135`) |
| Crash recovery | Reservation expiry only: a claim older than `ReservationExpiry` (default 90s) may be stolen by another worker. No heartbeat, no lease renewal, and `JobTimeout` must be shorter than `ReservationExpiry` or the worker refuses to start (`worker.go:345-355`) |
| Lifecycle | `available → reserved → completed \| failed`, with `available` again on retry (`00001_create_jobs.sql:20`). No `cancelled`, no `running` distinct from `reserved` |
| Observability | `job_failures` history, terminal-history and expired-reservation indexes, `slog` on attempt failure and superseded settlement. No metrics hooks, no query API, no per-attempt records, no duration/wait-time data |
| Consumers | `indifeed` at `v0.1.0` (dispatcher, registry, two queues as two workers, `DispatchTx` on `*sql.Tx`, `Permanent`/`Diagnostic`). Its tests call `ProcessNext` directly (`indifeed/internal/feedback/*`) |
| Tests | 39 tests over a real PostgreSQL in an isolated schema per test via `options=-csearch_path=…` (`queue_test.go:22-80`): claim exclusivity, stale fencing, expiry, retry boundaries, redaction, shutdown, backoff saturation |

Two v0.1.0 facts matter for grading the upgrade:

- **`job_failures.job_id` is `ON DELETE RESTRICT`** (`00001_create_jobs.sql:64-65`).
  A job cannot be deleted while any failure row exists.
- **`Worker.NotifyChannel` is currently ineffective.** The trigger hardcodes
  `pg_notify('jobs', NEW.queue)` (`00001_create_jobs.sql:88`) while the worker
  LISTENs on `w.notifyChannel()` (`worker.go:157-171`). A consumer that sets a
  custom channel (indifeed sets `indifeed_jobs`) receives no notifications and
  survives on the polling fallback alone. This is a v0.1.0 defect, not a feature.
  §5.9 resolves it.

**What v0.1.0 is not:** there is no cancellation, no worker identity or heartbeat, no
lease, no maximum attempts, no priority, no per-key sequencing, no queue-wide
concurrency limit, no per-attempt history, no job events, no batch/group concept, no
query API, and no pgx-native path.

---

## 2. Decisions this spec must satisfy

These are normative requirements of this document. They originated in consumer
reconnaissance, but they are recorded here so that this repository's specification
stands alone and no external document can change the contract.

1. **Progress is persisted and append-only.** A client must be able to inspect
   current progress, retrieve prior progress, reconnect to a stream, resume from a
   known sequence, and observe work done by another process. Where the ownership
   boundary sits is decided in §4.
2. **Cancellation mechanics belong to the queue; authorization does not.** The
   queue supports requesting cancellation of queued and running jobs and propagates
   it to the handler context. Whether a caller may cancel is the application's
   decision. Terminal jobs are never retroactively marked cancelled.
3. **Retries are generic; Morpheus's policy is not encoded in the queue.** The queue
   supports retry/backoff because other consumers need it. Morpheus runs one
   execution attempt per audit run: a failed audit must not silently rerun and
   append more evidence, findings and cost to the same run. A user-visible retry
   creates a new run (`retry_of_run_id` is a Morpheus column, not a queue concept).
4. **Reports are solved and out of scope here.** `audit_runs.report_markdown` is
   canonical; workers and API processes need no shared filesystem.
5. **Ownership is explicit.** A process starting must never infer that every
   `reserved` job belongs to a dead process. Claims, leases and heartbeats are
   durable; expired ownership is detectable and recoverable according to queue
   policy. Morpheus wants an abandoned audit to become `failed`, not to be rerun
   automatically.
6. **Transactional enqueue is required.** An application must be able to create its
   domain row and enqueue the job in one transaction: never a run without a job,
   never a job whose run rolled back. Morpheus writes with pgx natively, so this
   must work from a `pgx.Tx` as well as a `*sql.Tx`.
7. **Sequencing is a shared requirement.** Queue-wide concurrency limits and
   optional per-key sequential execution, generically: Morpheus wants
   `queue = "morpheus.audit"`, `sequence_key = "project:<id>"`, one at a time, while
   allowing several audits overall. No Morpheus or project concepts in the module.
8. **Room for batching.** Generic grouping (many URLs, bulk email, competitor
   analysis, imports) must not be expensive to add later.
9. **Admin-UI-shaped state.** The persistence and query model must expose queues,
   counts by state, individual jobs, attempts, duration, worker/claim, lease, retry
   state, cancellation, failure and later group membership. The UI itself is not
   built now.
10. **Release compatibility.** v0.2.0 uses the existing module path
    `git.bit-monkey.io/bitmonkey/jobqueue`. Existing consumers must keep compiling
    without source changes. "v2" is a specification name only.
11. **Lease semantics require a coordinated upgrade.** Reservation-expiry workers
    and lease-aware workers must never run concurrently against the same schema
    (§3, §5.2).
12. **Domain progress is not queue history.** The queue owns generic execution
    events; the consumer owns its domain domain events (§4).

---

## 3. Compatibility rules

v0.1.0 has a live consumer (`indifeed`) and a published tag. Three categories:

### 3.1 Additive changes (free, land whenever)

A new column with a default, a new option, a new method, a new table, a new worker
field, a new handler-context accessor, new event/attempt records. Nothing in v0.1.0
has to change and no consumer has to change for these. The **pgx enqueue API
(§5.1)**, `MaxAttempts`, backoff jitter, sequencing and concurrency options, the
queue registry, `job_attempts`, `job_events`, the read API and the cancellation
columns are all in this category for a consumer that does not opt in.

### 3.2 The existing `database/sql` transaction API is preserved exactly

```go
DispatchTx(context.Context, *sql.Tx, string, any, ...jobqueue.Option) (int64, error)
```

This signature does not change. It is **not** replaced by a generic transaction
interface and its `*sql.Tx` parameter is **not** widened. A consumer that declares
its own interface over this method (indifeed does, at
`internal/feedback/queued_email.go:17`) continues to compile and needs no change.
pgx support is added as a **separate** entry point (§5.1), not by mutating this one.

### 3.3 Non-additive changes

Two changes are not additive and are decided here, once:

- **`cancelled` as a terminal status** (§5.4, §5.5). This rewrites existing
  status/terminal/reservation `CHECK` constraints and partial indexes in migration
  `00004`. It is additive at the Go source level but not at the schema level, and it
  changes the set of terminal states a consumer may observe.
- **Ownership moved from fixed reservation expiry to lease + heartbeat** (§5.2).
  This adds durable ownership columns, changes the reclaim predicate and changes the
  `JobTimeout`/`ReservationExpiry` startup rule. It is **not additive, not at the
  API level and not at the database level**, because the meaning of a `reserved` row
  changes from "owned for a fixed window" to "owned while the lease is renewed".

### 3.4 Coordinated upgrade is required

A v0.1.0 worker and a v0.2.0 worker **must never operate concurrently against the
same jobqueue schema or database**. This is not a compatibility target and no
interoperability shim will be built:

- A v0.1.0 worker reclaims on `reserved_at < now() - ReservationExpiry` and knows
  nothing about `lease_expires_at`, so it steals a healthy long-running v0.2.0 job.
- A v0.2.0 worker reclaims on `lease_expires_at < now()`, so a row claimed by a
  v0.1.0 worker (which leaves the new column NULL) is never reclaimed at all.

The supported upgrade sequence is:

1. Stop accepting/creating new work where necessary.
2. Drain all queued/reserved work, or otherwise bring the queue to a clean state.
3. Stop every old worker.
4. Verify there are no `reserved` jobs.
5. Apply the jobqueue schema migration.
6. Deploy/start the new workers.
7. Resume work.

The migration fails clearly if step 4 is not satisfied (§5.10). Queued (`available`)
jobs need not be drained: they are unowned and a new worker picks them up. Only
`reserved` rows make the migration unsafe.

### 3.5 Migration rules

- Module migrations stay embedded and monotonic. Add `00002_…`, `00003_…`, etc.;
  never edit an applied migration.
- `jobqueue_goose_db_version` stays separate from application migration history.
- Constraint and index rewrites (the `cancelled` status work) are explicit steps in
  the migration, not incidental to adding a nullable column.
- A consumer using its own PostgreSQL schema through `search_path` keeps that
  behaviour; a shared database deployment uses one agreed queue schema and globally
  namespaced queue names.

---

## 4. Progress and history: where the boundary lies

The requirement (§2.1) is durable, append-only, sequence-resumable progress that any
process can read. The decision is where to keep it.

**The queue owns generic execution history. The consumer owns domain progress.**

Two tables in the queue:

- **`job_attempts`** — one row per claim: `job_id`, `attempt`, `worker_id`,
  `claimed_at`, `heartbeat_at`, `finished_at`, `outcome`
  (`completed | failed | cancelled | lease_expired`), `error_code`, `error_message`,
  `duration_ms`. This is the record an admin UI needs, and the data an operator needs
  to answer "how long does this job take, and who ran it".
- **`job_events`** — append-only, `seq` identity, `job_id`, `at`, `type`, and a small
  `jsonb` detail with the generic fields (`worker_id`, `attempt`, `available_at`,
  `error_code`). The bounded type vocabulary is: `enqueued`, `claimed`,
  `lease_renewed`, `lease_lost`, `retry_scheduled`, `cancel_requested`, `cancelled`,
  `completed`, `failed`, `reclaimed`. `claimed` marks the start of an attempt; there
  is no separate `attempt_started` event. A client resumes from `seq > last_seen`,
  which is exactly the requirement, and the queue provides it for every consumer
  without each consumer inventing one.

And, in a consumer, its own domain stream. For Morpheus that is:

- **`audit_run_events`** — append-only, `seq` identity, `run_id`, `at`, `type`, and a
  domain `payload` (`jsonb`): `product_extraction`, `search_progress`,
  `candidate_discovery`, `competitor_assessment`, `finding_created`,
  `report_generation`. Written by the audit engine's event sink.

**Pinned decision.** `jobqueue` events are execution-infrastructure history.
`audit_run_events` is Morpheus/domain/client progress. They are distinct streams and
the domain stream is not modelled as queue events. Domain events are **not** forced
into the generic queue vocabulary merely because execution passes through the queue.
The only overlap is the lifecycle boundary: a queue `claimed`/`completed`/`failed`
event and the domain stream's attempt correlation may both exist, and that is
acceptable duplication, not a second source of truth.

Why not one system:

- The vocabularies differ. `claimed`/`lease_renewed`/`reclaimed` are queue facts;
  "24 searches, 11 pages, 9 positioning findings" is Morpheus's. One table would
  either teach the queue Morpheus's event types — which §2.7 forbids for jobs and
  which would repeat for events — or reduce domain progress to an opaque blob with
  no queryable structure, which is the worst of both.
- The lifetimes differ. Queue history is operational and prunable within days
  (§5.8); a run's progress narrative belongs to the run and outlives its job row.
- The coverage differs. Morpheus also executes runs *without* the queue (the TUI and
  `--no-tui` call `Start` = `Create` + `Execute` in-process today), and those runs
  need progress too. Domain progress must not depend on a queue row existing.
- The write patterns differ. Queue events are written at claim/settle/retry points by
  the worker; domain events are written continuously by the engine mid-stage.

The join is `job_attempts.job_id` ↔ the run id in the job payload, plus the attempt
identity the handler can read from its context (§5.7). Morpheus stores the job id and
attempt on the run (or in its events), so an API can interleave the two timelines and
answer "the audit is in market discovery, on attempt 1, claimed by worker w-3 at
10:02".

Cost: two tables in the queue, one in Morpheus, and a row per claim plus a handful of
rows per job — negligible against a job that crawls a site for six minutes.

---

## 5. Target behaviour

### 5.1 Enqueue, including transactionally from pgx

The existing API is preserved byte-for-byte (§3.2):

```go
func (d *QueueDispatcher) Dispatch(ctx context.Context, command string, arguments any, options ...Option) (int64, error)
func (d *QueueDispatcher) DispatchTx(ctx context.Context, tx *sql.Tx, command string, arguments any, options ...Option) (int64, error)
```

pgx support is a separate entry point. It needs no `*sql.DB`, so it is a
package-level function, not a method that requires a dummy dispatcher:

```go
// PgxTx is satisfied by pgx.Tx and *pgxpool.Pool.
type PgxTx interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func EnqueueTx(ctx context.Context, tx PgxTx, command string, arguments any, options ...Option) (int64, error)
```

`EnqueueTx` uses exactly the same payload construction, insert SQL, idempotency
handling and validation as `Dispatch`/`DispatchTx`; only the driver binding differs.
The `enqueued` event (§4) is written in the caller's transaction. A pgx consumer
uses it as:

```go
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

if _, err := store.CreateRunTx(ctx, tx, project); err != nil { return err }
if _, err := jobqueue.EnqueueTx(ctx, tx, "morpheus.audit", payload{RunID: run.ID},
        jobqueue.Queue("morpheus.audit"), jobqueue.SequenceKey("project:"+project.ID),
        jobqueue.MaxAttempts(1)); err != nil { return err }

return tx.Commit(ctx)
```

Atomicity is already there in v0.1.0 (`queue.go:28`); this is an interface problem,
not a semantics problem. Atomicity requirement: if the caller rolls back, neither
domain row nor job nor event exists; if it commits, all exist. `*pgxpool.Pool` also
satisfies `PgxTx`; a pool call is a single implicit transaction and is safe for the
non-transactional case.

### 5.2 Ownership: claims, leases, heartbeats

Replace "reservation with a fixed expiry" with "lease that the owner renews". The
ownership columns are **retained and extended**, not renamed:

| Column | Responsibility |
| --- | --- |
| `reservation_id` | The fencing token identifying the current claim. Every settlement and every renewal matches it. |
| `reserved_at` | When the current claim began. |
| `lease_expires_at` | Whether the current claim is still live. |
| `worker_id` | The claiming worker's stable identity (new). |
| `heartbeat_at` | When the owner last renewed (new). |

- `jobs.worker_id text` — consumer-supplied, defaulting to
  `hostname:pid:<random>`, stable for the process, so an operator can see who holds
  a job and an admin UI can group by worker.
- The worker renews leases on an interval (`HeartbeatInterval`, default ~10s, well
  under `LeaseDuration`, default ~60s) for every job it is running. Renewal is
  fenced: `UPDATE … WHERE id=$1 AND status='reserved' AND reservation_id=$2 AND
  worker_id=$3`, so a stale worker cannot extend ownership it has lost.
- Claiming may only steal a job whose lease has expired
  (`status='reserved' AND lease_expires_at < clock_timestamp()`), never merely
  because a claim is old. The predicate handles `lease_expires_at IS NULL` as
  expired, so no row is unreclaimable.
- Settlement continues to fence on `status='reserved' AND reservation_id=$n`. The
  fencing token is unchanged; leases govern *when* a claim may be stolen, the
  reservation id governs *whether* a settlement is accepted.
- `JobTimeout` stays the handler deadline. The v0.1.0 rule "`JobTimeout` must be
  shorter than `ReservationExpiry`" is **removed**; the lease outlives the handler
  because the heartbeat renews it, which is what makes a six-minute or thirty-minute
  audit safe without a thirty-minute stale-claim window. `ReservationExpiry` is kept
  as a deprecated alias mapping to the initial `LeaseDuration` for one release;
  supplying both is an error.
- **Expired-lease policy** is per queue: `fail` or `requeue`. The default for any
  queue is `requeue`, so generic consumers keep today's behaviour. Morpheus sets
  `fail`: an abandoned audit becomes failed rather than rerun.
- **Expired-lease policy is subordinate to `MaxAttempts`** (§5.3). If the attempt cap
  is already consumed, an expired lease settles terminally regardless of a `requeue`
  default.
- A reconciliation entry point (`Reap(ctx, …)`) makes expiry handling deterministic
  and observable rather than a side effect of the next claim.
- A lease expiry is an ownership fact, not proof that the prior handler stopped. The
  module remains **at-least-once** for side effects and documents that.

Mixed old/new workers are forbidden (§3.4) and the migration guard (§5.10) enforces
the drain that makes the transition safe.

### 5.3 Retries and maximum attempts

Unchanged in kind, extended in reach:

- `MaxAttempts(n)` dispatch option (additive). **`MaxAttempts` counts execution
  claims, not only handler failures.** The attempt counter is incremented when a
  worker successfully obtains a claim that permits handler execution. Therefore
  `MaxAttempts(1)` means the handler can execute **at most once**.
- If the claiming worker dies or its lease expires after that single attempt, the job
  has already consumed its attempt and **must not be silently executed by another
  worker**. On lease expiry, when `max_attempts > 0` and `attempts >= max_attempts`,
  the job settles terminally as `failed` regardless of the queue's
  `expired_lease_policy`. This is what lets Morpheus guarantee
  `one audit run ID = one execution attempt`.
- `MaxAttempts(0)`/unset keeps today's window-bounded behaviour: the job goes back to
  `available` and the queue's expired-lease policy applies.
- Attempts that exhaust the cap are settled terminally, never left claimable and
  invisible: a job that cannot be retried is `failed`, with `job_attempts` and
  `job_events` recording why.
- `retry_attempts` is **retained for compatibility and diagnostics only**. It counts
  released handler failures (the current meaning) and is the input to backoff. It is
  not the input to `MaxAttempts`. The distinction is documented on the type:

  | Field | Counts | Drives |
  | --- | --- | --- |
  | `attempts` | execution claims (incremented on claim, including re-claims after lease expiry) | `MaxAttempts` |
  | `retry_attempts` | released handler failures | backoff and the retry-window comparison |

- Backoff gains jitter (additive): `1m << (retry_attempts-1)` with ±20% jitter, so a
  batch of failures does not retry in lockstep. The saturating bound stays unchanged.
- `Permanent(err)`, `Diagnostic(code, message, err)`, and the retry window all stay
  as they are.

### 5.4 Cancellation

- New terminal status `cancelled`, with `cancel_requested_at` and `cancelled_at`
  columns, and the status `CHECK` extended in migration `00004` (§5.5).
- Cancellation state is **durable in PostgreSQL**. Notification exists only to reduce
  cancellation latency. `RequestCancel(ctx, jobID) (CancellationResult, error)`:
  - job `available` (including in backoff) → terminal `cancelled` immediately,
    recorded in `job_events`;
  - job `reserved` → `cancel_requested_at = now()`, and the owning worker cancels the
    handler's context;
  - job already terminal → the result says so; the row is **not touched**. This is
    §2.2's "no retroactive cancellation".
- On a cancellation request the implementation:
  1. persists the cancellation request/state atomically;
  2. issues a PostgreSQL notification **in that same transaction**;
  3. lets a running worker wake promptly from the notification;
  4. is also detected by workers during periodic heartbeat/lease activity.

  Because state is durable and the heartbeat check is unconditional, a missed or
  disconnected `LISTEN` **cannot lose a cancellation**. Notification is an
  optimisation, never the source of truth.
- Propagation: the owning worker watches its running jobs for a cancel request and
  cancels the handler context. The handler sees `context.Canceled`; the worker
  settles the job `cancelled` rather than `failed`. `context.Canceled` caused by
  shutdown or worker lifecycle is settled distinctly and is not reported as a user
  cancellation.
- Retries: a cancelled job never retries, even if attempts remain.
- Races, documented and tested:
  - **cancellation ↔ claim**: whichever commits first wins; a job cancelled before
    the claim is never claimed; a job claimed first receives `cancel_requested_at`
    and is cancelled while running.
  - **cancellation ↔ handler success**: the first terminal write wins; a job that
    completes while a cancel request is in flight stays `completed`, and the race is
    visible in `job_events`. A completed job is never rewritten to cancelled.
  - **cancellation ↔ handler failure**: the first terminal write wins; a later
    cancellation reports the existing terminal result and rewrites nothing.
  - **cancellation ↔ lease expiry**: a reclaim and a cancellation cannot both produce
    a terminal write; exactly one terminal transition is recorded, and a cancel
    request that arrives after reclaim/cancellation reports the terminal result.
- The module returns what happened; whether the caller was allowed to ask is the
  application's business (§2.2). No authorization code in the module.

### 5.5 Cancelled as a terminal state

`cancelled` is a genuine third terminal status, not a variant of `failed`.

Migration `00004` must **explicitly** replace or update every affected constraint and
index from `00001_create_jobs.sql`, rather than treating this as a
nullable-column-only migration:

- `jobs_status_check` (:22) — add `cancelled`.
- `jobs_reservation_check` (:38) — `cancelled` rows have `reserved_at`/`reservation_id`
  cleared, like other terminal states.
- `jobs_terminal_check` (:42) — `cancelled` requires `cancelled_at IS NOT NULL` and
  neither `completed_at` nor `failed_at`.
- `idx_jobs_terminal_history` (:53) — extend the partial predicate to include
  `cancelled`.
- Add the cancellation columns and a partial index for pending cancellation requests.

Consumer assumptions to search for and document (the plan requires an audit as part
of the release work): any predicate equivalent to `status = 'completed' OR status =
'failed'`, `status IN ('completed','failed')`, or `completed_at IS NOT NULL OR
failed_at IS NOT NULL` is now an incomplete definition of "terminal". Consumers must
include `cancelled`. Indifeed is source-compatible; its only such assumptions are in
tests. Release notes call out `cancelled` as a new terminal state.

### 5.6 Queue registration and defaults

Queue registration/configuration is **optional**. Enqueuing into an unknown queue
atomically creates the `job_queues` row with safe generic defaults using an
idempotent, concurrency-safe insert (`INSERT … ON CONFLICT (name) DO NOTHING`), so
consumers need no setup and an admin UI always has a queue to list.

`job_queues`: `name text PRIMARY KEY`, `max_concurrency int` (NULL = unlimited),
`sequence_concurrency int NOT NULL DEFAULT 1` (the queue default for keys that do not
set their own), `expired_lease_policy text NOT NULL DEFAULT 'requeue'`.

`jobs.sequence_key text` (nullable, dispatch option `SequenceKey`) and
`jobs.sequence_concurrency int NOT NULL DEFAULT 1` (`SequenceConcurrency(n)`).

Two explicit APIs, with deterministic semantics:

```go
// EnsureQueue creates the queue with the supplied configuration only if it does
// not exist. It never mutates an existing row. This is the operation enqueue uses
// implicitly. Returns true if the row was created.
func EnsureQueue(ctx context.Context, db *sql.DB, cfg QueueConfig) (bool, error)

// RegisterQueue creates the queue or updates an existing one to exactly the
// supplied configuration. Returns the outcome so callers can tell creation from a
// changed existing queue from a no-op.
func RegisterQueue(ctx context.Context, db *sql.DB, cfg QueueConfig) (QueueRegistration, error)
```

Rules, so behaviour cannot depend on ordering:

- Auto-create on enqueue uses `EnsureQueue` semantics: it inserts defaults on first
  use and **never overwrites** an existing row. A generic enqueue can never clobber
  a queue that a consumer configured deliberately.
- `RegisterQueue` is an explicit upsert: it sets the row to exactly `cfg` and
  returns `created`, `updated` or `unchanged`. Retrying the same call is idempotent.
  It means "make this queue's stored configuration equal to the supplied
  configuration": it may both tighten and loosen limits, including restoring
  `max_concurrency = NULL` (unlimited). "Only tighten" semantics are deliberately
  not implemented.
- Concurrent first enqueues are race-safe and result in exactly one row with
  defaults.
- Morpheus calls `RegisterQueue` before its first enqueue with
  `expired_lease_policy = fail`, `sequence_concurrency = 1`, and its chosen
  `max_concurrency`. A queue registered *after* an implicit default creation is
  updated by `RegisterQueue`; that is the defined, deterministic resolution.
- Existing consumers that never register a queue keep `requeue` and unlimited
  concurrency — sensible defaults that preserve v0.1.0 throughput.

### 5.7 Sequencing and concurrency

Generic, count-based, in the claim predicate — the same shape serves both limits. The
claim statement skips a candidate when:

- the queue already has `max_concurrency` live reserved jobs, or
- its `sequence_key` already has `sequence_concurrency` live reserved jobs.

Count only live, unexpired claims. Blocked jobs are skipped, not claimed and
released, so they never hold locks while waiting, and FIFO ordering means the oldest
eligible job goes first. Because a plain non-locking `COUNT(*)` predicate is not
sufficient under race, the candidate query is serialised per queue (or per queue and
sequence key) with a session advisory lock, or the count is taken against a lockable
`job_queues` row; the plan chooses one and tests it under contention. The module
knows nothing about projects; `sequence_key` is opaque text.

### 5.8 Pruning and retention

Retention is a policy the module supports, not one it invents. Correctness rule:
**pruning a terminal job removes that job's associated queue history.** There is no
orphan-history state.

`Prune(ctx, PruneOptions)` deletes only `completed | failed | cancelled` jobs that
match the caller's cutoff/queue filter, and cascades the purely job-owned history:

- `job_attempts`
- `job_events`
- `job_failures`
- any other purely job-owned queue history added later

To make this possible, migration `00002` changes
`job_failures.job_id REFERENCES jobs(id)` from `ON DELETE RESTRICT` to
`ON DELETE CASCADE`, and the new `job_attempts`/`job_events` foreign keys are
`ON DELETE CASCADE` from the start. "Authoritative history" means authoritative while
retained, not retained indefinitely; `Prune` is the explicit, auditable act that ends
retention. Pruning never deletes a non-terminal job, and never leaves an
attempt/event/failure row without its job.

Retention values are separate from correctness:

- There is **no automatic pruning** and no default cutoff enforced by the module.
- **30 days is the documented recommended retention window** for disposable jobqueue
  operational history (terminal jobs and their `job_attempts`, `job_events` and
  `job_failures`). It is guidance only: the caller invokes `Prune`, may choose a
  longer or shorter window, and the module never deletes anything on its own.
- The 30-day recommendation applies to jobqueue operational history only. It does
  **not** apply to Morpheus audit/report history or any other consumer's domain
  records, which have their own retention policies and are not stored in these
  tables.
- `PruneOptions` carries the cutoff time, optional queue filter and a bounded batch
  limit; the caller decides. `Prune(ctx, db, PruneOptions{TerminalBefore: time.Now().Add(-30*24*time.Hour)})`
  is the documented starting point.
- The plan adds an index supporting terminal-job prune scans.

### 5.9 Notifications

The wake-up channel is **library-owned and fixed** for this phase. The module owns
one fixed job channel (the existing `jobs`, kept to minimise migration churn) and one
library-owned cancellation channel (§5.4). Both are produced by the module's own
triggers/statements, and the worker LISTENs on those fixed channels.

`Worker.NotifyChannel` is **retained in the struct so existing consumers compile, and
explicitly deprecated**. It is no longer read for the job channel. To avoid a
field that appears functional but is silently ignored:

- a non-empty `NotifyChannel` that differs from the library channel produces a
  startup warning naming the field as deprecated and ignored;
- the README and package docs state the channel is library-owned from v0.2.0;
- the field is slated for removal in a later release, with its own deprecation cycle.

This does not introduce a compatibility break now: indifeed's `NotifyChannel:
"indifeed_jobs"` keeps compiling and, as a side effect, starts actually receiving
notifications on the library channel (v0.1.0 ignored it and relied on polling). The
plan tests that a lost `LISTEN` still loses no work and no cancellation.

### 5.10 Migration guard and upgrade tooling

`Migrate` performs a pre-flight check before applying the lease migration and
**fails clearly** if, and only if, the upgrade is unsafe:

- the guard applies **only while the lease/ownership migration is pending**: the
  `jobs` table exists and does not yet have `lease_expires_at`;
- in that state, if `SELECT count(*) FROM jobs WHERE status = 'reserved'` is
  non-zero, `Migrate` returns a typed error (for example `ErrActiveReservations`,
  carrying the count) and applies nothing;
- a fresh install (no `jobs` table) and a drained database proceed normally;
- **once the lease migration has been applied, `Migrate` never rejects startup**
  merely because running jobs exist. Later `Migrate` calls are unconditional, so a
  v0.2.0 deployment with live `reserved` jobs restarts normally.

The scope is therefore exactly: `pending lease migration + reserved jobs = typed
migration safety error`. It is not "`Migrate` refuses whenever a job is running".

This is intentional: an application refusing to start during an unsafe upgrade is
preferable to silently permitting double execution or stranded jobs.

The guard is tested: a v0.1.0 database with a reserved job cannot migrate and
receives the typed error; after the job is settled, migration succeeds; a v0.2.0
database with a live reserved job migrates (no-op) without error; a fresh install
migrates. `available` jobs do not block migration. This makes step 4 of §3.4
mechanical instead of a runbook hope.

### 5.11 Read / admin API and sensitive payloads

`GetJob`, `ListJobs`, `CountJobs`, `ListAttempts`, `ListEvents` and `QueueStats` are
new, additive read APIs (§5.12).

**Safe read APIs apply `Sensitive` payload redaction regardless of job status.** A
queued, reserved or running job must not expose raw sensitive arguments merely because
settlement has not happened yet. The stored `sensitive` JSON pointers are applied to
the payload whenever it is read; if redaction fails, the existing conservative
fallback (`terminalRedactionFallback`) is used. The representation returned by the
read API is:

```go
type JobView struct {
    ID             int64
    Queue          string
    Command        string
    Status         string
    Arguments      json.RawMessage // redacted via the payload's Sensitive pointers
    Sensitive      []string
    Metadata       map[string]any
    Attempts       int
    RetryAttempts  int
    MaxAttempts    int
    AvailableAt    time.Time
    ReservedAt     *time.Time
    LeaseExpiresAt *time.Time
    WorkerID       *string
    CancelRequestedAt *time.Time
    CompletedAt    *time.Time
    FailedAt       *time.Time
    CancelledAt    *time.Time
    LastError      *string
}
```

There is **no unrestricted raw-payload API** in this work. A consumer with a concrete
need to read raw arguments uses its own database access under its own trust boundary;
the module does not provide a bypass. The same rule applies to `job_events` detail and
`job_attempts` error messages: bounded, redacted, no raw handler errors or sensitive
payloads.

### 5.12 API ergonomics (target)

```go
// Producer — existing, unchanged
dispatcher := jobqueue.NewDispatcher(db)                  // *sql.DB, kept
dispatcher.Dispatch(ctx, command, args, opts...)          // kept
dispatcher.DispatchTx(ctx, sqlTx, command, args, opts...) // kept, exact signature
jobqueue.EnqueueTx(ctx, pgxTx, command, args, opts...)    // new: pgx-native
dispatcher.RequestCancel(ctx, jobID)                      // new

// Queue registry (new)
jobqueue.EnsureQueue(ctx, db, cfg)                        // create-if-absent
jobqueue.RegisterQueue(ctx, db, cfg)                      // create-or-update

// Options (additive)
Queue(name), SequenceKey(key), SequenceConcurrency(n), MaxAttempts(n),
Delay(d), RetryWindow(d), IdempotencyKey(k), Sensitive(ptr), Metadata(map)

// Worker
registry.Register("morpheus.audit", handler)              // kept
worker := &jobqueue.Worker{DB: db, DatabaseURL: dsn, Registry: registry,
    WorkerID: "worker-1", Queue: "morpheus.audit", Concurrency: 2,
    JobTimeout: 30 * time.Minute, LeaseDuration: time.Minute,
    HeartbeatInterval: 10 * time.Second}                  // new fields
worker.Run(ctx)                                           // kept
worker.ProcessNext(ctx)                                   // kept (tests, one-shot)

// Handler: what the handler needs about its own execution
func handle(ctx context.Context, arguments []byte) error {
    info, _ := jobqueue.JobFromContext(ctx)               // new
    // info.ID, info.Attempt, info.WorkerID, info.Queue, info.SequenceKey
}

// Read side (new): the admin UI's data, not the UI
jobs, err := dispatcher.ListJobs(ctx, jobqueue.JobFilter{Queue: "morpheus.audit", Status: []string{"reserved"}})
counts, err := dispatcher.CountJobs(ctx, jobqueue.JobFilter{})
attempts, err := dispatcher.ListAttempts(ctx, jobID)
events, err := dispatcher.ListEvents(ctx, jobID, afterSeq, limit)
job, err := dispatcher.GetJob(ctx, jobID)
```

`Handler` and `Registry` signatures do not change; `JobFromContext` is added rather
than widening `Handler`.

### 5.13 Observability

Required state, queryable: queue, status, `attempts`, `retry_attempts`,
`max_attempts`, `available_at`, `reserved_at`, `lease_expires_at`, `heartbeat_at`,
`worker_id`, `cancel_requested_at`, `completed_at`, `failed_at`, `cancelled_at`,
`last_error`, plus `job_attempts` (duration, outcome) and `job_events` (timeline).
Queue depth, wait time (enqueue → first claim), run duration, retry counts, failures
and lease expiries are all derivable from those.

A live metrics hook (Prometheus/OTel) is deferred: it is additive, and the durable
data above is the part that must exist first. `slog` logging stays as it is, with the
worker identity added to the fields.

---

## 6. Required, desirable, deferred

**Required for the first Morpheus integration**

| # | Requirement | § |
| --- | --- | --- |
| R1 | pgx-native transactional enqueue (`EnqueueTx`), `DispatchTx` unchanged | 5.1 |
| R2 | Worker identity, leases, heartbeats, fenced renewal, per-queue expired-lease policy, reaper | 5.2 |
| R3 | Cancellation: queued and running, durable, propagated to the handler context, never retroactive | 5.4 |
| R4 | `MaxAttempts` counting execution claims, terminally settled when exhausted, and never re-executed after a lost lease when exhausted | 5.3 |
| R5 | Per-key sequential execution (`sequence_key`, `sequence_concurrency`) | 5.7 |
| R6 | `job_attempts` + `job_events` (generic execution history, resumable by `seq`) | 4 |
| R7 | The run id is the whole payload; the handler can read attempt/worker identity | 5.12 |
| R8 | Queue registration with defaults and explicit `EnsureQueue`/`RegisterQueue` | 5.6 |
| R9 | Coordinated-upgrade migration guard | 5.10 |

**Strongly desirable now, because it is cheap and shared**

| # | Requirement | Why now |
| --- | --- | --- |
| D1 | Queue-wide `max_concurrency` via `job_queues` | Same predicate as R5; a table the admin UI needs anyway |
| D2 | Read-side query API (`ListJobs`, `CountJobs`, `GetJob`, `ListAttempts`, `ListEvents`, `QueueStats`) with status-independent redaction | §2.9 asks for the data; exposing it is a thin layer over the indexes, and redaction is a trust-boundary requirement |
| D3 | Backoff jitter | Two lines, removes lockstep retries across workers |
| D4 | `Prune(ctx, PruneOptions)` with cascading job-owned history and caller-supplied retention | Without it `job_events` grows forever; retention is a policy the module should support, not invent |
| D5 | Reaper as an explicit, callable operation | Makes lease expiry observable rather than a claim-time side effect |
| D6 | Notification cleanup (`NotifyChannel` deprecated, library-owned channels) | Removes a field that appears functional but is ignored |

**Deliberately deferred**

| # | Deferred | Why it is safe to defer |
| --- | --- | --- |
| X1 | Priority/weighted scheduling | Separate queues already express it (indifeed does exactly this); a `priority` column plus index ordering is additive |
| X2 | Recurring/cron schedules | `Delay` covers "later"; recurring work is a scheduler, not a queue, and has no requester yet |
| X3 | Batch/group implementation and aggregate progress | jobs are individual rows with stable ids; `batch_id` + `job_batches` is one additive migration |
| X4 | Admin UI (web) | §2.9 asks for queryable state and an API; the UI is a separate deliverable |
| X5 | Live metrics hook | Additive; durable data first |
| X6 | Job payload schema registry / typed args | Handlers already unmarshal their own structs; a registry is a convenience, not a correctness need |
| X7 | Multi-database support | PostgreSQL-only is the design (§2.6 needs same-database transactions) |
| X8 | Automatic retention/background pruning | Retention is a caller policy (§5.8); a scheduler is additive |

---

## 7. Acceptance criteria

The module is v0.2.0 when, against a real PostgreSQL:

1. A pgx transaction that creates a domain row and enqueues a job commits both or
   neither, and a rolled-back transaction leaves no job.
2. `DispatchTx(*sql.Tx)` retains its exact signature and v0.1.0 call patterns compile
   and behave (verified against indifeed).
3. Two workers racing for one job: exactly one runs it; the other claims nothing.
4. A worker killed mid-job has its job reclaimed only after its lease expires, and
   only according to the queue's expired-lease policy (`fail` → terminal failure;
   `requeue` → claimable again), unless `MaxAttempts` is exhausted, in which case it
   is terminally failed.
5. A healthy, heartbeating worker's job is never stolen, however long it runs, and a
   stale owner cannot renew a reclaimed claim.
6. A queued job can be cancelled before it is claimed: it never runs, and ends
   `cancelled`.
7. A running job can be cancelled: its handler's context is cancelled, and it ends
   `cancelled`, not `failed`, and never retries.
8. Cancellation is durable: it survives a lost/disconnected notification and is
   detected on the next heartbeat.
9. Races between cancellation and claim, handler success, handler failure, and lease
   expiry each have exactly one terminal winner, and the loser is reported without
   rewriting history. A completed job is never rewritten to cancelled.
10. `MaxAttempts(1)` means one execution attempt: a failing handler leaves the job
    `failed` with one `job_attempts` row and no second run; a killed worker's job is
    terminally failed after lease expiry and is never executed again.
11. Two jobs sharing a `sequence_key` never run concurrently; jobs with different keys
    do; and a queue's `max_concurrency` is respected across processes.
12. Enqueuing into an unknown queue creates it with `requeue` and defaults,
    concurrency-safely; `RegisterQueue` deterministically creates or updates an
    existing queue, and auto-create never overwrites explicit configuration.
13. Every state transition of a job appears in `job_events` in `seq` order, and a
    client resuming from `seq > n` sees each event exactly once.
14. `job_attempts` records worker, claim time, finish time, duration and outcome for
    every attempt, successful or not.
15. `Migrate` fails with a clear typed error when `reserved` jobs exist, applying
    nothing, and succeeds after they are settled; a fresh install migrates.
16. `Prune` deletes a terminal job and cascades its `job_attempts`, `job_events` and
    `job_failures`, leaves no orphans, and refuses non-terminal jobs.
17. `GetJob`/`ListJobs` redact `Sensitive` payload pointers for every status,
    including `available` and `reserved`, and expose no raw-payload escape hatch.
18. `NotifyChannel` is documented as deprecated and ignored, a non-default value
    warns at startup, and the library channels deliver wake-ups without it.
19. The v0.1.0 API still compiles and behaves for a consumer that changes nothing
    (verified against indifeed's usage patterns), with `cancelled` documented as a new
    terminal state.
