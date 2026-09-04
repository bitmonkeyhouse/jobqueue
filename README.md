# jobqueue

A PostgreSQL-backed job queue for Go. You **enqueue** work in one place, and a
**worker** runs it in another — reliably, with automatic retries, timeouts, and
idempotency protection.

If you know how to write a Go function, you know how to use this.

---

## What you need

- **Go 1.26 or newer** (check with `go version`)
- **A running PostgreSQL** (any modern version; 14+ is a safe bet)
- The ability to import the module (network access to
  `git.bit-monkey.io`)

No Redis, no RabbitMQ, no separate service. It's just Postgres and your app.

---

## 1. Install

In your Go project, run:

```bash
go get git.bit-monkey.io/bitmonkey/jobqueue
```

That's it. No other setup.

---

## 2. Open a database connection

The queue needs one `*sql.DB` connected to your PostgreSQL database. Use the
`pgx` driver:

```go
import (
    "database/sql"

    _ "github.com/jackc/pgx/v5/stdlib"
)

db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/mydb?sslmode=disable")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

> The `_` import is what actually registers the driver. Don't delete it.

Replace the connection string with your own database details.

---

## 3. Create the tables (one time)

Before you can enqueue or process jobs, the queue's tables must exist. Call
`Migrate` **once, at startup**, before anything else uses the queue:

```go
import "git.bit-monkey.io/bitmonkey/jobqueue"

if err := jobqueue.Migrate(context.Background(), db); err != nil {
    log.Fatal(err)
}
```

It creates the `jobs` and `job_failures` tables (plus a few indexes and a
notification trigger). It tracks its own schema version in a table called
`jobqueue_goose_db_version`, so it will **not** clash with any other migration
tooling you use.

Calling it again later is harmless — it only applies migrations you don't have
yet.

---

## 4. Enqueue a job (the producer)

A **job** is just a **named command** plus some **arguments**. Arguments must be
a JSON object, array, or `nil`.

```go
dispatcher := jobqueue.NewDispatcher(db)

id, err := dispatcher.Dispatch(
    context.Background(),
    "send_welcome_email",              // command name
    map[string]any{                    // arguments (JSON object)
        "user_id": 42,
        "email":   "pat@example.com",
    },
)
if err != nil {
    log.Fatal(err)
}
log.Printf("enqueued job %d", id)
```

Your worker (step 5) will look up the `"send_welcome_email"` command and run it
with those arguments.

### Enqueue inside your own transaction

If you're already inside a `sql.Tx` and want the job to only exist if your
transaction commits, use `DispatchTx`:

```go
id, err := dispatcher.DispatchTx(ctx, tx, "send_welcome_email", args)
```

If the transaction rolls back, the job disappears with it.

---

## 5. Run jobs (the worker)

The worker needs two things:

1. A **registry** mapping command names to handler functions.
2. A `Worker` struct that connects to the database.

A **handler** is a function that receives the job's arguments (as raw JSON
bytes) and does the work:

```go
func sendWelcomeEmail(ctx context.Context, arguments []byte) error {
    var args struct {
        UserID int    `json:"user_id"`
        Email  string `json:"email"`
    }
    if err := json.Unmarshal(arguments, &args); err != nil {
        return err
    }

    // ... actually send the email ...
    log.Printf("sending welcome email to %s", args.Email)
    return nil
}
```

Now wire it up:

```go
registry := jobqueue.NewRegistry()
if err := registry.Register("send_welcome_email", sendWelcomeEmail); err != nil {
    log.Fatal(err)
}

worker := &jobqueue.Worker{
    DB:          db,
    DatabaseURL: "postgres://user:password@localhost:5432/mydb?sslmode=disable", // optional but recommended
    Registry:    registry,
    Logger:      slog.Default(),
}

if err := worker.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

`worker.Run` **blocks forever**, pulling jobs off the queue and running them as
they arrive. Press Ctrl-C to stop it.

### What a handler means

- **Return `nil`** → the job succeeded. It's marked completed.
- **Return any other error** → the job failed and will be **retried
  automatically** (see "Retries" below).

Arguments arrive as raw JSON. Unmarshal them into your own struct — the queue
never imposes a shape on you.

---

## 6. Put it all together

Here is a complete, copy-pasteable program that enqueues one job and runs it:

```go
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "log/slog"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
    "git.bit-monkey.io/bitmonkey/jobqueue"
)

const dsn = "postgres://user:password@localhost:5432/mydb?sslmode=disable"

func main() {
    db, err := sql.Open("pgx", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := jobqueue.Migrate(context.Background(), db); err != nil {
        log.Fatal(err)
    }

    // Enqueue one job.
    dispatcher := jobqueue.NewDispatcher(db)
    id, err := dispatcher.Dispatch(context.Background(), "say_hello",
        map[string]any{"name": "world"})
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("enqueued job %d", id)

    // Handle jobs.
    registry := jobqueue.NewRegistry()
    if err := registry.Register("say_hello", sayHello); err != nil {
        log.Fatal(err)
    }

    worker := &jobqueue.Worker{
        DB:          db,
        DatabaseURL: dsn,
        Registry:    registry,
        Logger:      slog.Default(),
    }

    // Blocks until you stop the program.
    if err := worker.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}

func sayHello(ctx context.Context, arguments []byte) error {
    var args struct {
        Name string `json:"name"`
    }
    if err := json.Unmarshal(arguments, &args); err != nil {
        return err
    }
    log.Printf("hello, %s!", args.Name)
    return nil
}
```

Run it with:

```bash
go run .
```

You should see `enqueued job 1` followed by `hello, world!`.

---

## 7. Dispatch options

Add options to `Dispatch` (or `DispatchTx`) to customize a job. Chain as many
as you like.

```go
id, err := dispatcher.Dispatch(ctx, "cmd", args,
    jobqueue.Queue("emails"),
    jobqueue.Delay(5*time.Minute),
    jobqueue.RetryWindow(2*time.Hour),
    jobqueue.IdempotencyKey("user-42-welcome"),
    jobqueue.Sensitive("/arguments/password"),
    jobqueue.Metadata(map[string]any{"trace_id": "abc123"}),
)
```

| Option | What it does |
|---|---|
| `Queue(name)` | Put the job in a named queue instead of `"default"`. Each queue is worked independently. |
| `Delay(d)` | Don't make the job available until `d` has passed. Useful for "do this later" jobs. |
| `RetryWindow(d)` | How long (from when the job first becomes available) retries are allowed. Default **24 hours**. |
| `IdempotencyKey(key)` | Prevent duplicate jobs with the same key **while one is still active**. Dispatching the same key twice returns the existing job's ID. |
| `Sensitive(pointer)` | Mark part of the arguments as secret, so it's scrubbed from the stored payload after the job finishes (see "Secrets" below). |
| `SensitiveArguments()` | Shorthand to mark **all** arguments as secret. |
| `Metadata(values)` | Attach arbitrary extra data to the job (a JSON object). |

Defaults when you pass no options:

- queue: `default`
- delay: none (available immediately)
- retry window: 24 hours

---

## 8. Retries

When a handler returns an error, the job is retried automatically.

- Backoff starts at **1 minute**, then **doubles**: 1m, 2m, 4m, 8m, …
- Retries continue until the **retry window** runs out (default 24 hours from
  when the job first became available).
- After that, the job is marked **failed** permanently.

A handler that succeeds after a few retries is completely normal.

### "This will never work" — permanent failures

Some errors will never fix themselves (bad input, missing account, etc.).
Wrap them with `Permanent` and the job is **failed immediately, no retry**:

```go
import "git.bit-monkey.io/bitmonkey/jobqueue"

func chargeCard(ctx context.Context, arguments []byte) error {
    if cardIsExpired() {
        return jobqueue.Permanent(fmt.Errorf("card is expired"))
    }
    // ...
    return nil
}
```

### Timeouts

By default a handler gets **30 seconds** before its context is canceled. If you
need more (or less):

```go
worker.JobTimeout = 2 * time.Minute
```

> `JobTimeout` **must** be shorter than `ReservationExpiry` (default 90s). If
> you raise the timeout, raise the reservation expiry too, or the worker will
> refuse to start.

---

## 9. Secrets / sensitive data

Arguments are stored as JSON in the database. If part of that JSON is secret
(an API key, a password), mark it so the queue scrubs it from the stored
payload **after the job is done**:

```go
id, err := dispatcher.Dispatch(ctx, "call_api", map[string]any{
    "url":      "https://api.example.com/x",
    "password": "hunter2",
},
    jobqueue.Sensitive("/arguments/password"),
)
```

- `/arguments` → mark the whole arguments object as secret.
- `/arguments/field` → mark one field.
- `/arguments/list/0` → mark one element of an array.

Sensitive values are removed from the completed/failed job record. (The raw
value still needs to exist while the job runs — the scrub happens at the end.)

---

## 10. Error diagnostics

You can attach a **safe, stable error code** to an error so operators can see
what happened without exposing internal error text:

```go
return jobqueue.Diagnostic("card_expired", "customer card is expired",
    fmt.Errorf("stripe said %q", internalDetail))
```

- `code` must look like `lowercase_words_and_numbers` (max 64 chars).
- `message` is shown to operators (max 500 chars).
- The underlying error stays available to your own code via `errors.Is` /
  `errors.As`, but the queue stores only the safe code + message.

Failed jobs land in the `job_failures` table with this code and message, plus
the attempt count.

---

## 11. Worker configuration reference

Set these fields on your `Worker` struct. Every field is optional — shown are
the defaults.

| Field | Default | Meaning |
|---|---|---|
| `DB` | *(required)* | The `*sql.DB` to use for claiming/settling jobs. |
| `DatabaseURL` | *(empty)* | Connection string used for Postgres `LISTEN`/`NOTIFY`. Empty = polling only (still works, just less snappy). |
| `Registry` | *(required)* | The command → handler registry. |
| `Queue` | `"default"` | Which queue this worker pulls from. |
| `NotifyChannel` | `"jobs"` | The Postgres NOTIFY channel. Leave it alone unless you know why. |
| `Concurrency` | `2` | How many jobs this worker processes at once. |
| `JobTimeout` | `30s` | Max time one handler gets to finish. |
| `ReservationExpiry` | `90s` | How long a claimed job stays "reserved" before another worker may steal it. |
| `PollInterval` | `15s` | How often to check for work when there's nothing to do (polling fallback). |
| `ShutdownGrace` | `35s` | How long in-flight jobs get to finish when the worker is stopped. |
| `SettlementTimeout` | `5s` | Max time allowed to write a job's final result to the DB. |
| `Logger` | `slog.Default()` | Structured logger for errors and lifecycle events. |

### Run a worker for a specific queue

```go
worker.Queue = "emails"
```

Now this worker only touches jobs dispatched with `Queue("emails")`. Run
multiple workers on different queues, or multiple workers on the same queue for
more throughput — jobs are claimed safely so no two workers run the same job.

---

## 12. How it works (the short version)

1. `Dispatch` inserts a row into the `jobs` table and a Postgres trigger sends
   a `NOTIFY`.
2. A worker listening for that notification claims the next available job,
   marks it `reserved`, and runs its handler.
3. Handler returns:
   - `nil` → job marked `completed`.
   - error → job made available again after a backoff (or marked `failed`).
4. A reservation has an expiry, so if a worker **crashes** mid-job, the job
   becomes claimable again after `ReservationExpiry` instead of being lost.

At-least-once delivery: a job **may** run more than once if a worker crashes at
the wrong moment. Make your handlers tolerate being run twice.

---

## 13. Troubleshooting

**`worker.Run` returns `"job queue database is required"`**
You forgot `DB`, or the `*sql.DB` is `nil`.

**`worker.Run` returns `"job handler registry is required"`**
You forgot `Registry`, or it's `nil`.

**`worker.Run` returns `"job timeout must be shorter than reservation expiry"`**
You set `JobTimeout >= ReservationExpiry`. Raise `ReservationExpiry` or lower
`JobTimeout`.

**`Dispatch` returns `"job command is required"`**
The command string is empty. Give it a name.

**`Dispatch` returns `"job arguments must be an object, array, or null"`**
Arguments must be a map/slice/`nil` — not a plain string or number.

**Jobs never get picked up**
- Did you call `Migrate`?
- Is a worker actually running (`worker.Run`)?
- Is the worker's `Queue` the same as the job's queue?
- Is the job delayed (`Delay`) or still in retry backoff? It won't run until its
  `available_at` time.

**I want to see what the queue is doing**
Look at the tables directly:

```sql
SELECT id, queue, status, attempts, retry_attempts, available_at
FROM jobs
ORDER BY id DESC
LIMIT 20;
```

And failures:

```sql
SELECT job_id, command, error_code, error_message, terminal, occurred_at
FROM job_failures
ORDER BY id DESC;
```

---

## License

See your repository's license terms. This README describes the public API as of
tag `v0.1.0`.
