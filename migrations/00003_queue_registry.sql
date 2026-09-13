-- +goose Up
-- +goose StatementBegin
-- Queue registry. A row is created with safe generic defaults on first enqueue
-- (never overwriting an explicitly registered queue) so consumers need no setup.
CREATE TABLE job_queues (
    name text PRIMARY KEY,
    max_concurrency integer,
    sequence_concurrency integer NOT NULL DEFAULT 1,
    expired_lease_policy text NOT NULL DEFAULT 'requeue',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT job_queues_name_nonempty CHECK (btrim(name) <> ''),
    CONSTRAINT job_queues_max_concurrency_check CHECK (max_concurrency IS NULL OR max_concurrency > 0),
    CONSTRAINT job_queues_sequence_concurrency_check CHECK (sequence_concurrency > 0),
    CONSTRAINT job_queues_expired_lease_policy_check CHECK (expired_lease_policy IN ('fail', 'requeue'))
);

ALTER TABLE jobs
    ADD COLUMN sequence_key text,
    ADD COLUMN sequence_concurrency integer NOT NULL DEFAULT 1,
    ADD COLUMN max_attempts integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT jobs_sequence_key_nonempty CHECK (sequence_key IS NULL OR btrim(sequence_key) <> ''),
    ADD CONSTRAINT jobs_sequence_concurrency_check CHECK (sequence_concurrency > 0),
    ADD CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 0);

-- Existing v0.1.0 jobs predate the registry; backfill a default queue row for
-- every queue in use so queued jobs stay claimable after the upgrade.
INSERT INTO job_queues (name)
SELECT DISTINCT queue FROM jobs
ON CONFLICT (name) DO NOTHING;

-- Capacity counting only considers live claims (unexpired reservations once
-- leases exist); these partial indexes support the claim predicate.
CREATE INDEX idx_jobs_queue_reserved ON jobs (queue)
    WHERE status = 'reserved';
CREATE INDEX idx_jobs_sequence_reserved ON jobs (queue, sequence_key)
    WHERE status = 'reserved' AND sequence_key IS NOT NULL;
-- Supports the terminal-job prune scan from WP7.
CREATE INDEX idx_jobs_terminal_prune ON jobs (updated_at, id)
    WHERE status IN ('completed', 'failed', 'cancelled');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_terminal_prune;
DROP INDEX IF EXISTS idx_jobs_sequence_reserved;
DROP INDEX IF EXISTS idx_jobs_queue_reserved;
ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS jobs_max_attempts_check,
    DROP CONSTRAINT IF EXISTS jobs_sequence_concurrency_check,
    DROP CONSTRAINT IF EXISTS jobs_sequence_key_nonempty,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS sequence_concurrency,
    DROP COLUMN IF EXISTS sequence_key;
DROP TABLE IF EXISTS job_queues;
-- +goose StatementEnd
