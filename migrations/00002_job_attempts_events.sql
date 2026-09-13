-- +goose Up
-- +goose StatementBegin
-- Generic execution history: one row per claim (attempt) and an append-only
-- event timeline. Both are purely job-owned and cascade with the job so that
-- Prune can end retention without orphaning history.
CREATE TABLE job_attempts (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    queue text NOT NULL,
    attempt integer NOT NULL,
    worker_id text,
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz,
    finished_at timestamptz,
    outcome text,
    error_code text,
    error_message text,
    duration_ms bigint,
    CONSTRAINT job_attempts_queue_nonempty CHECK (btrim(queue) <> ''),
    CONSTRAINT job_attempts_attempt_check CHECK (attempt > 0),
    CONSTRAINT job_attempts_outcome_check CHECK (
        outcome IS NULL OR outcome IN ('completed', 'failed', 'cancelled', 'lease_expired')
    ),
    CONSTRAINT job_attempts_finished_check CHECK (
        (finished_at IS NULL AND outcome IS NULL)
        OR (finished_at IS NOT NULL AND outcome IS NOT NULL)
    ),
    CONSTRAINT job_attempts_duration_check CHECK (duration_ms IS NULL OR duration_ms >= 0),
    CONSTRAINT job_attempts_error_code_check CHECK (
        error_code IS NULL
        OR (length(error_code) BETWEEN 1 AND 64 AND error_code ~ '^[a-z][a-z0-9_]*$')
    ),
    CONSTRAINT job_attempts_error_message_check CHECK (
        error_message IS NULL OR length(error_message) BETWEEN 1 AND 500
    ),
    CONSTRAINT job_attempts_job_attempt_unique UNIQUE (job_id, attempt)
);

CREATE INDEX idx_job_attempts_job_history ON job_attempts (job_id, attempt);
CREATE INDEX idx_job_attempts_recent ON job_attempts (claimed_at DESC, id DESC);
CREATE INDEX idx_job_attempts_open ON job_attempts (job_id)
    WHERE outcome IS NULL;

CREATE TABLE job_events (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    type text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT job_events_type_check CHECK (type IN (
        'enqueued', 'claimed', 'lease_renewed', 'lease_lost', 'retry_scheduled',
        'cancel_requested', 'cancelled', 'completed', 'failed', 'reclaimed'
    )),
    CONSTRAINT job_events_detail_check CHECK (jsonb_typeof(detail) = 'object')
);

CREATE INDEX idx_job_events_job_history ON job_events (job_id, seq);
CREATE INDEX idx_job_events_recent ON job_events (at DESC, seq DESC);

-- "Authoritative while retained" means Prune may delete a job together with its
-- failure history. RESTRICT made that impossible; the history stays authoritative
-- until the operator prunes it.
ALTER TABLE job_failures DROP CONSTRAINT job_failures_job_id_fkey;
ALTER TABLE job_failures ADD CONSTRAINT job_failures_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;

-- The module owns the job notification trigger and channel. A legacy schema may
-- carry a consumer-local trigger/channel; replace it so wake-ups reach the
-- library channel. Triggers are intentionally not part of legacy baseline
-- verification, precisely because this migration normalises them.
DROP TRIGGER IF EXISTS jobs_notify_after_insert ON jobs;
CREATE OR REPLACE FUNCTION notify_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('jobs', NEW.queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER jobs_notify_after_insert
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION notify_job();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE job_failures DROP CONSTRAINT job_failures_job_id_fkey;
ALTER TABLE job_failures ADD CONSTRAINT job_failures_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE RESTRICT;
DROP TABLE IF EXISTS job_events;
DROP TABLE IF EXISTS job_attempts;
-- +goose StatementEnd
