-- +goose Up
-- +goose StatementBegin
CREATE TABLE jobs (
    id bigserial PRIMARY KEY,
    queue text NOT NULL DEFAULT 'default',
    payload jsonb NOT NULL,
    idempotency_key text,
    status text NOT NULL DEFAULT 'available',
    attempts integer NOT NULL DEFAULT 0,
    retry_attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    retry_until timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    reserved_at timestamptz,
    reservation_id text,
    completed_at timestamptz,
    failed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_queue_nonempty CHECK (btrim(queue) <> ''),
    CONSTRAINT jobs_idempotency_key_nonempty CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    CONSTRAINT jobs_status_check CHECK (status IN ('available', 'reserved', 'completed', 'failed')),
    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_retry_attempts_check CHECK (retry_attempts >= 0),
    CONSTRAINT jobs_retry_window_check CHECK (retry_until >= available_at),
    CONSTRAINT jobs_payload_check CHECK (COALESCE(
        jsonb_typeof(payload) = 'object'
        AND payload ? 'command'
        AND jsonb_typeof(payload->'command') = 'string'
        AND btrim(payload->>'command') <> ''
        AND payload ? 'arguments'
        AND (payload->'arguments' = 'null'::jsonb OR jsonb_typeof(payload->'arguments') IN ('object', 'array'))
        AND payload ? 'sensitive'
        AND jsonb_typeof(payload->'sensitive') = 'array'
        AND NOT jsonb_path_exists(payload->'sensitive', '$[*] ? (@.type() != "string")')
        AND (NOT (payload ? 'metadata') OR jsonb_typeof(payload->'metadata') = 'object')
    , false)),
    CONSTRAINT jobs_reservation_check CHECK (
        (status = 'reserved' AND reserved_at IS NOT NULL AND reservation_id IS NOT NULL)
        OR (status <> 'reserved' AND reserved_at IS NULL AND reservation_id IS NULL)
    ),
    CONSTRAINT jobs_terminal_check CHECK (
        (status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL)
        OR (status = 'failed' AND failed_at IS NOT NULL AND completed_at IS NULL)
        OR (status IN ('available', 'reserved') AND completed_at IS NULL AND failed_at IS NULL)
    )
);

CREATE INDEX idx_jobs_available ON jobs (queue, available_at, id)
    WHERE status = 'available';
CREATE INDEX idx_jobs_expired_reservations ON jobs (reserved_at, id)
    WHERE status = 'reserved';
CREATE UNIQUE INDEX idx_jobs_active_idempotency ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND status IN ('available', 'reserved');
CREATE INDEX idx_jobs_terminal_history ON jobs (status, updated_at DESC, id DESC)
    WHERE status IN ('completed', 'failed');

CREATE TABLE job_failures (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    queue text NOT NULL,
    command text NOT NULL,
    attempts integer NOT NULL,
    retry_attempts integer NOT NULL,
    error_code text NOT NULL,
    error_message text NOT NULL,
    terminal boolean NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_failures_queue_nonempty CHECK (btrim(queue) <> ''),
    CONSTRAINT job_failures_command_nonempty CHECK (btrim(command) <> ''),
    CONSTRAINT job_failures_attempts_check CHECK (attempts >= 0 AND retry_attempts >= 0),
    CONSTRAINT job_failures_error_code_check CHECK (
        length(error_code) BETWEEN 1 AND 64
        AND error_code ~ '^[a-z][a-z0-9_]*$'
    ),
    CONSTRAINT job_failures_error_message_check CHECK (
        length(error_message) BETWEEN 1 AND 500
    )
);

-- Job history is retained with its parent job. RESTRICT deliberately prevents
-- deleting a job while its authoritative failure history exists.
CREATE INDEX idx_job_failures_job_history ON job_failures (job_id, occurred_at, id);
CREATE INDEX idx_job_failures_operations ON job_failures (terminal, occurred_at DESC, id DESC);

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
DROP TABLE IF EXISTS job_failures;
DROP TABLE IF EXISTS jobs;
DROP FUNCTION IF EXISTS notify_job();
