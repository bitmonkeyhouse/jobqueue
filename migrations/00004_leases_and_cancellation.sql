-- +goose Up
-- +goose StatementBegin
-- Explicit ownership: a claim is a lease the owner renews. reservation_id stays
-- the fencing token; reserved_at records when the claim began; lease_expires_at
-- decides whether the claim is still live. Mixed v0.1.0/v0.2.0 workers are
-- unsupported, and Migrate refuses to cross this boundary while reserved jobs
-- exist (see migrate.go).
ALTER TABLE jobs
    ADD COLUMN worker_id text,
    ADD COLUMN heartbeat_at timestamptz,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN cancel_requested_at timestamptz,
    ADD COLUMN cancelled_at timestamptz;

-- cancelled is a genuine third terminal status, so the status, terminal and
-- history predicates are rewritten explicitly rather than treated as an added
-- nullable column.
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('available', 'reserved', 'completed', 'failed', 'cancelled'));

ALTER TABLE jobs DROP CONSTRAINT jobs_terminal_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_terminal_check CHECK (
    (status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL AND cancelled_at IS NULL)
    OR (status = 'failed' AND failed_at IS NOT NULL AND completed_at IS NULL AND cancelled_at IS NULL)
    OR (status = 'cancelled' AND cancelled_at IS NOT NULL AND completed_at IS NULL AND failed_at IS NULL)
    OR (status IN ('available', 'reserved') AND completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL)
);

DROP INDEX idx_jobs_expired_reservations;
CREATE INDEX idx_jobs_lease_expiry ON jobs (lease_expires_at, id)
    WHERE status = 'reserved';
CREATE INDEX idx_jobs_worker ON jobs (worker_id, status)
    WHERE worker_id IS NOT NULL;
CREATE INDEX idx_jobs_cancel_pending ON jobs (cancel_requested_at)
    WHERE status = 'reserved' AND cancel_requested_at IS NOT NULL;

DROP INDEX idx_jobs_terminal_history;
CREATE INDEX idx_jobs_terminal_history ON jobs (status, updated_at DESC, id DESC)
    WHERE status IN ('completed', 'failed', 'cancelled');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_terminal_history;
CREATE INDEX idx_jobs_terminal_history ON jobs (status, updated_at DESC, id DESC)
    WHERE status IN ('completed', 'failed');

DROP INDEX IF EXISTS idx_jobs_cancel_pending;
DROP INDEX IF EXISTS idx_jobs_worker;
DROP INDEX IF EXISTS idx_jobs_lease_expiry;
CREATE INDEX idx_jobs_expired_reservations ON jobs (reserved_at, id)
    WHERE status = 'reserved';

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_terminal_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_terminal_check CHECK (
    (status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL)
    OR (status = 'failed' AND failed_at IS NOT NULL AND completed_at IS NULL)
    OR (status IN ('available', 'reserved') AND completed_at IS NULL AND failed_at IS NULL)
);

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('available', 'reserved', 'completed', 'failed'));

ALTER TABLE jobs
    DROP COLUMN IF EXISTS cancelled_at,
    DROP COLUMN IF EXISTS cancel_requested_at,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS worker_id;
-- +goose StatementEnd
