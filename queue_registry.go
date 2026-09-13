package jobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ExpiredLeasePolicy is what happens to a job whose owner stopped renewing its
// lease. It is configured per queue.
type ExpiredLeasePolicy string

const (
	// ExpiredLeaseRequeue returns an abandoned job to the available pool. This is
	// the default so generic consumers keep v0.1.0 behaviour.
	ExpiredLeaseRequeue ExpiredLeasePolicy = "requeue"
	// ExpiredLeaseFail terminally fails an abandoned job instead of rerunning it.
	ExpiredLeaseFail ExpiredLeasePolicy = "fail"
)

// QueueConfig is a queue's stored configuration. A nil MaxConcurrency means
// unlimited. SequenceConcurrency is the default for sequence keys that do not
// set their own; zero means 1.
type QueueConfig struct {
	Name                string
	MaxConcurrency      *int
	SequenceConcurrency int
	ExpiredLeasePolicy  ExpiredLeasePolicy
}

// QueueRegistration reports what an exact-upsert RegisterQueue call did.
type QueueRegistration int

const (
	QueueCreated QueueRegistration = iota
	QueueUpdated
	QueueUnchanged
)

func (r QueueRegistration) String() string {
	switch r {
	case QueueCreated:
		return "created"
	case QueueUpdated:
		return "updated"
	case QueueUnchanged:
		return "unchanged"
	default:
		return "unknown"
	}
}

func normalizedQueueConfig(cfg QueueConfig) (QueueConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		return cfg, errors.New("queue name is required")
	}
	if cfg.SequenceConcurrency < 0 {
		return cfg, errors.New("queue sequence concurrency cannot be negative")
	}
	if cfg.SequenceConcurrency == 0 {
		cfg.SequenceConcurrency = 1
	}
	if cfg.ExpiredLeasePolicy == "" {
		cfg.ExpiredLeasePolicy = ExpiredLeaseRequeue
	}
	switch cfg.ExpiredLeasePolicy {
	case ExpiredLeaseRequeue, ExpiredLeaseFail:
	default:
		return cfg, fmt.Errorf("unknown expired lease policy %q", cfg.ExpiredLeasePolicy)
	}
	if cfg.MaxConcurrency != nil && *cfg.MaxConcurrency <= 0 {
		return cfg, errors.New("queue max concurrency must be positive")
	}
	return cfg, nil
}

// EnsureQueue creates the queue with the supplied configuration only if it does
// not exist, and never mutates an existing row. This is the operation enqueue
// uses implicitly. It returns true when the row was created.
func EnsureQueue(ctx context.Context, db *sql.DB, cfg QueueConfig) (bool, error) {
	if db == nil {
		return false, errors.New("job queue database is required")
	}
	cfg, err := normalizedQueueConfig(cfg)
	if err != nil {
		return false, err
	}
	result, err := db.ExecContext(ctx, `
INSERT INTO job_queues (name, max_concurrency, sequence_concurrency, expired_lease_policy)
VALUES ($1,$2,$3,$4)
ON CONFLICT (name) DO NOTHING`, cfg.Name, cfg.MaxConcurrency, cfg.SequenceConcurrency, string(cfg.ExpiredLeasePolicy))
	if err != nil {
		return false, fmt.Errorf("ensure job queue: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("ensure job queue rows: %w", err)
	}
	return affected == 1, nil
}

// RegisterQueue makes the queue's stored configuration equal to cfg, creating it
// if absent. It is an exact upsert: it may both tighten and loosen configuration,
// including restoring unlimited concurrency (MaxConcurrency nil). "Only tighten"
// semantics are deliberately not implemented.
func RegisterQueue(ctx context.Context, db *sql.DB, cfg QueueConfig) (QueueRegistration, error) {
	if db == nil {
		return QueueUnchanged, errors.New("job queue database is required")
	}
	cfg, err := normalizedQueueConfig(cfg)
	if err != nil {
		return QueueUnchanged, err
	}
	var created bool
	err = db.QueryRowContext(ctx, `
INSERT INTO job_queues (name, max_concurrency, sequence_concurrency, expired_lease_policy)
VALUES ($1,$2,$3,$4)
ON CONFLICT (name) DO UPDATE
SET max_concurrency=EXCLUDED.max_concurrency,
    sequence_concurrency=EXCLUDED.sequence_concurrency,
    expired_lease_policy=EXCLUDED.expired_lease_policy,
    updated_at=clock_timestamp()
WHERE job_queues.max_concurrency IS DISTINCT FROM EXCLUDED.max_concurrency
   OR job_queues.sequence_concurrency IS DISTINCT FROM EXCLUDED.sequence_concurrency
   OR job_queues.expired_lease_policy IS DISTINCT FROM EXCLUDED.expired_lease_policy
RETURNING (xmax = 0) AS created`, cfg.Name, cfg.MaxConcurrency, cfg.SequenceConcurrency, string(cfg.ExpiredLeasePolicy)).Scan(&created)
	if errors.Is(err, sql.ErrNoRows) {
		return QueueUnchanged, nil
	}
	if err != nil {
		return QueueUnchanged, fmt.Errorf("register job queue: %w", err)
	}
	if created {
		return QueueCreated, nil
	}
	return QueueUpdated, nil
}
