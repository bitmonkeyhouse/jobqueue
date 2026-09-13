package jobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// JobOutcome is the terminal outcome recorded for one execution attempt.
type JobOutcome string

const (
	OutcomeCompleted    JobOutcome = "completed"
	OutcomeFailed       JobOutcome = "failed"
	OutcomeCancelled    JobOutcome = "cancelled"
	OutcomeLeaseExpired JobOutcome = "lease_expired"
)

// JobEventType is the bounded set of generic, queue-owned lifecycle events.
type JobEventType string

const (
	EventEnqueued        JobEventType = "enqueued"
	EventClaimed         JobEventType = "claimed"
	EventLeaseRenewed    JobEventType = "lease_renewed"
	EventLeaseLost       JobEventType = "lease_lost"
	EventRetryScheduled  JobEventType = "retry_scheduled"
	EventCancelRequested JobEventType = "cancel_requested"
	EventCancelled       JobEventType = "cancelled"
	EventCompleted       JobEventType = "completed"
	EventFailed          JobEventType = "failed"
	EventReclaimed       JobEventType = "reclaimed"
)

// JobAttempt is one claim of a job. Nullable fields are nil until the attempt
// finishes (or while later packages add worker identity/heartbeat data).
type JobAttempt struct {
	ID           int64
	JobID        int64
	Queue        string
	Attempt      int
	WorkerID     *string
	ClaimedAt    time.Time
	HeartbeatAt  *time.Time
	FinishedAt   *time.Time
	Outcome      *JobOutcome
	ErrorCode    *string
	ErrorMessage *string
	DurationMS   *int64
}

// JobEvent is one row of the append-only queue timeline. Detail carries bounded
// generic fields only; it never contains handler errors or sensitive payloads.
type JobEvent struct {
	Seq    int64
	JobID  int64
	At     time.Time
	Type   JobEventType
	Detail json.RawMessage
}

// sqlExecer is the subset of *sql.Tx and *sql.DB used for history writes. Both
// the enqueue transaction and the settlement transaction satisfy it, so history
// rows are always written in the caller's transaction.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertEvent(ctx context.Context, execer sqlExecer, jobID int64, eventType JobEventType, detail map[string]any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode job event detail: %w", err)
	}
	if _, err := execer.ExecContext(ctx,
		`INSERT INTO job_events (job_id, type, detail) VALUES ($1,$2,$3::jsonb)`,
		jobID, string(eventType), encoded); err != nil {
		return fmt.Errorf("insert job event %s: %w", eventType, err)
	}
	return nil
}

func finishAttempt(ctx context.Context, execer sqlExecer, jobID int64, attempt int, outcome JobOutcome, code, message string) error {
	var codeArg, messageArg any
	if code != "" {
		codeArg = code
	}
	if message != "" {
		messageArg = message
	}
	if _, err := execer.ExecContext(ctx, `
UPDATE job_attempts
SET finished_at=clock_timestamp(), outcome=$3, error_code=$4, error_message=$5,
    duration_ms=GREATEST(0, (EXTRACT(EPOCH FROM (clock_timestamp()-claimed_at))*1000)::bigint)
WHERE job_id=$1 AND attempt=$2 AND outcome IS NULL`,
		jobID, attempt, string(outcome), codeArg, messageArg); err != nil {
		return fmt.Errorf("finish job attempt: %w", err)
	}
	return nil
}

// attemptDetail builds a bounded event detail from a job settlement. errorCode,
// when non-empty, is a safe code (never a raw handler error).
func attemptDetail(job Job, errorCode string) map[string]any {
	detail := map[string]any{"attempt": job.Attempts}
	if job.WorkerID != "" {
		detail["worker_id"] = job.WorkerID
	}
	if errorCode != "" {
		detail["error_code"] = errorCode
	}
	return detail
}
