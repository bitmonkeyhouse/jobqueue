package jobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JobView is the safe, redacted representation of a job returned by the read
// API. Arguments have the payload's Sensitive JSON pointers removed for every
// status, including available and reserved jobs, so an operator query can never
// expose unredacted sensitive arguments. There is deliberately no raw-payload
// accessor.
type JobView struct {
	ID                int64
	Queue             string
	Command           string
	Status            string
	Arguments         json.RawMessage
	Sensitive         []string
	Metadata          map[string]any
	Attempts          int
	RetryAttempts     int
	MaxAttempts       int
	WorkerID          *string
	AvailableAt       time.Time
	ReservedAt        *time.Time
	LeaseExpiresAt    *time.Time
	CancelRequestedAt *time.Time
	CompletedAt       *time.Time
	FailedAt          *time.Time
	CancelledAt       *time.Time
	LastError         *string
}

// JobFilter selects jobs for the read API. Zero values mean "no filter" and a
// default page size.
type JobFilter struct {
	Queues      []string
	Status      []string
	WorkerID    string
	SequenceKey string
	AfterID     int64
	Limit       int
}

// JobCount is one (queue, status) bucket from CountJobs.
type JobCount struct {
	Queue  string
	Status string
	Count  int64
}

// QueueStat is a per-queue operational summary.
type QueueStat struct {
	Queue             string
	Available         int64
	Reserved          int64
	Completed         int64
	Failed            int64
	Cancelled         int64
	OldestAvailableAt *time.Time
}

var validStatuses = map[string]struct{}{
	"available": {}, "reserved": {}, "completed": {}, "failed": {}, "cancelled": {},
}

const (
	defaultJobLimit   = 100
	maxJobLimit       = 1000
	defaultEventLimit = 500
	maxEventLimit     = 2000
)

func normalizeLimit(limit, def, max int) int {
	if limit <= 0 {
		return def
	}
	if limit > max {
		return max
	}
	return limit
}

// GetJob returns a single redacted job view.
func (d *QueueDispatcher) GetJob(ctx context.Context, jobID int64) (JobView, error) {
	if d == nil || d.db == nil {
		return JobView{}, errors.New("job queue database is required")
	}
	row := d.db.QueryRowContext(ctx, jobViewSelect+` WHERE id=$1`, jobID)
	view, err := scanJobView(row)
	if errors.Is(err, sql.ErrNoRows) {
		return JobView{}, ErrJobNotFound
	}
	if err != nil {
		return JobView{}, fmt.Errorf("get job: %w", err)
	}
	return view, nil
}

const jobViewSelect = `
SELECT id, queue, status, payload, attempts, retry_attempts, max_attempts,
       available_at, reserved_at, lease_expires_at, worker_id,
       cancel_requested_at, completed_at, failed_at, cancelled_at, last_error
FROM jobs`

// ListJobs returns redacted job views ordered by id for stable pagination. Use
// JobFilter.AfterID with the last id of the previous page to continue.
func (d *QueueDispatcher) ListJobs(ctx context.Context, filter JobFilter) ([]JobView, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("job queue database is required")
	}
	where, args := jobFilterClause(filter)
	args = append(args, normalizeLimit(filter.Limit, defaultJobLimit, maxJobLimit))
	query := jobViewSelect + where + fmt.Sprintf(` ORDER BY id ASC LIMIT $%d`, len(args))
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	views := make([]JobView, 0)
	for rows.Next() {
		view, err := scanJobView(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return views, nil
}

func jobFilterClause(filter JobFilter) (string, []any) {
	var clauses []string
	var args []any
	add := func(value any, format string) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(format, len(args)))
	}
	if len(filter.Queues) > 0 {
		add(filter.Queues, "queue = ANY($%d)")
	}
	if len(filter.Status) > 0 {
		add(filter.Status, "status = ANY($%d)")
	}
	if strings.TrimSpace(filter.WorkerID) != "" {
		add(strings.TrimSpace(filter.WorkerID), "worker_id = $%d")
	}
	if strings.TrimSpace(filter.SequenceKey) != "" {
		add(strings.TrimSpace(filter.SequenceKey), "sequence_key = $%d")
	}
	if filter.AfterID > 0 {
		add(filter.AfterID, "id > $%d")
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

// CountJobs returns job counts grouped by queue and status.
func (d *QueueDispatcher) CountJobs(ctx context.Context, filter JobFilter) ([]JobCount, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("job queue database is required")
	}
	where, args := jobFilterClause(filter)
	rows, err := d.db.QueryContext(ctx,
		`SELECT queue, status, count(*) FROM jobs`+where+` GROUP BY queue, status ORDER BY queue, status`, args...)
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()
	counts := make([]JobCount, 0)
	for rows.Next() {
		var count JobCount
		if err := rows.Scan(&count.Queue, &count.Status, &count.Count); err != nil {
			return nil, err
		}
		counts = append(counts, count)
	}
	return counts, rows.Err()
}

// ListAttempts returns every attempt recorded for a job in attempt order.
func (d *QueueDispatcher) ListAttempts(ctx context.Context, jobID int64) ([]JobAttempt, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("job queue database is required")
	}
	rows, err := d.db.QueryContext(ctx, `
SELECT id, job_id, queue, attempt, worker_id, claimed_at, heartbeat_at,
       finished_at, outcome, error_code, error_message, duration_ms
FROM job_attempts WHERE job_id=$1 ORDER BY attempt`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()
	attempts := make([]JobAttempt, 0)
	for rows.Next() {
		var attempt JobAttempt
		var workerID, outcome, errorCode, errorMessage sql.NullString
		var heartbeatAt, finishedAt sql.NullTime
		var duration sql.NullInt64
		if err := rows.Scan(&attempt.ID, &attempt.JobID, &attempt.Queue, &attempt.Attempt,
			&workerID, &attempt.ClaimedAt, &heartbeatAt, &finishedAt, &outcome,
			&errorCode, &errorMessage, &duration); err != nil {
			return nil, err
		}
		attempt.WorkerID = nullStringPtr(workerID)
		attempt.HeartbeatAt = nullTimePtr(heartbeatAt)
		attempt.FinishedAt = nullTimePtr(finishedAt)
		if outcome.Valid {
			value := JobOutcome(outcome.String)
			attempt.Outcome = &value
		}
		attempt.ErrorCode = nullStringPtr(errorCode)
		attempt.ErrorMessage = nullStringPtr(errorMessage)
		if duration.Valid {
			value := duration.Int64
			attempt.DurationMS = &value
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}

// ListEvents returns the append-only job timeline after a sequence number so a
// client can resume with seq > last seen.
func (d *QueueDispatcher) ListEvents(ctx context.Context, jobID, afterSeq int64, limit int) ([]JobEvent, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("job queue database is required")
	}
	rows, err := d.db.QueryContext(ctx, `
SELECT seq, job_id, at, type, detail
FROM job_events WHERE job_id=$1 AND seq > $2 ORDER BY seq LIMIT $3`,
		jobID, afterSeq, normalizeLimit(limit, defaultEventLimit, maxEventLimit))
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	events := make([]JobEvent, 0)
	for rows.Next() {
		var event JobEvent
		if err := rows.Scan(&event.Seq, &event.JobID, &event.At, &event.Type, &event.Detail); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

// QueueStats returns per-queue depth and running counts.
func (d *QueueDispatcher) QueueStats(ctx context.Context, queues []string) ([]QueueStat, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("job queue database is required")
	}
	query := `
SELECT queue,
       count(*) FILTER (WHERE status='available'),
       count(*) FILTER (WHERE status='reserved'),
       count(*) FILTER (WHERE status='completed'),
       count(*) FILTER (WHERE status='failed'),
       count(*) FILTER (WHERE status='cancelled'),
       min(available_at) FILTER (WHERE status='available')
FROM jobs`
	var args []any
	if len(queues) > 0 {
		args = append(args, queues)
		query += ` WHERE queue = ANY($1)`
	}
	query += ` GROUP BY queue ORDER BY queue`
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}
	defer rows.Close()
	stats := make([]QueueStat, 0)
	for rows.Next() {
		var stat QueueStat
		var oldest sql.NullTime
		if err := rows.Scan(&stat.Queue, &stat.Available, &stat.Reserved, &stat.Completed,
			&stat.Failed, &stat.Cancelled, &oldest); err != nil {
			return nil, err
		}
		stat.OldestAvailableAt = nullTimePtr(oldest)
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

func scanJobView(scanner interface{ Scan(...any) error }) (JobView, error) {
	var view JobView
	var payload []byte
	var reservedAt, leaseExpiresAt, cancelRequestedAt, completedAt, failedAt, cancelledAt sql.NullTime
	var workerID, lastError sql.NullString
	if err := scanner.Scan(&view.ID, &view.Queue, &view.Status, &payload, &view.Attempts,
		&view.RetryAttempts, &view.MaxAttempts, &view.AvailableAt, &reservedAt,
		&leaseExpiresAt, &workerID, &cancelRequestedAt, &completedAt, &failedAt,
		&cancelledAt, &lastError); err != nil {
		return JobView{}, err
	}
	view.Command, view.Arguments, view.Sensitive, view.Metadata = redactedView(payload)
	view.WorkerID = nullStringPtr(workerID)
	view.ReservedAt = nullTimePtr(reservedAt)
	view.LeaseExpiresAt = nullTimePtr(leaseExpiresAt)
	view.CancelRequestedAt = nullTimePtr(cancelRequestedAt)
	view.CompletedAt = nullTimePtr(completedAt)
	view.FailedAt = nullTimePtr(failedAt)
	view.CancelledAt = nullTimePtr(cancelledAt)
	view.LastError = nullStringPtr(lastError)
	return view, nil
}

// redactedView applies the payload's Sensitive pointers regardless of job
// status. It never returns unredacted sensitive arguments.
func redactedView(payload []byte) (string, json.RawMessage, []string, map[string]any) {
	var original struct {
		Sensitive []string       `json:"sensitive"`
		Metadata  map[string]any `json:"metadata"`
	}
	_ = json.Unmarshal(payload, &original)
	command := safelyExtractCommand(payload)
	redacted, err := redactPayload(payload)
	if err != nil {
		redacted = terminalRedactionFallback(command)
	}
	var document struct {
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(redacted, &document); err != nil {
		return command, nil, original.Sensitive, original.Metadata
	}
	return command, document.Arguments, original.Sensitive, original.Metadata
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func nullTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
