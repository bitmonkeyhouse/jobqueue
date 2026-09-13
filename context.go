package jobqueue

import "context"

// JobInfo describes the execution a handler is currently running. It is read
// with JobFromContext so consumers can correlate domain work with the queue's
// attempt/worker identity without changing the Handler signature.
type JobInfo struct {
	ID            int64
	Queue         string
	Command       string
	Attempt       int
	MaxAttempts   int
	WorkerID      string
	SequenceKey   string
	ReservationID string
}

type jobInfoKey struct{}

// JobFromContext returns the queue execution info for the current handler, or
// false when called outside a job handler.
func JobFromContext(ctx context.Context) (JobInfo, bool) {
	info, ok := ctx.Value(jobInfoKey{}).(JobInfo)
	return info, ok
}

func withJobInfo(ctx context.Context, job Job, workerID string) context.Context {
	return context.WithValue(ctx, jobInfoKey{}, JobInfo{
		ID:            job.ID,
		Queue:         job.Queue,
		Command:       job.Command,
		Attempt:       job.Attempts,
		MaxAttempts:   job.MaxAttempts,
		WorkerID:      workerID,
		SequenceKey:   job.SequenceKey,
		ReservationID: job.ReservationID,
	})
}
