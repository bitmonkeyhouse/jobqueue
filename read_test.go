package jobqueue

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestReadAPIHidesSensitiveArgumentsForEveryStatus(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("read.secret", func(context.Context, []byte) error { return nil })

	arguments := map[string]any{"token": "read-secret-value", "public": "visible"}
	id, err := dispatcher.Dispatch(ctx, "read.secret", arguments, Sensitive("/arguments/token"))
	if err != nil {
		t.Fatal(err)
	}

	assertRedacted := func(stage string) {
		t.Helper()
		view, err := dispatcher.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("%s: GetJob: %v", stage, err)
		}
		raw := string(view.Arguments)
		if strings.Contains(raw, "read-secret-value") {
			t.Fatalf("%s: sensitive value leaked: %s", stage, raw)
		}
		if !strings.Contains(raw, "visible") {
			t.Fatalf("%s: non-sensitive value missing: %s", stage, raw)
		}
	}

	assertRedacted("available")

	worker := &Worker{DB: db, Registry: registry}
	job, err := claimNextFor(ctx, db, DefaultQueue, 0, worker.workerID())
	if err != nil {
		t.Fatal(err)
	}
	assertRedacted("reserved")

	if err := completeJob(ctx, db, job); err != nil {
		t.Fatal(err)
	}
	assertRedacted("completed")
}

func TestReadAPIRedactsWholeArgumentsWhenMarked(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, err := NewDispatcher(db).Dispatch(ctx, "read.all", map[string]any{"token": "all-secret"}, SensitiveArguments())
	if err != nil {
		t.Fatal(err)
	}
	view, err := NewDispatcher(db).GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(view.Arguments), "all-secret") {
		t.Fatalf("whole-arguments redaction leaked: %s", view.Arguments)
	}
}

func TestReadAPIListFilterAndPaginate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	var ids []int64
	for i := 0; i < 3; i++ {
		id, err := dispatcher.Dispatch(ctx, "read.list", map[string]any{"n": i}, Queue("read.a"))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := dispatcher.Dispatch(ctx, "read.list", map[string]any{}, Queue("read.b")); err != nil {
		t.Fatal(err)
	}

	page, err := dispatcher.ListJobs(ctx, JobFilter{Queues: []string{"read.a"}, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].ID != ids[0] || page[1].ID != ids[1] {
		t.Fatalf("first page = %+v", page)
	}
	next, err := dispatcher.ListJobs(ctx, JobFilter{Queues: []string{"read.a"}, AfterID: page[1].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || next[0].ID != ids[2] {
		t.Fatalf("second page = %+v", next)
	}
	byStatus, err := dispatcher.ListJobs(ctx, JobFilter{Status: []string{"available"}, Queues: []string{"read.a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(byStatus) != 3 {
		t.Fatalf("status filter returned %d jobs", len(byStatus))
	}
}

func TestReadAPICountsAndStats(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("read.count", func(context.Context, []byte) error { return nil })
	if _, err := dispatcher.Dispatch(ctx, "read.count", map[string]any{}, Queue("read.counts")); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(ctx, "read.count", map[string]any{}, Queue("read.counts")); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, Queue: "read.counts"}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}

	counts, err := dispatcher.CountJobs(ctx, JobFilter{Queues: []string{"read.counts"}})
	if err != nil {
		t.Fatal(err)
	}
	byStatus := map[string]int64{}
	for _, count := range counts {
		byStatus[count.Status] = count.Count
	}
	if byStatus["completed"] != 1 || byStatus["available"] != 1 {
		t.Fatalf("counts = %+v", byStatus)
	}

	stats, err := dispatcher.QueueStats(ctx, []string{"read.counts"})
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 || stats[0].Available != 1 || stats[0].Completed != 1 || stats[0].OldestAvailableAt == nil {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestReadAPIAttemptsAndEventResume(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dispatcher := NewDispatcher(db)
	registry := NewRegistry()
	_ = registry.Register("read.history", func(context.Context, []byte) error { return nil })
	id, err := dispatcher.Dispatch(ctx, "read.history", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}

	attempts, err := dispatcher.ListAttempts(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Outcome == nil || *attempts[0].Outcome != OutcomeCompleted {
		t.Fatalf("attempts = %+v", attempts)
	}

	events, err := dispatcher.ListEvents(ctx, id, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 {
		t.Fatalf("events = %+v", events)
	}
	afterFirst, err := dispatcher.ListEvents(ctx, id, events[0].Seq, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFirst) != len(events)-1 || afterFirst[0].Seq <= events[0].Seq {
		t.Fatalf("resume after seq %d returned %d events", events[0].Seq, len(afterFirst))
	}
	seen := map[int64]bool{}
	for _, event := range events {
		if seen[event.Seq] {
			t.Fatalf("duplicate seq %d", event.Seq)
		}
		seen[event.Seq] = true
	}
	var detail map[string]any
	if err := json.Unmarshal(afterFirst[len(afterFirst)-1].Detail, &detail); err != nil {
		t.Fatalf("event detail is not an object: %v", err)
	}
}

func TestReadAPIGetJobNotFound(t *testing.T) {
	db := testDB(t)
	if _, err := NewDispatcher(db).GetJob(context.Background(), 424242); err != ErrJobNotFound {
		t.Fatalf("GetJob unknown = %v", err)
	}
}
