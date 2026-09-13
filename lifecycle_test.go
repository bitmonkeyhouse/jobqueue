package jobqueue

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestJobFromContext(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	type result struct {
		info JobInfo
		ok   bool
	}
	got := make(chan result, 1)
	registry := NewRegistry()
	if err := registry.Register("ctx.command", func(handlerCtx context.Context, _ []byte) error {
		info, ok := JobFromContext(handlerCtx)
		got <- result{info: info, ok: ok}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	id, err := NewDispatcher(db).Dispatch(ctx, "ctx.command", map[string]any{},
		Queue("ctx.queue"), SequenceKey("project:1"))
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{DB: db, Registry: registry, WorkerID: "ctx-worker", Queue: "ctx.queue"}
	if processed, err := worker.ProcessNext(ctx); err != nil || !processed {
		t.Fatalf("ProcessNext = %v, %v", processed, err)
	}
	gotResult := <-got
	if !gotResult.ok {
		t.Fatal("JobFromContext returned false inside a handler")
	}
	if gotResult.info.ID != id || gotResult.info.Attempt != 1 || gotResult.info.WorkerID != "ctx-worker" ||
		gotResult.info.Queue != "ctx.queue" || gotResult.info.SequenceKey != "project:1" {
		t.Fatalf("job info = %+v", gotResult.info)
	}
}

func TestJobFromContextOutsideHandler(t *testing.T) {
	if _, ok := JobFromContext(context.Background()); ok {
		t.Fatal("JobFromContext returned true outside a handler")
	}
}

func TestWorkerStartLogsIdentity(t *testing.T) {
	db := testDB(t)
	var buffer bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	worker := &Worker{DB: db, Registry: NewRegistry(), WorkerID: "log-worker",
		PollInterval: time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(&buffer, nil))}
	exited := make(chan error, 1)
	go func() { exited <- worker.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
	if !strings.Contains(buffer.String(), "log-worker") {
		t.Fatalf("worker identity not logged: %s", buffer.String())
	}
}
