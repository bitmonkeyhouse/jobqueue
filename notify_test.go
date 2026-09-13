package jobqueue

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNotifyChannelIsLibraryOwned(t *testing.T) {
	worker := &Worker{NotifyChannel: "custom_channel"}
	if got := worker.notifyChannel(); got != DefaultNotifyChannel {
		t.Fatalf("notifyChannel = %q, want library channel %q", got, DefaultNotifyChannel)
	}
}

func TestNotifyChannelDeprecationWarning(t *testing.T) {
	var buffer bytes.Buffer
	worker := &Worker{NotifyChannel: "custom_channel",
		Logger: slog.New(slog.NewTextHandler(&buffer, nil))}
	worker.warnDeprecatedNotifyChannel()
	if !strings.Contains(buffer.String(), "deprecated") {
		t.Fatalf("no deprecation warning emitted: %q", buffer.String())
	}

	buffer.Reset()
	defaulted := &Worker{NotifyChannel: DefaultNotifyChannel,
		Logger: slog.New(slog.NewTextHandler(&buffer, nil))}
	defaulted.warnDeprecatedNotifyChannel()
	if buffer.Len() != 0 {
		t.Fatalf("unexpected warning for the library channel: %q", buffer.String())
	}
}

func TestLibraryChannelDeliversWakeUps(t *testing.T) {
	db, dsn := openIsolatedSchemaWithDSN(t, "test_jobqueue_notify_")
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A custom NotifyChannel must not stop the worker from listening on the
	// library channel.
	worker := &Worker{DB: db, DatabaseURL: dsn, Queue: "notify.queue", NotifyChannel: "custom_channel"}
	notifications := worker.startListener(ctx)

	// The listener emits a rescan wake on connect; drain it first.
	select {
	case <-notifications:
	case <-time.After(3 * time.Second):
		t.Fatal("listener never connected")
	}
	if _, err := NewDispatcher(db).Dispatch(ctx, "notify.command", map[string]any{}, Queue("notify.queue")); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-notifications:
		if payload != "notify.queue" {
			t.Fatalf("notification payload = %q, want queue", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("library channel did not deliver the enqueue wake-up")
	}
}
