package jobqueue

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPayloadValidatesAndRedactsNestedPointers(t *testing.T) {
	payload, _, err := buildPayload("example.send", map[string]any{
		"profile": map[string]any{"email/address": "person@example.com", "name": "Safe"},
		"items":   []any{"secret", "safe"},
	}, Sensitive("/arguments/profile/email~1address"), Sensitive("/arguments/items/0"), Metadata(map[string]any{"kind": "example"}))
	if err != nil {
		t.Fatalf("buildPayload: %v", err)
	}
	redacted, err := redactPayload(payload)
	if err != nil {
		t.Fatalf("redactPayload: %v", err)
	}
	if strings.Contains(string(redacted), "person@example.com") || strings.Contains(string(redacted), "secret") {
		t.Fatalf("redacted payload retains sensitive values: %s", redacted)
	}
	if !strings.Contains(string(redacted), `"name":"Safe"`) || !strings.Contains(string(redacted), `"kind":"example"`) {
		t.Fatalf("redacted payload lost safe values: %s", redacted)
	}
}

func TestSensitiveArgumentsRedactsWholeArguments(t *testing.T) {
	payload, _, err := buildPayload("email.send", map[string]string{"code": "123456"}, SensitiveArguments())
	if err != nil {
		t.Fatal(err)
	}
	redacted, err := redactPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(redacted, &document); err != nil {
		t.Fatal(err)
	}
	if document["arguments"] != nil || document["command"] != "email.send" || document["redacted"] != true {
		t.Fatalf("terminal payload = %#v", document)
	}
}

func TestOptionsCanBeReusedConcurrently(t *testing.T) {
	options := []Option{
		Queue(" priority "),
		IdempotencyKey(" example:1 "),
		RetryWindow(7),
		Delay(3),
		Sensitive("/arguments/secret"),
		Metadata(map[string]any{"kind": "example"}),
	}
	const goroutines = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload, cfg, err := buildPayload("example", map[string]any{"secret": i}, options...)
			if err != nil {
				errs <- err
				return
			}
			if cfg.queue != "priority" || cfg.idempotencyKey != "example:1" || cfg.retryWindowNanos != 7 || !strings.Contains(string(payload), `"kind":"example"`) {
				errs <- fmt.Errorf("unexpected config/payload: %+v %s", cfg, payload)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestRetryWindowRejectsNonPositiveValue(t *testing.T) {
	for _, window := range []time.Duration{0, -time.Second} {
		if _, _, err := buildPayload("example", map[string]string{"value": "x"}, RetryWindow(window)); err == nil {
			t.Fatalf("retry window %s accepted", window)
		}
	}
}

func TestIdempotencyKeyRejectsBlankValue(t *testing.T) {
	if _, _, err := buildPayload("example", map[string]string{"value": "x"}, IdempotencyKey(" ")); err == nil {
		t.Fatal("blank idempotency key accepted")
	}
}

func TestSensitivePointerMustTargetArguments(t *testing.T) {
	_, _, err := buildPayload("example.send", map[string]string{"value": "x"}, Metadata(map[string]any{"secret": "x"}), Sensitive("/metadata/secret"))
	if err == nil || !strings.Contains(err.Error(), "must target arguments") {
		t.Fatalf("error = %v, want arguments-only pointer", err)
	}
}

func TestSensitivePointerMustExist(t *testing.T) {
	_, _, err := buildPayload("example.send", map[string]string{"value": "x"}, Sensitive("/arguments/missing"))
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %v, want missing pointer", err)
	}
}

func TestPayloadRejectsScalarArguments(t *testing.T) {
	for _, arguments := range []any{"value", 42, true} {
		if _, _, err := buildPayload("example", arguments); err == nil || !strings.Contains(err.Error(), "object, array, or null") {
			t.Fatalf("scalar %#v error = %v", arguments, err)
		}
	}
	if _, _, err := buildPayload("example", nil); err != nil {
		t.Fatalf("nil arguments should normalize to object: %v", err)
	}
}

func TestPayloadRejectsInvalidPointerEscapesAndArrayIndexes(t *testing.T) {
	for _, pointer := range []string{"arguments/value", "/arguments/~2", "/arguments/items/01", "/arguments/items/3", "/arguments/items/999999999999999999999999999999999999999"} {
		_, _, err := buildPayload("example", map[string]any{"value": "x", "items": []string{"a"}}, Sensitive(pointer))
		if err == nil {
			t.Fatalf("pointer %q unexpectedly accepted", pointer)
		}
	}
}
