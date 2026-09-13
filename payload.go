package jobqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultQueue       = "default"
	DefaultRetryWindow = 24 * time.Hour
)

type Payload struct {
	Command   string          `json:"command"`
	Arguments json.RawMessage `json:"arguments"`
	Sensitive []string        `json:"sensitive"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
}

type dispatchConfig struct {
	queue               string
	delayNanos          int64
	retryWindowNanos    int64
	idempotencyKey      string
	sensitive           []string
	metadata            map[string]any
	sequenceKey         string
	sequenceConcurrency int
	maxAttempts         int
}

type Option func(*dispatchConfig) error

func Queue(name string) Option {
	normalized := strings.TrimSpace(name)
	return func(c *dispatchConfig) error {
		if normalized == "" {
			return errors.New("queue name is required")
		}
		c.queue = normalized
		return nil
	}
}

// RetryWindow sets the total period during which attempts may run. The window
// begins when the job first becomes available, after any dispatch delay.
func RetryWindow(window time.Duration) Option {
	return func(c *dispatchConfig) error {
		if window <= 0 {
			return errors.New("job retry window must be positive")
		}
		c.retryWindowNanos = int64(window)
		return nil
	}
}

func Delay(delay time.Duration) Option {
	return func(c *dispatchConfig) error {
		if delay < 0 {
			return errors.New("job delay cannot be negative")
		}
		c.delayNanos = int64(delay)
		return nil
	}
}

// IdempotencyKey prevents more than one available or reserved job with the
// same key on a queue. Terminal history does not prevent a later dispatch.
func IdempotencyKey(key string) Option {
	normalized := strings.TrimSpace(key)
	return func(c *dispatchConfig) error {
		if normalized == "" {
			return errors.New("job idempotency key is required")
		}
		c.idempotencyKey = normalized
		return nil
	}
}

func Sensitive(pointer string) Option {
	return func(c *dispatchConfig) error {
		c.sensitive = append(c.sensitive, pointer)
		return nil
	}
}

func SensitiveArguments() Option { return Sensitive("/arguments") }

func Metadata(values map[string]any) Option {
	encoded, encodeErr := json.Marshal(values)
	return func(c *dispatchConfig) error {
		if encodeErr != nil {
			return fmt.Errorf("marshal job metadata: %w", encodeErr)
		}
		var cloned map[string]any
		if err := json.Unmarshal(encoded, &cloned); err != nil {
			return fmt.Errorf("clone job metadata: %w", err)
		}
		c.metadata = cloned
		return nil
	}
}

// SequenceKey groups related jobs so at most SequenceConcurrency of them run at
// once. The key is opaque text; the queue has no concept of projects.
func SequenceKey(key string) Option {
	normalized := strings.TrimSpace(key)
	return func(c *dispatchConfig) error {
		if normalized == "" {
			return errors.New("sequence key is required")
		}
		c.sequenceKey = normalized
		return nil
	}
}

// SequenceConcurrency overrides the queue's default for jobs sharing this job's
// sequence key. Zero leaves the queue default in place.
func SequenceConcurrency(limit int) Option {
	return func(c *dispatchConfig) error {
		if limit < 0 {
			return errors.New("sequence concurrency cannot be negative")
		}
		c.sequenceConcurrency = limit
		return nil
	}
}

// MaxAttempts caps execution claims: MaxAttempts(1) means the handler runs at
// most once. Zero (the default) keeps window-bounded retries.
func MaxAttempts(limit int) Option {
	return func(c *dispatchConfig) error {
		if limit < 0 {
			return errors.New("max attempts cannot be negative")
		}
		c.maxAttempts = limit
		return nil
	}
}

func buildPayload(command string, arguments any, options ...Option) ([]byte, dispatchConfig, error) {
	cfg := dispatchConfig{queue: DefaultQueue, retryWindowNanos: int64(DefaultRetryWindow)}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, cfg, err
		}
	}
	command = strings.TrimSpace(command)
	if command == "" {
		return nil, cfg, errors.New("job command is required")
	}
	args, err := json.Marshal(arguments)
	if err != nil {
		return nil, cfg, fmt.Errorf("marshal job arguments: %w", err)
	}
	var decoded any
	if err := json.Unmarshal(args, &decoded); err != nil {
		return nil, cfg, fmt.Errorf("decode job arguments: %w", err)
	}
	if decoded == nil {
		decoded = map[string]any{}
		args = json.RawMessage(`{}`)
	} else {
		switch decoded.(type) {
		case map[string]any, []any:
		default:
			return nil, cfg, errors.New("job arguments must be an object, array, or null")
		}
	}
	sensitive := append([]string{}, cfg.sensitive...)
	payload := Payload{Command: command, Arguments: args, Sensitive: sensitive, Metadata: cfg.metadata}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, cfg, fmt.Errorf("marshal job payload: %w", err)
	}
	if err := validatePayload(data); err != nil {
		return nil, cfg, err
	}
	return data, cfg, nil
}

func validatePayload(data []byte) error {
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("invalid job payload: %w", err)
	}
	command, ok := document["command"].(string)
	if !ok || strings.TrimSpace(command) == "" {
		return errors.New("job payload command is required")
	}
	arguments, ok := document["arguments"]
	if !ok {
		return errors.New("job payload arguments are required")
	}
	if arguments != nil {
		switch arguments.(type) {
		case map[string]any, []any:
		default:
			return errors.New("job payload arguments must be an object, array, or null")
		}
	}
	if metadata, exists := document["metadata"]; exists {
		if _, ok := metadata.(map[string]any); !ok {
			return errors.New("job payload metadata must be an object")
		}
	}
	paths, ok := document["sensitive"].([]any)
	if !ok {
		return errors.New("job payload sensitive paths must be an array")
	}
	seen := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		pointer, ok := value.(string)
		if !ok {
			return errors.New("sensitive JSON Pointer must be a string")
		}
		if pointer != "/arguments" && !strings.HasPrefix(pointer, "/arguments/") {
			return fmt.Errorf("sensitive JSON Pointer %q must target arguments", pointer)
		}
		if _, exists := seen[pointer]; exists {
			return fmt.Errorf("duplicate sensitive JSON Pointer %q", pointer)
		}
		if _, err := valueAtPointer(document, pointer); err != nil {
			return fmt.Errorf("sensitive JSON Pointer %q: %w", pointer, err)
		}
		seen[pointer] = struct{}{}
	}
	return nil
}

func valueAtPointer(document any, pointer string) (any, error) {
	if pointer == "" {
		return document, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("must start with /")
	}
	current := document
	for _, raw := range strings.Split(pointer[1:], "/") {
		token, err := decodePointerToken(raw)
		if err != nil {
			return nil, err
		}
		switch value := current.(type) {
		case map[string]any:
			var exists bool
			current, exists = value[token]
			if !exists {
				return nil, errors.New("path does not exist")
			}
		case []any:
			index, err := parseArrayIndex(token, len(value))
			if err != nil {
				return nil, err
			}
			current = value[index]
		default:
			return nil, errors.New("path traverses a scalar value")
		}
	}
	return current, nil
}

func decodePointerToken(token string) (string, error) {
	var result strings.Builder
	for i := 0; i < len(token); i++ {
		if token[i] != '~' {
			result.WriteByte(token[i])
			continue
		}
		if i+1 >= len(token) {
			return "", errors.New("invalid ~ escape")
		}
		i++
		switch token[i] {
		case '0':
			result.WriteByte('~')
		case '1':
			result.WriteByte('/')
		default:
			return "", errors.New("invalid ~ escape")
		}
	}
	return result.String(), nil
}

func parseArrayIndex(token string, length int) (int, error) {
	if token == "" || token == "-" || (len(token) > 1 && token[0] == '0') {
		return 0, errors.New("invalid array index")
	}
	parsed, err := strconv.ParseUint(token, 10, 63)
	if err != nil || parsed > uint64(^uint(0)>>1) {
		return 0, errors.New("invalid array index")
	}
	index := int(parsed)
	if index >= length {
		return 0, errors.New("array index is out of range")
	}
	return index, nil
}

func redactPayload(data []byte) ([]byte, error) {
	if err := validatePayload(data); err != nil {
		return nil, err
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	paths := document["sensitive"].([]any)
	sort.SliceStable(paths, func(i, j int) bool {
		return strings.Count(paths[i].(string), "/") > strings.Count(paths[j].(string), "/")
	})
	for _, value := range paths {
		if err := removeAtPointer(document, value.(string)); err != nil {
			return nil, err
		}
	}
	document["sensitive"] = []any{}
	document["redacted"] = true
	return json.Marshal(document)
}

func removeAtPointer(document map[string]any, pointer string) error {
	if pointer == "" {
		return errors.New("redacting the entire payload is not allowed")
	}
	parts := strings.Split(pointer[1:], "/")
	current := any(document)
	for i, raw := range parts {
		token, err := decodePointerToken(raw)
		if err != nil {
			return err
		}
		last := i == len(parts)-1
		switch value := current.(type) {
		case map[string]any:
			if last {
				if pointer == "/arguments" {
					value[token] = nil
				} else {
					delete(value, token)
				}
				return nil
			}
			current = value[token]
		case []any:
			index, err := parseArrayIndex(token, len(value))
			if err != nil {
				return err
			}
			if last {
				value[index] = nil
				return nil
			}
			current = value[index]
		default:
			return errors.New("path traverses a scalar value")
		}
	}
	return nil
}
