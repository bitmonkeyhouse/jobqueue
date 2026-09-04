package jobqueue

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

const (
	maxFailureCodeLength    = 64
	maxFailureMessageLength = 500
)

var failureCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// DiagnosticError carries an explicitly safe, stable operator diagnostic while
// retaining the underlying cause for errors.Is/errors.As and structured logs.
// Code and Message must not contain credentials, personal data, provider response
// bodies, or other tenant-sensitive values.
type DiagnosticError struct {
	Code    string
	Message string
	Cause   error
}

func (e *DiagnosticError) Error() string {
	if e == nil {
		return "job failed"
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	if strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	return "job failed"
}

func (e *DiagnosticError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Diagnostic attaches a safe code and operator message to an underlying error.
// Invalid diagnostic metadata safely falls back when the error is persisted.
func Diagnostic(code, message string, cause error) error {
	return &DiagnosticError{Code: code, Message: message, Cause: cause}
}

type failureDiagnostic struct {
	Code    string
	Message string
}

// SafeDiagnostic returns the bounded diagnostic metadata that may be persisted
// or shown to operators. Arbitrary underlying error text is never returned.
func SafeDiagnostic(err error) (code, message string) {
	diagnostic := diagnosticForError(err)
	return diagnostic.Code, diagnostic.Message
}

func diagnosticForError(err error) failureDiagnostic {
	if errors.Is(err, context.DeadlineExceeded) {
		return failureDiagnostic{Code: "job_timeout", Message: "job handler exceeded its timeout"}
	}
	if errors.Is(err, context.Canceled) {
		return failureDiagnostic{Code: "job_canceled", Message: "job handler was canceled"}
	}
	var safe *DiagnosticError
	if errors.As(err, &safe) {
		code := strings.TrimSpace(safe.Code)
		message := strings.TrimSpace(safe.Message)
		if failureCodePattern.MatchString(code) && len(code) <= maxFailureCodeLength && message != "" {
			return failureDiagnostic{Code: code, Message: truncateFailureMessage(message)}
		}
	}
	return failureDiagnostic{Code: "handler_failed", Message: "job handler returned an error"}
}

func truncateFailureMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "job handler returned an error"
	}
	if len(message) <= maxFailureMessageLength {
		return message
	}
	return message[:maxFailureMessageLength]
}

func (d failureDiagnostic) summary() string {
	return d.Code + ": " + d.Message
}
