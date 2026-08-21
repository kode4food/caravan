package testing

import (
	"context"
	"log/slog"
)

// TestSlogHandler captures log records for tests
type TestSlogHandler struct {
	Logs chan slog.Record
}

// NewTestSlogHandler returns a Handler that captures debug-level records
func NewTestSlogHandler() *TestSlogHandler {
	return &TestSlogHandler{
		Logs: make(chan slog.Record, 10),
	}
}

// Enabled reports whether the level should be captured
func (h *TestSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelDebug
}

// Handle captures a record when buffer space is available
func (h *TestSlogHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.Logs <- r:
	default:
	}
	return nil
}

// WithAttrs returns the Handler unchanged
func (h *TestSlogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

// WithGroup returns the Handler unchanged
func (h *TestSlogHandler) WithGroup(_ string) slog.Handler {
	return h
}
