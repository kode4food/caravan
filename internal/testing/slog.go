package testing

import (
	"context"
	"log/slog"
)

type TestSlogHandler struct {
	Logs     chan slog.Record
	minLevel slog.Leveler
}

func NewTestSlogHandler() *TestSlogHandler {
	var minLevel slog.LevelVar
	minLevel.Set(slog.LevelDebug)
	return &TestSlogHandler{
		Logs:     make(chan slog.Record, 10),
		minLevel: &minLevel,
	}
}

func (h *TestSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel.Level()
}

func (h *TestSlogHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.Logs <- r:
	default:
	}
	return nil
}

func (h *TestSlogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *TestSlogHandler) WithGroup(_ string) slog.Handler {
	return h
}
