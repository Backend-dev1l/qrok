package logger

import (
	"context"
	"errors"
	"log/slog"
)

// multiHandler — фанаут одной записи в несколько slog-хендлеров.
// Типовой сценарий: pretty в терминал для человека + JSON в файл/коллектор
// для машины, каждый со своим уровнем:
//
//	log := slog.New(logger.NewMultiHandler(
//	    logger.NewPrettyHandler(os.Stderr, nil),
//	    slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug}),
//	))
type multiHandler struct {
	handlers []slog.Handler
}

func NewMultiHandler(handlers ...slog.Handler) slog.Handler {
	return &multiHandler{handlers: handlers}
}

// Enabled — запись проходит, если её готов принять хотя бы один хендлер;
// точная фильтрация по уровню происходит в Handle для каждого отдельно.
func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: next}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: next}
}
