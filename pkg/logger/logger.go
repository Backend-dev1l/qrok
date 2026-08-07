// Package logger — единая настройка slog для всех бинарей qrok:
// JSON в проде, цветной pretty-вывод в dev/CLI, уровень из конфига,
// проброс через context и фанаут в несколько хендлеров (NewMultiHandler).
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"qrok/pkg/fault"
)

const (
	FormatJSON   = "json"
	FormatText   = "text"
	FormatPretty = "pretty" // цветной человекочитаемый вывод для dev/CLI
)

type Config struct {
	Level     string // debug | info | warn | error (по умолчанию info)
	Format    string // json | text | pretty (по умолчанию json)
	AddSource bool   // добавлять file:line к записям (json/text)
}

// New создаёт настроенный *slog.Logger, пишущий в w (обычно os.Stdout).
func New(w io.Writer, cfg Config) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{Level: level, AddSource: cfg.AddSource}

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case FormatText:
		handler = slog.NewTextHandler(w, opts)
	case FormatPretty:
		handler = NewPrettyHandler(w, &PrettyOptions{Level: level})
	case FormatJSON, "":
		handler = slog.NewJSONHandler(w, opts)
	default:
		return nil, fault.ErrValidation.
			Newf("unknown log format %q", cfg.Format).
			WithOp("logger.new").
			WithHint("allowed values: json, text, pretty")
	}

	return slog.New(handler), nil
}

// MustNew — как New, но паникует при невалидном конфиге. Для main(), где
// без логгера всё равно продолжать нечем.
func MustNew(w io.Writer, cfg Config) *slog.Logger {
	l, err := New(w, cfg)
	if err != nil {
		panic(err)
	}
	return l
}

// Default — логгер по умолчанию для main() до чтения конфига: JSON, info, stdout.
func Default() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fault.ErrValidation.
			Newf("unknown log level %q", s).
			WithOp("logger.new").
			WithHint("allowed values: debug, info, warn, error")
	}
}

type ctxKey struct{}

// NewContext кладёт логгер в контекст — так request_id/tenant_id, добавленные
// middleware через logger.With(...), доезжают до всех слоёв без глобальных переменных.
func NewContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext достаёт логгер из контекста; если его там нет — возвращает Default,
// чтобы вызывающий код никогда не получал nil.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return Default()
}

// Error логирует ошибку со всеми структурированными атрибутами fault
// (error_code, op, args, полная цепочка причин).
func Error(ctx context.Context, msg string, err error) {
	FromContext(ctx).LogAttrs(ctx, slog.LevelError, msg, fault.LogAttrs(err)...)
}
