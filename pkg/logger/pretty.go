package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

const (
	ansiReset  = "\x1b[0m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
	ansiBold   = "\x1b[1m"
)

type PrettyOptions struct {
	Level   slog.Leveler // минимальный уровень, по умолчанию info
	NoColor bool         // NO_COLOR из окружения имеет приоритет
}

// PrettyHandler — человекочитаемый slog-хендлер для dev-режима и CLI:
//
//	00:05:03.123 INF событие доставлено tunnel=orders event_id=01J... latency_ms=12
//	00:05:04.201 ERR ошибка подписки error_code=SERVICE_UNAVAILABLE op=agent.kafka.subscribe
//
// Уровень подсвечивается цветом (debug — серый, info — голубой, warn — жёлтый,
// error — красный), ключи атрибутов приглушены. Цвет отключается NO_COLOR.
type PrettyHandler struct {
	w      io.Writer
	mu     *sync.Mutex
	level  slog.Leveler
	color  bool
	attrs  []slog.Attr // уже с групповым префиксом в ключе
	groups []string
}

func NewPrettyHandler(w io.Writer, opts *PrettyOptions) *PrettyHandler {
	if opts == nil {
		opts = &PrettyOptions{}
	}
	level := opts.Level
	if level == nil {
		level = slog.LevelInfo
	}
	return &PrettyHandler{
		w:     w,
		mu:    &sync.Mutex{},
		level: level,
		color: !opts.NoColor && os.Getenv("NO_COLOR") == "",
	}
}

func (h *PrettyHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	if !r.Time.IsZero() {
		b.WriteString(h.paint(ansiDim, r.Time.Format("15:04:05.000")))
		b.WriteByte(' ')
	}

	b.WriteString(h.levelBadge(r.Level))
	b.WriteByte(' ')

	if r.Level >= slog.LevelError {
		b.WriteString(h.paint(ansiBold, r.Message))
	} else {
		b.WriteString(r.Message)
	}

	for _, a := range h.attrs {
		h.appendAttr(&b, a, nil)
	}
	r.Attrs(func(a slog.Attr) bool {
		h.appendAttr(&b, a, h.groups)
		return true
	})

	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := h.clone()
	for _, a := range attrs {
		h2.attrs = append(h2.attrs, prefixAttr(a, h.groups))
	}
	return h2
}

func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := h.clone()
	h2.groups = append(h2.groups, name)
	return h2
}

func (h *PrettyHandler) clone() *PrettyHandler {
	return &PrettyHandler{
		w:      h.w,
		mu:     h.mu, // общий mutex: клоны пишут в один writer
		level:  h.level,
		color:  h.color,
		attrs:  append([]slog.Attr(nil), h.attrs...),
		groups: append([]string(nil), h.groups...),
	}
}

// appendAttr пишет " key=value"; группы разворачиваются в префикс через точку.
func (h *PrettyHandler) appendAttr(b *strings.Builder, a slog.Attr, groups []string) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}

	if a.Value.Kind() == slog.KindGroup {
		g := groups
		if a.Key != "" {
			g = append(append([]string(nil), groups...), a.Key)
		}
		for _, ga := range a.Value.Group() {
			h.appendAttr(b, ga, g)
		}
		return
	}

	key := a.Key
	if len(groups) > 0 {
		key = strings.Join(groups, ".") + "." + key
	}

	val := a.Value.String()
	if strings.ContainsAny(val, " \t\n\"") {
		val = fmt.Sprintf("%q", val)
	}

	b.WriteByte(' ')
	b.WriteString(h.paint(ansiDim, key+"="))
	b.WriteString(val)
}

func (h *PrettyHandler) levelBadge(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return h.paint(ansiRed+ansiBold, "ERR")
	case l >= slog.LevelWarn:
		return h.paint(ansiYellow, "WRN")
	case l >= slog.LevelInfo:
		return h.paint(ansiCyan, "INF")
	default:
		return h.paint(ansiDim, "DBG")
	}
}

func (h *PrettyHandler) paint(code, s string) string {
	if !h.color {
		return s
	}
	return code + s + ansiReset
}

func prefixAttr(a slog.Attr, groups []string) slog.Attr {
	if len(groups) == 0 {
		return a
	}
	a.Key = strings.Join(groups, ".") + "." + a.Key
	return a
}
