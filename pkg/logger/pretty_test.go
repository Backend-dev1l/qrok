package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestPrettyHandlerOutput(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(NewPrettyHandler(&buf, &PrettyOptions{NoColor: true}))

	l.Info("event delivered", "tunnel", "orders", "latency_ms", 12)

	out := buf.String()
	for _, want := range []string{"INF", "event delivered", "tunnel=orders", "latency_ms=12"} {
		if !strings.Contains(out, want) {
			t.Errorf("в выводе нет %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b[") {
		t.Errorf("при NoColor не должно быть ANSI-кодов:\n%s", out)
	}
}

func TestPrettyHandlerColor(t *testing.T) {
	t.Setenv("NO_COLOR", "") // на случай NO_COLOR в окружении CI

	var buf bytes.Buffer
	l := slog.New(NewPrettyHandler(&buf, nil))

	l.Error("everything broke")

	if !strings.Contains(buf.String(), ansiRed) {
		t.Errorf("error-уровень должен быть красным:\n%q", buf.String())
	}
}

func TestPrettyHandlerLevels(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(NewPrettyHandler(&buf, &PrettyOptions{Level: slog.LevelWarn, NoColor: true}))

	l.Info("miss")
	l.Warn("warning")
	l.Error("error")

	out := buf.String()
	if strings.Contains(out, "miss") {
		t.Error("info-запись прошла при уровне warn")
	}
	if !strings.Contains(out, "WRN") || !strings.Contains(out, "ERR") {
		t.Errorf("нет бейджей WRN/ERR:\n%s", out)
	}
}

func TestPrettyHandlerGroupsAndWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(NewPrettyHandler(&buf, &PrettyOptions{NoColor: true})).
		With("request_id", "req-1").
		WithGroup("req")

	l.Info("hello", "path", "/api/v1/events")

	out := buf.String()
	if !strings.Contains(out, "request_id=req-1") {
		t.Errorf("атрибут из With потерялся:\n%s", out)
	}
	if !strings.Contains(out, "req.path=/api/v1/events") {
		t.Errorf("группа не развернулась в префикс:\n%s", out)
	}
}

func TestPrettyHandlerQuotesValuesWithSpaces(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(NewPrettyHandler(&buf, &PrettyOptions{NoColor: true}))

	l.Info("msg", "err", "connection refused by peer")

	if !strings.Contains(buf.String(), `err="connection refused by peer"`) {
		t.Errorf("значение с пробелами должно быть в кавычках:\n%s", buf.String())
	}
}

func TestPrettyViaConfig(t *testing.T) {
	var buf bytes.Buffer
	l, err := New(&buf, Config{Format: "pretty"})
	if err != nil {
		t.Fatal(err)
	}
	l.Info("hi")
	if strings.HasPrefix(buf.String(), "{") {
		t.Errorf("Format=pretty не должен давать JSON:\n%s", buf.String())
	}
}

func TestMultiHandlerFanout(t *testing.T) {
	var pretty, jsonBuf bytes.Buffer
	l := slog.New(NewMultiHandler(
		NewPrettyHandler(&pretty, &PrettyOptions{NoColor: true}),
		slog.NewJSONHandler(&jsonBuf, nil),
	))

	l.Info("fanout", "key", "value")

	if !strings.Contains(pretty.String(), "fanout") {
		t.Errorf("запись не дошла до pretty-хендлера: %s", pretty.String())
	}
	var rec map[string]any
	if err := json.Unmarshal(jsonBuf.Bytes(), &rec); err != nil {
		t.Fatalf("запись не дошла до JSON-хендлера: %v", err)
	}
	if rec["key"] != "value" {
		t.Errorf("атрибуты потерялись в JSON-ветке: %v", rec)
	}
}

func TestMultiHandlerPerHandlerLevels(t *testing.T) {
	var debugBuf, errorBuf bytes.Buffer
	l := slog.New(NewMultiHandler(
		slog.NewJSONHandler(&debugBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.NewJSONHandler(&errorBuf, &slog.HandlerOptions{Level: slog.LevelError}),
	))

	l.Debug("only in first")

	if debugBuf.Len() == 0 {
		t.Error("debug-хендлер должен был принять запись")
	}
	if errorBuf.Len() != 0 {
		t.Errorf("error-хендлер не должен был принять debug-запись: %s", errorBuf.String())
	}
}

func TestMultiHandlerWithAttrsPropagates(t *testing.T) {
	var a, b bytes.Buffer
	l := slog.New(NewMultiHandler(
		slog.NewJSONHandler(&a, nil),
		slog.NewJSONHandler(&b, nil),
	)).With("request_id", "req-9")

	l.Info("hi")

	for name, buf := range map[string]*bytes.Buffer{"first": &a, "second": &b} {
		if !strings.Contains(buf.String(), "req-9") {
			t.Errorf("WithAttrs не дошёл до хендлера %s: %s", name, buf.String())
		}
	}
}

func TestMultiHandlerEnabled(t *testing.T) {
	m := NewMultiHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError}),
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn}),
	)

	ctx := context.Background()
	if !m.Enabled(ctx, slog.LevelWarn) {
		t.Error("warn должен проходить: второй хендлер его принимает")
	}
	if m.Enabled(ctx, slog.LevelInfo) {
		t.Error("info не должен проходить: оба хендлера строже")
	}
}
