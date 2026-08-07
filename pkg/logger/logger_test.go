package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"qrok/pkg/fault"
)

func TestNewJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	l, err := New(&buf, Config{Level: "info", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}

	l.Info("hello", "key", "value")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("вывод не JSON: %v\n%s", err, buf.String())
	}
	if rec["msg"] != "hello" || rec["key"] != "value" {
		t.Errorf("неожиданная запись: %v", rec)
	}
}

func TestNewTextFormat(t *testing.T) {
	var buf bytes.Buffer
	l, err := New(&buf, Config{Format: "text"})
	if err != nil {
		t.Fatal(err)
	}

	l.Info("hello")

	if strings.HasPrefix(buf.String(), "{") {
		t.Errorf("ожидался текстовый формат, получен JSON: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "msg=hello") {
		t.Errorf("нет msg=hello: %s", buf.String())
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l, err := New(&buf, Config{Level: "warn"})
	if err != nil {
		t.Fatal(err)
	}

	l.Info("should not appear")
	l.Warn("should appear")

	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Error("info-запись прошла при уровне warn")
	}
	if !strings.Contains(out, "should appear") {
		t.Error("warn-запись не прошла")
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, Config{Level: "trace"}); fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("невалидный уровень: ожидался VALIDATION_ERROR, получено %v", err)
	}
	if _, err := New(&bytes.Buffer{}, Config{Format: "xml"}); fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("невалидный формат: ожидался VALIDATION_ERROR, получено %v", err)
	}
}

func TestDefaultsAreValid(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, Config{}); err != nil {
		t.Errorf("пустой конфиг должен быть валиден (json/info): %v", err)
	}
}

func TestContextRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	l := MustNew(&buf, Config{}).With("request_id", "req-1")

	ctx := NewContext(context.Background(), l)
	FromContext(ctx).Info("from ctx")

	if !strings.Contains(buf.String(), "req-1") {
		t.Errorf("атрибуты логгера из контекста потерялись: %s", buf.String())
	}

	if FromContext(context.Background()) == nil {
		t.Error("FromContext без логгера должен вернуть Default, а не nil")
	}
}

func TestErrorHelper(t *testing.T) {
	var buf bytes.Buffer
	ctx := NewContext(context.Background(), MustNew(&buf, Config{}))

	err := fault.ErrServiceUnavail.
		Wrap(errors.New("connection refused"), "kafka unavailable").
		WithOp("agent.kafka.subscribe")
	Error(ctx, "subscription error", err)

	var rec map[string]any
	if jsonErr := json.Unmarshal(buf.Bytes(), &rec); jsonErr != nil {
		t.Fatalf("вывод не JSON: %v", jsonErr)
	}
	if rec["error_code"] != "SERVICE_UNAVAILABLE" {
		t.Errorf("нет error_code: %v", rec)
	}
	if rec["op"] != "agent.kafka.subscribe" {
		t.Errorf("нет op: %v", rec)
	}
	if !strings.Contains(rec["error"].(string), "connection refused") {
		t.Errorf("цепочка причин потерялась: %v", rec)
	}
}
