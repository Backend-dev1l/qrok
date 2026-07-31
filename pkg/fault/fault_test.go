package fault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestErrorFormat(t *testing.T) {
	tests := []struct {
		name string
		err  *Fault
		want string
	}{
		{
			name: "только код",
			err:  ErrNotFound.Err(),
			want: "NOT_FOUND",
		},
		{
			name: "сообщение",
			err:  ErrNotFound.New("event not found"),
			want: "event not found",
		},
		{
			name: "op + сообщение",
			err:  ErrNotFound.New("event not found").WithOp("eventstore.get"),
			want: "eventstore.get: event not found",
		},
		{
			name: "op + сообщение + причина",
			err:  ErrServiceUnavail.Wrap(errors.New("connection refused"), "kafka недоступна").WithOp("agent.kafka.subscribe"),
			want: "agent.kafka.subscribe: kafka недоступна: connection refused",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWrapPreservesChain(t *testing.T) {
	root := errors.New("dial tcp: connection refused")
	err := ErrServiceUnavail.Wrap(root, "не удалось подключиться")

	if !errors.Is(err, root) {
		t.Error("errors.Is не находит первопричину через Wrap")
	}

	var f *Fault
	if !errors.As(err, &f) {
		t.Fatal("errors.As не находит *Fault")
	}
	if f.Code() != ErrServiceUnavail {
		t.Errorf("Code() = %q, want %q", f.Code(), ErrServiceUnavail)
	}
}

func TestFromError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if FromError(nil) != nil {
			t.Error("FromError(nil) должен вернуть nil")
		}
	})

	t.Run("fault в цепочке", func(t *testing.T) {
		inner := ErrForbidden.New("нет доступа")
		wrapped := ErrInternal.Wrap(inner, "внешний слой")
		if got := FromError(wrapped).Code(); got != ErrInternal {
			t.Errorf("должен вернуться ближайший Fault: got %q", got)
		}
	})

	t.Run("обычная ошибка -> UNHANDLED", func(t *testing.T) {
		f := FromError(errors.New("boom"))
		if f.Code() != ErrUnhandled {
			t.Errorf("Code() = %q, want %q", f.Code(), ErrUnhandled)
		}
		if f.Message() != "internal server error" {
			t.Errorf("Message() = %q", f.Message())
		}
	})

	t.Run("context.DeadlineExceeded -> TIMEOUT", func(t *testing.T) {
		if got := FromError(context.DeadlineExceeded).Code(); got != ErrTimeout {
			t.Errorf("Code() = %q, want %q", got, ErrTimeout)
		}
	})

	t.Run("context.Canceled -> CANCELED", func(t *testing.T) {
		if got := FromError(context.Canceled).Code(); got != ErrCanceled {
			t.Errorf("Code() = %q, want %q", got, ErrCanceled)
		}
	})

	t.Run("context важнее оборачивающего fault", func(t *testing.T) {
		err := ErrInternal.Wrap(context.DeadlineExceeded, "db query")
		if got := FromError(err).Code(); got != ErrTimeout {
			t.Errorf("Code() = %q, want %q", got, ErrTimeout)
		}
	})
}

func TestHTTPStatus(t *testing.T) {
	tests := []struct {
		code Code
		want int
	}{
		{ErrValidation, http.StatusBadRequest},
		{ErrNotFound, http.StatusNotFound},
		{ErrUnauthorized, http.StatusUnauthorized},
		{ErrForbidden, http.StatusForbidden},
		{ErrConflict, http.StatusConflict},
		{ErrRateLimited, http.StatusTooManyRequests},
		{ErrTimeout, http.StatusGatewayTimeout},
		{ErrCanceled, http.StatusRequestTimeout},
		{ErrServiceUnavail, http.StatusServiceUnavailable},
		{Code("НЕИЗВЕСТНЫЙ"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		if got := tt.code.Err().HTTPStatus(); got != tt.want {
			t.Errorf("%s: HTTPStatus() = %d, want %d", tt.code, got, tt.want)
		}
	}
}

func TestGRPCCode(t *testing.T) {
	tests := []struct {
		code Code
		want codes.Code
	}{
		{ErrNotFound, codes.NotFound},
		{ErrUnauthorized, codes.Unauthenticated},
		{ErrForbidden, codes.PermissionDenied},
		{ErrRateLimited, codes.ResourceExhausted},
		{ErrTimeout, codes.DeadlineExceeded},
		{ErrCanceled, codes.Canceled},
		{ErrServiceUnavail, codes.Unavailable},
		{Code("НЕИЗВЕСТНЫЙ"), codes.Internal},
	}
	for _, tt := range tests {
		if got := tt.code.Err().GRPCCode(); got != tt.want {
			t.Errorf("%s: GRPCCode() = %v, want %v", tt.code, got, tt.want)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	if !IsRetryable(ErrServiceUnavail.Err()) {
		t.Error("SERVICE_UNAVAILABLE должен быть retryable")
	}
	if !IsRetryable(ErrTimeout.Err()) {
		t.Error("TIMEOUT должен быть retryable")
	}
	if !IsRetryable(ErrRateLimited.Err()) {
		t.Error("RATE_LIMITED должен быть retryable")
	}
	if IsRetryable(ErrNotFound.Err()) {
		t.Error("NOT_FOUND не должен быть retryable")
	}
	if IsRetryable(ErrCanceled.Err()) {
		t.Error("CANCELED не должен быть retryable")
	}
	if IsRetryable(nil) {
		t.Error("nil не должен быть retryable")
	}
}

func TestWriteHTTPError(t *testing.T) {
	t.Run("fault с args", func(t *testing.T) {
		rec := httptest.NewRecorder()
		err := ErrNotFound.New("event not found").WithArg("event_id", "01J000")

		WriteHTTPError(context.Background(), rec, err)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}

		var resp ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("невалидный JSON: %v", err)
		}
		if resp.Error.Code != "NOT_FOUND" {
			t.Errorf("code = %q", resp.Error.Code)
		}
		if resp.Error.Message != "event not found" {
			t.Errorf("message = %q", resp.Error.Message)
		}
		if resp.Error.Args["event_id"] != "01J000" {
			t.Errorf("args = %v", resp.Error.Args)
		}
	})

	t.Run("внутренности не утекают", func(t *testing.T) {
		rec := httptest.NewRecorder()
		err := ErrInternal.Wrap(errors.New("pq: duplicate key value"), "не удалось сохранить событие").
			WithOp("eventstore.insert")

		WriteHTTPError(context.Background(), rec, err)

		body := rec.Body.String()
		if strings.Contains(body, "duplicate key") || strings.Contains(body, "eventstore.insert") {
			t.Errorf("op/cause утекли в ответ клиенту: %s", body)
		}
	})

	t.Run("обычная ошибка -> 500 без деталей", func(t *testing.T) {
		rec := httptest.NewRecorder()
		WriteHTTPError(context.Background(), rec, errors.New("секретная внутренняя ошибка"))

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d", rec.Code)
		}
		if strings.Contains(rec.Body.String(), "секретная") {
			t.Errorf("текст сырой ошибки утёк клиенту: %s", rec.Body.String())
		}
	})

	t.Run("args опускаются, если пусты", func(t *testing.T) {
		rec := httptest.NewRecorder()
		WriteHTTPError(context.Background(), rec, ErrBadRequest.New("bad"))
		if strings.Contains(rec.Body.String(), "args") {
			t.Errorf("пустые args не должны сериализоваться: %s", rec.Body.String())
		}
	})
}

func TestRenderCLI(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	err := ErrServiceUnavail.
		Wrap(errors.New("dial tcp 10.0.1.5:9092: connection refused"), "не удалось подключиться к Kafka").
		WithOp("agent.kafka.subscribe").
		WithHint("проверьте --brokers и что порт 9092 доступен с этой машины")

	out := RenderCLI(err)

	for _, want := range []string{
		"✗ не удалось подключиться к Kafka (SERVICE_UNAVAILABLE)",
		"операция:",
		"agent.kafka.subscribe",
		"причина:",
		"connection refused",
		"подсказка:",
		"проверьте --brokers",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("в выводе нет %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b[") {
		t.Error("при NO_COLOR не должно быть ANSI-кодов")
	}

	if RenderCLI(nil) != "" {
		t.Error("RenderCLI(nil) должен вернуть пустую строку")
	}
}

func TestLogAttrs(t *testing.T) {
	err := ErrConflict.New("событие уже существует").
		WithOp("eventstore.insert").
		WithArg("event_id", "01J000")

	attrs := LogAttrs(err)

	found := map[string]bool{}
	for _, a := range attrs {
		found[a.Key] = true
	}
	for _, key := range []string{"error_code", "error", "message", "op", "args"} {
		if !found[key] {
			t.Errorf("в LogAttrs нет атрибута %q", key)
		}
	}

	if LogAttrs(nil) != nil {
		t.Error("LogAttrs(nil) должен вернуть nil")
	}
}

func TestCodeOf(t *testing.T) {
	if CodeOf(nil) != "" {
		t.Error("CodeOf(nil) должен вернуть пустой код")
	}
	if got := CodeOf(ErrForbidden.Err()); got != ErrForbidden {
		t.Errorf("CodeOf = %q", got)
	}
	if got := CodeOf(errors.New("x")); got != ErrUnhandled {
		t.Errorf("CodeOf для сырой ошибки = %q, want UNHANDLED_ERROR", got)
	}
}
