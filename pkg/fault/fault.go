// Package fault — единый пакет ошибок qrok для agent, dev-cli и server.
//
// Правила использования:
//   - service создаёт прикладную ошибку и оборачивает исходную причину один раз;
//   - transport создаёт ошибки только для нарушений своего протокола;
//   - message пишется на границе, где понятен пользовательский контекст;
//   - маппинг в HTTP/gRPC-статусы живёт только здесь — хендлеры про статусы не знают;
//   - в HTTP-хендлерах ошибки всегда уходят через WriteHTTPError,
//     в CLI — через RenderCLI, в логи — через LogAttrs.
package fault

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
)

type Code string

const (
	ErrValidation     Code = "VALIDATION_ERROR"
	ErrNotFound       Code = "NOT_FOUND"
	ErrBadRequest     Code = "BAD_REQUEST"
	ErrUnauthorized   Code = "UNAUTHORIZED"
	ErrForbidden      Code = "FORBIDDEN"
	ErrConflict       Code = "CONFLICT"
	ErrUnprocessable  Code = "UNPROCESSABLE_ENTITY"
	ErrRateLimited    Code = "RATE_LIMITED"
	ErrTimeout        Code = "TIMEOUT"
	ErrCanceled       Code = "CANCELED"
	ErrInternal       Code = "INTERNAL_ERROR"
	ErrServiceUnavail Code = "SERVICE_UNAVAILABLE"
	ErrUnhandled      Code = "UNHANDLED_ERROR"
)

var _httpStatusMap = map[Code]int{
	ErrValidation:     http.StatusBadRequest,
	ErrNotFound:       http.StatusNotFound,
	ErrBadRequest:     http.StatusBadRequest,
	ErrUnauthorized:   http.StatusUnauthorized,
	ErrForbidden:      http.StatusForbidden,
	ErrConflict:       http.StatusConflict,
	ErrUnprocessable:  http.StatusUnprocessableEntity,
	ErrRateLimited:    http.StatusTooManyRequests,
	ErrTimeout:        http.StatusGatewayTimeout,
	ErrCanceled:       http.StatusRequestTimeout,
	ErrInternal:       http.StatusInternalServerError,
	ErrServiceUnavail: http.StatusServiceUnavailable,
	ErrUnhandled:      http.StatusInternalServerError,
}

var _grpcCodeMap = map[Code]codes.Code{
	ErrValidation:     codes.InvalidArgument,
	ErrNotFound:       codes.NotFound,
	ErrBadRequest:     codes.InvalidArgument,
	ErrUnauthorized:   codes.Unauthenticated,
	ErrForbidden:      codes.PermissionDenied,
	ErrConflict:       codes.AlreadyExists,
	ErrUnprocessable:  codes.FailedPrecondition,
	ErrRateLimited:    codes.ResourceExhausted,
	ErrTimeout:        codes.DeadlineExceeded,
	ErrCanceled:       codes.Canceled,
	ErrInternal:       codes.Internal,
	ErrServiceUnavail: codes.Unavailable,
	ErrUnhandled:      codes.Internal,
}

// Коды, при которых вызывающая сторона может повторить операцию (backoff у агента/CLI).
var _retryable = map[Code]bool{
	ErrServiceUnavail: true,
	ErrTimeout:        true,
	ErrRateLimited:    true,
}

type Fault struct {
	code    Code
	message string
	op      string
	hint    string
	args    map[string]string
	cause   error
}

func (f *Fault) Error() string {
	msg := f.message
	if msg == "" {
		msg = string(f.code)
	}
	if f.op != "" {
		msg = f.op + ": " + msg
	}
	if f.cause != nil {
		msg = msg + ": " + f.cause.Error()
	}
	return msg
}

func (f *Fault) Unwrap() error { return f.cause }

func (f *Fault) Code() Code { return f.code }

func (f *Fault) Message() string {
	if f.message == "" {
		return string(f.code)
	}
	return f.message
}

func (f *Fault) Op() string { return f.op }

func (f *Fault) Hint() string { return f.hint }

func (f *Fault) Args() map[string]string { return f.args }

func (f *Fault) HTTPStatus() int {
	if status, ok := _httpStatusMap[f.code]; ok {
		return status
	}
	return http.StatusInternalServerError
}

func (f *Fault) GRPCCode() codes.Code {
	if c, ok := _grpcCodeMap[f.code]; ok {
		return c
	}
	return codes.Internal
}

func (c Code) New(message string) *Fault {
	return &Fault{code: c, message: message, args: make(map[string]string)}
}

func (c Code) Newf(format string, a ...any) *Fault {
	return &Fault{code: c, message: fmt.Sprintf(format, a...), args: make(map[string]string)}
}

func (c Code) Err() *Fault {
	return &Fault{code: c, message: string(c), args: make(map[string]string)}
}

// Wrap оборачивает причину, сохраняя цепочку для errors.Is/As.
func (c Code) Wrap(err error, message string) *Fault {
	return &Fault{code: c, message: message, cause: err, args: make(map[string]string)}
}

func (c Code) Wrapf(err error, format string, a ...any) *Fault {
	return &Fault{code: c, message: fmt.Sprintf(format, a...), cause: err, args: make(map[string]string)}
}

func (f *Fault) WithArg(key, value string) *Fault {
	if f.args == nil {
		f.args = make(map[string]string)
	}
	f.args[key] = value
	return f
}

// WithOp помечает операцию, в которой возникла ошибка, например "agent.kafka.subscribe".
func (f *Fault) WithOp(op string) *Fault {
	f.op = op
	return f
}

// WithHint добавляет подсказку пользователю CLI, что делать с ошибкой.
func (f *Fault) WithHint(hint string) *Fault {
	f.hint = hint
	return f
}

func IsFault(err error) bool {
	var f *Fault
	return errors.As(err, &f)
}

// FromError сначала распознаёт отмену и deadline, затем достаёт ближайший
// *Fault из цепочки. Всё остальное неизвестное становится UNHANDLED_ERROR.
func FromError(err error) *Fault {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return ErrCanceled.Wrap(err, "operation canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout.Wrap(err, "operation timed out")
	}
	var f *Fault
	if errors.As(err, &f) {
		return f
	}
	return ErrUnhandled.Wrap(err, "internal server error")
}

// CodeOf возвращает код ошибки; для nil — пустой Code.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	return FromError(err).code
}

// IsRetryable сообщает, имеет ли смысл повторить операцию (используется backoff-логикой).
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	return _retryable[FromError(err).code]
}

// LogAttrs возвращает структурированные атрибуты ошибки для slog.
// Полная цепочка причин попадает в error, пользовательское сообщение — в message.
func LogAttrs(err error) []slog.Attr {
	f := FromError(err)
	if f == nil {
		return nil
	}
	attrs := []slog.Attr{
		slog.String("error_code", string(f.code)),
		slog.String("error", f.Error()),
		slog.String("message", f.Message()),
	}
	if f.op != "" {
		attrs = append(attrs, slog.String("op", f.op))
	}
	if len(f.args) > 0 {
		argAttrs := make([]any, 0, len(f.args))
		for k, v := range f.args {
			argAttrs = append(argAttrs, slog.String(k, v))
		}
		attrs = append(attrs, slog.Group("args", argAttrs...))
	}
	return attrs
}
