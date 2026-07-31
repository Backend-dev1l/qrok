package fault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Args    map[string]string `json:"args,omitempty"`
}

// WriteHTTPError отдаёт ошибку клиенту в едином JSON-формате.
// Внутренности (op, cause) наружу не утекают — только code, message, args и trace_id.
func WriteHTTPError(ctx context.Context, w http.ResponseWriter, err error) {
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return
	}
	f := FromError(err)

	traceID := ""
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.HasTraceID() {
		traceID = spanCtx.TraceID().String()
	}

	response := ErrorResponse{
		Error: ErrorDetail{
			Code:    string(f.code),
			Message: f.Message(),
			TraceID: traceID,
			Args:    f.args,
		},
	}

	if len(response.Error.Args) == 0 {
		response.Error.Args = nil
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.HTTPStatus())
	_ = json.NewEncoder(w).Encode(response)
}
