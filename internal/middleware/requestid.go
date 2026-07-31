package middleware

import (
	"context"
	"net/http"

	chimw "github.com/go-chi/chi/v5/middleware"
)

// RequestID пробрасывает или генерирует X-Request-Id.
func RequestID(next http.Handler) http.Handler {
	return chimw.RequestID(next)
}

// GetRequestID возвращает request_id из контекста.
func GetRequestID(ctx context.Context) string {
	return chimw.GetReqID(ctx)
}
