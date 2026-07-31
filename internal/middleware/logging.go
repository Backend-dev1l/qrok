package middleware

import (
	"log/slog"
	"net/http"
	"time"

	chimw "github.com/go-chi/chi/v5/middleware"
)

// RequestLogger логирует каждый HTTP-запрос: метод, путь, статус, duration, request_id.
// Запросы дольше slowThreshold логируются с уровнем Warn.
func RequestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return RequestLoggerWithThreshold(log, 500*time.Millisecond)
}

// RequestLoggerWithThreshold позволяет задать порог slow-request в тестах.
func RequestLoggerWithThreshold(log *slog.Logger, slowThreshold time.Duration) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			duration := time.Since(start)
			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", duration.Milliseconds(),
				"request_id", GetRequestID(r.Context()),
			}
			if duration >= slowThreshold {
				log.Warn("slow http request", attrs...)
				return
			}
			log.Info("http request", attrs...)
		})
	}
}
