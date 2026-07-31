package middleware_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/middleware"
)

func TestRequestLogger(t *testing.T) {
	t.Run("logs_request", func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		mw := middleware.RequestLogger(log)

		handler := middleware.RequestID(mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
		})))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", http.NoBody)
		req.Header.Set("X-Request-Id", "log-req-1")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusCreated, rec.Code)
		out := buf.String()
		assert.Contains(t, out, "http request")
		assert.Contains(t, out, "/api/v1/events")
		assert.Contains(t, out, "log-req-1")
	})

	t.Run("nil_logger_uses_default", func(t *testing.T) {
		mw := middleware.RequestLogger(nil)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		require.NotPanics(t, func() {
			handler.ServeHTTP(rec, req)
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("slow_request_logs_warn", func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		mw := middleware.RequestLoggerWithThreshold(log, 10*time.Millisecond)

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(20 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/slow", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Contains(t, buf.String(), "slow http request")
	})
}
