package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/middleware"
)

func TestRequestID(t *testing.T) {
	t.Run("propagates_incoming_header", func(t *testing.T) {
		var got string
		handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = middleware.GetRequestID(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("X-Request-Id", "req-123")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "req-123", got)
	})

	t.Run("generates_when_missing", func(t *testing.T) {
		var got string
		handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = middleware.GetRequestID(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.NotEmpty(t, got)
	})
}
