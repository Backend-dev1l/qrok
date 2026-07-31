package middleware_test

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestSecurityHeaders(t *testing.T) {
	t.Parallel()

	mw := middleware.SecurityHeaders()

	t.Run("sets_headers_without_tls", func(t *testing.T) {
		t.Parallel()

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
		assert.Equal(t, "strict-origin-when-cross-origin", rec.Header().Get("Referrer-Policy"))
		assert.NotEmpty(t, rec.Header().Get("Content-Security-Policy"))
		assert.Empty(t, rec.Header().Get("Strict-Transport-Security"))
	})

	t.Run("sets_hsts_with_tls", func(t *testing.T) {
		t.Parallel()

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.TLS = &tls.ConnectionState{}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.NotEmpty(t, rec.Header().Get("Strict-Transport-Security"))
	})
}
