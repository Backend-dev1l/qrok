package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestRealIP(t *testing.T) {
	t.Parallel()

	mw := middleware.RealIP

	t.Run("uses_x_forwarded_for", func(t *testing.T) {
		t.Parallel()

		var remoteAddr string
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			remoteAddr = r.RemoteAddr
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Forwarded-For", "203.0.113.10")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "203.0.113.10", remoteAddr)
	})

	t.Run("uses_x_real_ip", func(t *testing.T) {
		t.Parallel()

		var remoteAddr string
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			remoteAddr = r.RemoteAddr
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Real-IP", "198.51.100.20")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "198.51.100.20", remoteAddr)
	})

	t.Run("ignores_forwarding_headers_from_public_peer", func(t *testing.T) {
		t.Parallel()

		var remoteAddr string
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			remoteAddr = r.RemoteAddr
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "198.51.100.1:1234"
		req.Header.Set("X-Forwarded-For", "203.0.113.10")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, "198.51.100.1:1234", remoteAddr)
	})
}
