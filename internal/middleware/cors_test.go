package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestCORS(t *testing.T) {
	t.Parallel()

	origins := []string{"http://localhost:5173"}

	t.Run("allowed_origin", func(t *testing.T) {
		t.Parallel()

		mw := middleware.CORS(origins)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Origin", "http://localhost:5173")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "http://localhost:5173", rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "true", rec.Header().Get("Access-Control-Allow-Credentials"))
	})

	t.Run("disallowed_origin", func(t *testing.T) {
		t.Parallel()

		mw := middleware.CORS(origins)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Origin", "http://evil.example")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("preflight", func(t *testing.T) {
		t.Parallel()

		mw := middleware.CORS(origins)
		called := false
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodOptions, "/api/v1/events", nil)
		req.Header.Set("Origin", "http://localhost:5173")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.False(t, called)
		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Methods"))
	})

	t.Run("disabled_when_empty_origins", func(t *testing.T) {
		t.Parallel()

		mw := middleware.CORS(nil)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Origin", "http://localhost:5173")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})
}
