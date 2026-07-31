package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestAuth(t *testing.T) {
	t.Parallel()

	t.Run("allow_insecure", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Auth(middleware.AuthConfig{AllowInsecure: true})
		called := false
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.True(t, called)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("missing_token", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Auth(middleware.AuthConfig{})
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("invalid_bearer_format", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Auth(middleware.AuthConfig{})
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Authorization", "Token abc")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("empty_bearer_token", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Auth(middleware.AuthConfig{})
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Authorization", "Bearer ")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("tokens_not_configured", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Auth(middleware.AuthConfig{Auth: nil})
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req.Header.Set("Authorization", "Bearer qrok_agt_test")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}
