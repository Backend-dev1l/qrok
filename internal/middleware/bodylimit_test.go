package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestBodyLimit(t *testing.T) {
	t.Parallel()

	t.Run("within_limit", func(t *testing.T) {
		t.Parallel()

		mw := middleware.BodyLimit(64)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"ok":true}`))
		req.ContentLength = 11
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("content_length_exceeds_limit", func(t *testing.T) {
		t.Parallel()

		mw := middleware.BodyLimit(8)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("012345678901"))
		req.ContentLength = 11
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("disabled_when_zero", func(t *testing.T) {
		t.Parallel()

		mw := middleware.BodyLimit(0)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("012345678901"))
		req.ContentLength = 11
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
