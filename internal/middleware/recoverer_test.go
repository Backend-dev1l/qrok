package middleware_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/middleware"
)

func TestRecoverer(t *testing.T) {
	t.Parallel()

	mw := middleware.Recoverer(slog.Default())

	t.Run("no_panic", func(t *testing.T) {
		t.Parallel()

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("panic_string", func(t *testing.T) {
		t.Parallel()

		handler := mw(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			panic("something went wrong")
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		var resp map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		_, hasError := resp["error"]
		assert.True(t, hasError)
	})

	t.Run("panic_error", func(t *testing.T) {
		t.Parallel()

		handler := mw(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			panic(assert.AnError)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestRecovererNilLogger(t *testing.T) {
	t.Parallel()

	mw := middleware.Recoverer(nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	rec := httptest.NewRecorder()
	require.NotPanics(t, func() {
		handler.ServeHTTP(rec, req)
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
