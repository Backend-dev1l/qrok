package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"qrok/internal/middleware"
)

func TestTimeout(t *testing.T) {
	t.Parallel()

	t.Run("fast_request_passes", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Timeout(time.Second)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("slow_request_times_out", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Timeout(20 * time.Millisecond)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
				w.WriteHeader(http.StatusOK)
			}
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	})

	t.Run("disabled_when_zero", func(t *testing.T) {
		t.Parallel()

		mw := middleware.Timeout(0)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
