package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDashboardHandler_indexNoRedirectLoop(t *testing.T) {
	t.Parallel()

	h := dashboardHandler()

	for _, path := range []string{"/", "/index.html"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.NotEqual(t, http.StatusMovedPermanently, rec.Code, "unexpected redirect to %q", rec.Header().Get("Location"))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Body.String(), "qrok — лента событий")
		})
	}
}

func TestDashboardHandler_devicePage(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/device", nil)
	rec := httptest.NewRecorder()
	dashboardHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "подтверждение входа")
}
