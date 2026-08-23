package httpapi

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"qrok/internal/config"
)

func TestDeviceApprovalRequiresBearerToken(t *testing.T) {
	router := NewRouter(config.HTTP{MaxBodyBytes: 1 << 20}, &Handler{log: slog.Default()}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/oauth/device/approve",
		strings.NewReader(`{"user_code":"ABCD-EFGH","project_id":"project-1"}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
