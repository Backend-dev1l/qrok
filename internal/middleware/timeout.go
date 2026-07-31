package middleware

import (
	"net/http"
	"time"

	chimw "github.com/go-chi/chi/v5/middleware"
)

// Timeout отменяет контекст запроса по истечении дедлайна.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	if d <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return chimw.Timeout(d)
}
