package middleware

import (
	"net/http"

	chimw "github.com/go-chi/chi/v5/middleware"
)

// RealIP подставляет клиентский IP из X-Forwarded-For / X-Real-IP за LB.
func RealIP(next http.Handler) http.Handler {
	return chimw.RealIP(next)
}
