package middleware

import (
	"log/slog"
	"net/http"

	"qrok/pkg/fault"
)

// Recoverer перехватывает panic в HTTP-хендлерах и отдаёт fault.ErrInternal.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic in HTTP handler", "panic", rec, "path", r.URL.Path)
					fault.WriteHTTPError(r.Context(), w, fault.ErrInternal.New("internal server error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
