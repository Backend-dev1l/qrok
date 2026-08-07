package middleware

import (
	"net/http"

	"qrok/pkg/fault"
)

// BodyLimit ограничивает размер тела запроса (защита от body-bomb).
// Проверяет Content-Length до чтения и оборачивает body в MaxBytesReader.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	if maxBytes <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > maxBytes {
				fault.WriteHTTPError(r.Context(), w, fault.ErrValidation.
					Newf("request body exceeds limit of %d bytes", maxBytes).
					WithOp("middleware.body_limit"))
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}
