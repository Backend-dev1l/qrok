package middleware

import (
	"context"
	"net/http"
	"strings"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

type authService interface {
	AuthenticateAPI(ctx context.Context, plaintext string) (*models.Subject, error)
	InsecureSubject() *models.Subject
}

// AuthConfig configures API authentication middleware.
type AuthConfig struct {
	Auth          authService
	AllowInsecure bool
}

// Auth validates Bearer tokens and stores the subject in request context.
func Auth(cfg AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.AllowInsecure {
				if cfg.Auth != nil {
					next.ServeHTTP(w, r.WithContext(WithSubject(r.Context(), cfg.Auth.InsecureSubject())))
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			token, err := bearerToken(r)
			if err != nil {
				fault.WriteHTTPError(r.Context(), w, err)
				return
			}
			if cfg.Auth == nil {
				fault.WriteHTTPError(r.Context(), w, fault.ErrInternal.New("auth is not configured").WithOp("middleware.auth"))
				return
			}

			subject, err := cfg.Auth.AuthenticateAPI(r.Context(), token)
			if err != nil {
				fault.WriteHTTPError(r.Context(), w, err)
				return
			}

			next.ServeHTTP(w, r.WithContext(WithSubject(r.Context(), subject)))
		})
	}
}

func bearerToken(r *http.Request) (string, error) {
	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if raw == "" {
		return "", fault.ErrUnauthorized.
			New("missing Authorization header").
			WithOp("middleware.auth").
			WithHint("set header Authorization: Bearer <token>")
	}
	const prefix = "bearer "
	if len(raw) < len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", fault.ErrUnauthorized.New("expected Bearer token").WithOp("middleware.auth")
	}
	token := strings.TrimSpace(raw[len(prefix):])
	if token == "" {
		return "", fault.ErrUnauthorized.New("empty Bearer token").WithOp("middleware.auth")
	}
	return token, nil
}
