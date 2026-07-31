package middleware

import (
	"context"

	"qrok/internal/controlplane/model"
	"qrok/pkg/fault"
)

type ctxKey int

const principalKey ctxKey = iota

// WithSubject stores the authenticated subject in the request context.
func WithSubject(ctx context.Context, subject *model.Subject) context.Context {
	return context.WithValue(ctx, principalKey, subject)
}

// SubjectFromContext returns the subject set by Auth middleware.
func SubjectFromContext(ctx context.Context) (*model.Subject, bool) {
	subject, ok := ctx.Value(principalKey).(*model.Subject)
	return subject, ok && subject != nil
}

// RequireSubject is a helper for handlers that require authentication.
func RequireSubject(ctx context.Context) (*model.Subject, error) {
	subject, ok := SubjectFromContext(ctx)
	if !ok {
		return nil, fault.ErrUnauthorized.New("требуется авторизация").WithOp("middleware.auth")
	}
	return subject, nil
}
