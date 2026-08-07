package middleware

import (
	"context"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

type ctxKey int

const principalKey ctxKey = iota

// WithSubject stores the authenticated subject in the request context.
func WithSubject(ctx context.Context, subject *models.Subject) context.Context {
	return context.WithValue(ctx, principalKey, subject)
}

// SubjectFromContext returns the subject set by Auth middleware.
func SubjectFromContext(ctx context.Context) (*models.Subject, bool) {
	subject, ok := ctx.Value(principalKey).(*models.Subject)
	return subject, ok && subject != nil
}

// RequireSubject is a helper for handlers that require authentication.
func RequireSubject(ctx context.Context) (*models.Subject, error) {
	subject, ok := SubjectFromContext(ctx)
	if !ok {
		return nil, fault.ErrUnauthorized.New("authorization required").WithOp("middleware.auth")
	}
	return subject, nil
}
