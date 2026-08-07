package middleware_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/middleware"
	"qrok/pkg/fault"
)

func TestSubjectContext(t *testing.T) {
	t.Parallel()

	subject := &models.Subject{
		TokenID:   "tok-1",
		ProjectID: "prj-1",
	}

	t.Run("with_and_from_context", func(t *testing.T) {
		t.Parallel()

		ctx := middleware.WithSubject(context.Background(), subject)
		got, ok := middleware.SubjectFromContext(ctx)

		require.True(t, ok)
		assert.Equal(t, subject, got)
	})

	t.Run("missing_subject", func(t *testing.T) {
		t.Parallel()

		_, ok := middleware.SubjectFromContext(context.Background())
		assert.False(t, ok)
	})

	t.Run("require_subject_ok", func(t *testing.T) {
		t.Parallel()

		ctx := middleware.WithSubject(context.Background(), subject)
		got, err := middleware.RequireSubject(ctx)

		require.NoError(t, err)
		assert.Equal(t, subject, got)
	})

	t.Run("require_subject_missing", func(t *testing.T) {
		t.Parallel()

		_, err := middleware.RequireSubject(context.Background())
		require.Error(t, err)
		assert.Equal(t, fault.ErrUnauthorized, fault.FromError(err).Code())
	})
}
