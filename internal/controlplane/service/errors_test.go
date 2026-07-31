package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/service"
	"qrok/pkg/fault"
)

func TestMapRepoErrPreservesContext(t *testing.T) {
	t.Parallel()

	err := service.MapRepoErrForTest("test.op", context.Canceled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, fault.ErrCanceled, fault.FromError(err).Code())
}

func TestNotFoundErrMapsNoRows(t *testing.T) {
	t.Parallel()

	err := service.NotFoundErrForTest("test.op", "missing", pgx.ErrNoRows)
	require.Error(t, err)
	assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
}

func TestNotFoundErrMapsCorruptEvent(t *testing.T) {
	t.Parallel()

	err := service.NotFoundErrForTest("test.op", "missing", eventstore.ErrCorruptEvent)
	require.Error(t, err)
	assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
}

func TestUnauthorizedErrMapsNoRows(t *testing.T) {
	t.Parallel()

	err := service.UnauthorizedErrForTest("test.op", "bad token", pgx.ErrNoRows)
	require.Error(t, err)
	assert.Equal(t, fault.ErrUnauthorized, fault.FromError(err).Code())
}

func TestMapRepoErrUnknown(t *testing.T) {
	t.Parallel()

	err := service.MapRepoErrForTest("test.op", errors.New("boom"))
	require.Error(t, err)
	assert.Equal(t, fault.ErrInternal, fault.FromError(err).Code())
}
