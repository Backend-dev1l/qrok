package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

func TestDeliveryService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	insecure := &models.Subject{AllowAll: true}

	t.Run("record_result_delivered", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := NewDeliveryService(repo, nil)

		err := svc.RecordResult(ctx, insecure, &models.DeliveryResult{
			DeliveryID: "del-1",
			EventID:    "ev-1",
			StatusCode: 200,
			LatencyMS:  42,
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		rec := repo.upserted[0]
		assert.Equal(t, models.DeliveryStatusDelivered, rec.Status)
		assert.Equal(t, models.DeliveryKindLive, rec.Kind)
		require.NotNil(t, rec.StatusCode)
		assert.Equal(t, int32(200), *rec.StatusCode)
		require.NotNil(t, rec.LatencyMS)
		assert.Equal(t, int32(42), *rec.LatencyMS)
	})

	t.Run("record_result_failed_status_code", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := NewDeliveryService(repo, nil)

		err := svc.RecordResult(ctx, insecure, &models.DeliveryResult{
			DeliveryID: "del-2",
			EventID:    "ev-1",
			StatusCode: 500,
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		assert.Equal(t, models.DeliveryStatusFailed, repo.upserted[0].Status)
	})

	t.Run("record_result_failed_error_message", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := NewDeliveryService(repo, nil)

		err := svc.RecordResult(ctx, insecure, &models.DeliveryResult{
			DeliveryID: "del-3",
			EventID:    "ev-1",
			StatusCode: 200,
			Error:      "connection reset",
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		rec := repo.upserted[0]
		assert.Equal(t, models.DeliveryStatusFailed, rec.Status)
		assert.Equal(t, "connection reset", rec.Error)
	})

	t.Run("rejects_result_for_another_project", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		scope := newFakeAuthRepo()
		scope.eventProject["ev-1"] = "project-a"
		svc := NewDeliveryService(repo, scope)

		err := svc.RecordResult(ctx, &models.Subject{ProjectID: "project-b"}, &models.DeliveryResult{
			DeliveryID: "del-1",
			EventID:    "ev-1",
			StatusCode: 200,
		})

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.CodeOf(err))
		assert.Empty(t, repo.upserted)
	})

}
