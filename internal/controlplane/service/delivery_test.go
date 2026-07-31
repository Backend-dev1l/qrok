package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/model"
	"qrok/internal/controlplane/service"
	"qrok/pkg/fault"
)

func TestDeliveryService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("record_result_delivered", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := service.NewDeliveryService(repo)

		err := svc.RecordResult(ctx, &model.DeliveryResult{
			DeliveryID: "del-1",
			EventID:    "ev-1",
			StatusCode: 200,
			LatencyMS:  42,
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		rec := repo.upserted[0]
		assert.Equal(t, model.DeliveryStatusDelivered, rec.Status)
		assert.Equal(t, model.DeliveryKindLive, rec.Kind)
		require.NotNil(t, rec.StatusCode)
		assert.Equal(t, int32(200), *rec.StatusCode)
		require.NotNil(t, rec.LatencyMS)
		assert.Equal(t, int32(42), *rec.LatencyMS)
	})

	t.Run("record_result_failed_status_code", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := service.NewDeliveryService(repo)

		err := svc.RecordResult(ctx, &model.DeliveryResult{
			DeliveryID: "del-2",
			EventID:    "ev-1",
			StatusCode: 500,
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		assert.Equal(t, model.DeliveryStatusFailed, repo.upserted[0].Status)
	})

	t.Run("record_result_failed_error_message", func(t *testing.T) {
		t.Parallel()

		repo := &fakeDeliveryRepo{}
		svc := service.NewDeliveryService(repo)

		err := svc.RecordResult(ctx, &model.DeliveryResult{
			DeliveryID: "del-3",
			EventID:    "ev-1",
			StatusCode: 200,
			Error:      "connection reset",
		})

		require.NoError(t, err)
		require.Len(t, repo.upserted, 1)
		rec := repo.upserted[0]
		assert.Equal(t, model.DeliveryStatusFailed, rec.Status)
		assert.Equal(t, "connection reset", rec.Error)
	})

	t.Run("record_result_validation", func(t *testing.T) {
		t.Parallel()

		svc := service.NewDeliveryService(&fakeDeliveryRepo{})
		err := svc.RecordResult(ctx, nil)

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})
}
