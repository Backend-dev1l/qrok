package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/model"
	"qrok/internal/controlplane/service"
	"qrok/pkg/fault"
)

func TestEventService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	allowAll := &model.Subject{AllowAll: true}
	scoped := &model.Subject{ProjectID: "prj-1"}

	t.Run("list_events_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-1"] = &model.Event{ID: "ev-1", TunnelID: "tunnel-1", Topic: "orders"}

		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		items, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 10)

		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, "ev-1", items[0].Event.ID)
	})

	t.Run("list_events_forbidden", func(t *testing.T) {
		t.Parallel()

		authRepo := newFakeAuthRepo()
		authRepo.tunnelProject["tunnel-1"] = "other-prj"

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, authRepo)
		_, err := svc.ListEvents(ctx, scoped, "tunnel-1", 10)

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("list_events_empty_tunnel", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.ListEvents(ctx, allowAll, "", 10)

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})

	t.Run("list_events_limit_too_high", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 500)

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})

	t.Run("list_events_default_limit", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		for i := range 60 {
			id := fmt.Sprintf("ev-%02d", i)
			repo.events[id] = &model.Event{ID: id, TunnelID: "tunnel-1", Topic: "t"}
		}

		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		events, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 0)

		require.NoError(t, err)
		assert.Len(t, events, 50)
	})

	t.Run("get_event_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-1"] = &model.Event{
			ID:       "ev-1",
			TunnelID: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte(`{"ok":true}`),
		}

		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		view, err := svc.GetEvent(ctx, allowAll, "ev-1")

		require.NoError(t, err)
		assert.Equal(t, "ev-1", view.Event.ID)
		assert.Equal(t, `{"ok":true}`, string(view.Payload))
		assert.True(t, view.HasFullPayload)
	})

	t.Run("get_event_forbidden", func(t *testing.T) {
		t.Parallel()

		authRepo := newFakeAuthRepo()
		authRepo.eventProject["ev-1"] = "other-prj"

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, authRepo)
		_, err := svc.GetEvent(ctx, scoped, "ev-1")

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("get_event_not_found", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.GetEvent(ctx, allowAll, "missing")

		require.Error(t, err)
		assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
	})

	t.Run("ingest_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())

		inserted, err := svc.Ingest(ctx, allowAll, &model.Event{
			ID:       "ev-new",
			TunnelID: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte("data"),
		}, 1024)

		require.NoError(t, err)
		assert.True(t, inserted)
		assert.NotZero(t, repo.events["ev-new"].CreatedAt)
	})

	t.Run("ingest_duplicate", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-dup"] = &model.Event{
			ID: "ev-dup", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("x"),
		}

		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		inserted, err := svc.Ingest(ctx, allowAll, &model.Event{
			ID: "ev-dup", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("y"),
		}, 1024)

		require.NoError(t, err)
		assert.False(t, inserted)
	})

	t.Run("ingest_empty_payload", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.Ingest(ctx, allowAll, &model.Event{
			ID: "ev-1", TunnelID: "tunnel-1", Topic: "t",
		}, 1024)

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})

	t.Run("ingest_payload_too_large", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.Ingest(ctx, allowAll, &model.Event{
			ID: "ev-1", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("big"),
		}, 1)

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})

	t.Run("ingest_forbidden_wrong_tunnel", func(t *testing.T) {
		t.Parallel()

		svc := service.NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.Ingest(ctx, &model.Subject{
			TunnelID:  "tunnel-a",
			ProjectID: "prj-1",
		}, &model.Event{
			ID: "ev-1", TunnelID: "tunnel-b", Topic: "t", Payload: []byte("x"),
		}, 1024)

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("get_event_includes_deliveries", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &model.Event{
			ID: "ev-1", TunnelID: "tunnel-1", Topic: "orders", Payload: []byte("x"),
		}
		deliveries := &fakeDeliveryRepo{}
		deliveries.upserted = []*model.Delivery{{
			ID: "del-1", EventID: "ev-1", Kind: model.DeliveryKindLive,
			Status: model.DeliveryStatusFailed, Error: "connection refused",
		}}

		svc := service.NewEventService(events, deliveries, newFakeAuthRepo())
		view, err := svc.GetEvent(ctx, allowAll, "ev-1")

		require.NoError(t, err)
		require.Len(t, view.Deliveries, 1)
		assert.Equal(t, model.DeliveryStatusFailed, view.Deliveries[0].Status)
	})

	t.Run("list_events_includes_latest_delivery", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &model.Event{ID: "ev-1", TunnelID: "tunnel-1", Topic: "t"}
		deliveries := &fakeDeliveryRepo{}
		deliveries.upserted = []*model.Delivery{{
			ID: "del-1", EventID: "ev-1", Kind: model.DeliveryKindLive, Status: model.DeliveryStatusDelivered,
		}}

		svc := service.NewEventService(events, deliveries, newFakeAuthRepo())
		items, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 10)

		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NotNil(t, items[0].LatestDelivery)
		assert.Equal(t, model.DeliveryStatusDelivered, items[0].LatestDelivery.Status)
	})

	t.Run("ingest_sets_created_at", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		svc := service.NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		before := time.Now().UTC()

		_, err := svc.Ingest(ctx, allowAll, &model.Event{
			ID: "ev-ts", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("x"),
		}, 0)

		require.NoError(t, err)
		assert.False(t, repo.events["ev-ts"].CreatedAt.Before(before))
	})
}
