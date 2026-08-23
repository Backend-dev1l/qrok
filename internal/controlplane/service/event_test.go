package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

func TestEventService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	allowAll := &models.Subject{AllowAll: true}
	scoped := &models.Subject{ProjectID: "prj-1"}

	t.Run("list_events_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-1"] = &models.Event{ID: "ev-1", TunnelID: "tunnel-1", Topic: "orders"}

		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		items, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 10)

		require.NoError(t, err)
		require.Len(t, items, 1)
		assert.Equal(t, "ev-1", items[0].Event.ID)
	})

	t.Run("list_events_forbidden", func(t *testing.T) {
		t.Parallel()

		authRepo := newFakeAuthRepo()
		authRepo.tunnelProject["tunnel-1"] = "other-prj"

		svc := NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, authRepo)
		_, err := svc.ListEvents(ctx, scoped, "tunnel-1", 10)

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("list_events_default_limit", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		for i := range 60 {
			id := fmt.Sprintf("ev-%02d", i)
			repo.events[id] = &models.Event{ID: id, TunnelID: "tunnel-1", Topic: "t"}
		}

		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		events, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 0)

		require.NoError(t, err)
		assert.Len(t, events, 50)
	})

	t.Run("get_event_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-1"] = &models.Event{
			ID:       "ev-1",
			TunnelID: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte(`{"ok":true}`),
		}

		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
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

		svc := NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, authRepo)
		_, err := svc.GetEvent(ctx, scoped, "ev-1")

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("get_event_not_found", func(t *testing.T) {
		t.Parallel()

		svc := NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.GetEvent(ctx, allowAll, "missing")

		require.Error(t, err)
		assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
	})

	t.Run("ingest_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())

		inserted, err := svc.Ingest(ctx, allowAll, &models.Event{
			ID:       "ev-new",
			TunnelID: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte("data"),
		})

		require.NoError(t, err)
		assert.True(t, inserted)
		assert.NotZero(t, repo.events["ev-new"].CreatedAt)
	})

	t.Run("ingest_duplicate", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		repo.events["ev-dup"] = &models.Event{
			ID: "ev-dup", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("x"),
		}

		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		inserted, err := svc.Ingest(ctx, allowAll, &models.Event{
			ID: "ev-dup", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("y"),
		})

		require.NoError(t, err)
		assert.False(t, inserted)
	})

	t.Run("ingest_forbidden_wrong_tunnel", func(t *testing.T) {
		t.Parallel()

		svc := NewEventService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo())
		_, err := svc.Ingest(ctx, &models.Subject{
			TunnelID:  "tunnel-a",
			ProjectID: "prj-1",
		}, &models.Event{
			ID: "ev-1", TunnelID: "tunnel-b", Topic: "t", Payload: []byte("x"),
		})

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("get_event_includes_deliveries", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &models.Event{
			ID: "ev-1", TunnelID: "tunnel-1", Topic: "orders", Payload: []byte("x"),
		}
		deliveries := &fakeDeliveryRepo{}
		deliveries.upserted = []*models.Delivery{{
			ID: "del-1", EventID: "ev-1", Kind: models.DeliveryKindLive,
			Status: models.DeliveryStatusFailed, Error: "connection refused",
		}}

		svc := NewEventService(events, deliveries, newFakeAuthRepo())
		view, err := svc.GetEvent(ctx, allowAll, "ev-1")

		require.NoError(t, err)
		require.Len(t, view.Deliveries, 1)
		assert.Equal(t, models.DeliveryStatusFailed, view.Deliveries[0].Status)
	})

	t.Run("list_events_includes_latest_delivery", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &models.Event{ID: "ev-1", TunnelID: "tunnel-1", Topic: "t"}
		deliveries := &fakeDeliveryRepo{}
		deliveries.upserted = []*models.Delivery{{
			ID: "del-1", EventID: "ev-1", Kind: models.DeliveryKindLive, Status: models.DeliveryStatusDelivered,
		}}

		svc := NewEventService(events, deliveries, newFakeAuthRepo())
		items, err := svc.ListEvents(ctx, allowAll, "tunnel-1", 10)

		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NotNil(t, items[0].LatestDelivery)
		assert.Equal(t, models.DeliveryStatusDelivered, items[0].LatestDelivery.Status)
	})

	t.Run("ingest_sets_created_at", func(t *testing.T) {
		t.Parallel()

		repo := newFakeEventRepo()
		svc := NewEventService(repo, &fakeDeliveryRepo{}, newFakeAuthRepo())
		before := time.Now().UTC()

		_, err := svc.Ingest(ctx, allowAll, &models.Event{
			ID: "ev-ts", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("x"),
		})

		require.NoError(t, err)
		assert.False(t, repo.events["ev-ts"].CreatedAt.Before(before))
	})
}
