package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/tunnel"
	"qrok/pkg/fault"
)

func TestReplayService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	allowAll := &models.Subject{AllowAll: true}
	scoped := &models.Subject{ProjectID: "prj-1"}

	t.Run("replay_success", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &models.Event{
			ID:       "ev-1",
			TunnelID: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte(`{"ok":true}`),
		}
		deliveries := &fakeDeliveryRepo{}
		bus := inproc.New(16)

		subCh, unsub, err := bus.Subscribe(ctx)
		require.NoError(t, err)
		defer unsub()

		svc := NewReplayService(events, deliveries, newFakeAuthRepo(), bus)
		result, err := svc.Replay(ctx, allowAll, "ev-1", "target-1")

		require.NoError(t, err)
		assert.Equal(t, "ev-1", result.EventID)
		assert.Equal(t, "target-1", result.TargetID)
		assert.Equal(t, string(models.DeliveryStatusPending), result.Status)
		assert.NotEmpty(t, result.DeliveryID)

		require.Len(t, deliveries.pending, 1)
		assert.Equal(t, models.DeliveryKindReplay, deliveries.pending[0].Kind)

		select {
		case env := <-subCh:
			assert.True(t, env.GetIsReplay())
			assert.Equal(t, result.DeliveryID, env.GetHeaders()[tunnel.DeliveryIDHeader])
		default:
			t.Fatal("expected replay envelope on bus")
		}
	})

	t.Run("replay_default_target", func(t *testing.T) {
		t.Parallel()

		events := newFakeEventRepo()
		events.events["ev-1"] = &models.Event{
			ID: "ev-1", TunnelID: "tunnel-1", Topic: "t", Payload: []byte("x"),
		}

		svc := NewReplayService(events, &fakeDeliveryRepo{}, newFakeAuthRepo(), inproc.New(4))
		result, err := svc.Replay(ctx, allowAll, "ev-1", "")

		require.NoError(t, err)
		assert.Equal(t, "*", result.TargetID)
	})

	t.Run("replay_forbidden", func(t *testing.T) {
		t.Parallel()

		authRepo := newFakeAuthRepo()
		authRepo.eventProject["ev-1"] = "other-prj"

		svc := NewReplayService(newFakeEventRepo(), &fakeDeliveryRepo{}, authRepo, inproc.New(4))
		_, err := svc.Replay(ctx, scoped, "ev-1", "*")

		require.Error(t, err)
		assert.Equal(t, fault.ErrForbidden, fault.FromError(err).Code())
	})

	t.Run("replay_not_found", func(t *testing.T) {
		t.Parallel()

		svc := NewReplayService(newFakeEventRepo(), &fakeDeliveryRepo{}, newFakeAuthRepo(), inproc.New(4))
		_, err := svc.Replay(ctx, allowAll, "missing", "*")

		require.Error(t, err)
		assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
	})

}

func TestToReplayEnvelope(t *testing.T) {
	t.Parallel()

	partition := int32(1)
	offset := int64(42)
	ev := &models.Event{
		ID:           "01EVENT",
		TunnelID:     "tunnel-1",
		Topic:        "orders",
		Partition:    &partition,
		BrokerOffset: &offset,
		Headers:      map[string]string{"trace": "1"},
	}

	out := ToReplayEnvelope(ev, []byte(`{"ok":true}`), "01DELIVERY")
	if !out.GetIsReplay() {
		t.Fatal("expected is_replay=true")
	}
	if out.GetHeaders()[tunnel.DeliveryIDHeader] != "01DELIVERY" {
		t.Fatalf("delivery header = %q", out.GetHeaders()[tunnel.DeliveryIDHeader])
	}
	if string(out.GetPayload()) != `{"ok":true}` {
		t.Fatalf("payload = %q", out.GetPayload())
	}
}
