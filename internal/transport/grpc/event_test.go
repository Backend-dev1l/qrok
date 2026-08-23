package gateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	qrokv1 "qrok/internal/proto/qrok/v1"
)

func TestValidateEventEnvelope(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		err := validateEventEnvelope("test", &qrokv1.EventEnvelope{
			EventId:  "01ABC",
			TunnelId: "tunnel-1",
			Topic:    "orders",
			Payload:  []byte(`{"ok":true}`),
		})
		require.NoError(t, err)
	})

	t.Run("rejects_payload_ref", func(t *testing.T) {
		t.Parallel()
		err := validateEventEnvelope("test", &qrokv1.EventEnvelope{
			EventId:    "01ABC",
			TunnelId:   "tunnel-1",
			Topic:      "orders",
			PayloadRef: "s3/key",
		})
		require.Error(t, err)
	})

	t.Run("accepts_empty_kafka_payload", func(t *testing.T) {
		t.Parallel()
		err := validateEventEnvelope("test", &qrokv1.EventEnvelope{
			EventId:  "01ABC",
			TunnelId: "tunnel-1",
			Topic:    "orders",
		})
		require.NoError(t, err)
	})
}

func TestEventFromProto(t *testing.T) {
	t.Parallel()

	ev, err := eventFromProto(&qrokv1.EventEnvelope{
		EventId:    "01ABC",
		TunnelId:   "tunnel-1",
		Topic:      "orders",
		Partition:  2,
		Offset:     42,
		Key:        []byte("k"),
		Headers:    map[string]string{"trace": "1"},
		Payload:    []byte(`{"ok":true}`),
		BrokerTsMs: 1_700_000_000_000,
	})
	require.NoError(t, err)
	assert.Equal(t, "01ABC", ev.ID)
	assert.Equal(t, "tunnel-1", ev.TunnelID)
	assert.NotNil(t, ev.BrokerTS)
}
