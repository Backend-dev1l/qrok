//go:build e2e

package e2e_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/config"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/devcli"
)

// Сквозной сценарий replay: событие в Postgres → replay API → ListenStream → HTTP localhost.
// Требует make compose-up && make migrate-up (Postgres + MinIO).
func TestReplayDeliverToLocalhost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, objects := connectStores(t, ctx)
	defer pool.Close()

	const (
		tunnelID = "e2e-tunnel"
		eventID  = "01E2EEVENT0000000000000000"
	)

	stack := startTestStack(t, ctx, pool, objects)

	payload := []byte(`{"e2e":true}`)
	_, err := stack.EventRepo.Insert(ctx, &models.Event{
		ID:       eventID,
		TunnelID: tunnelID,
		Topic:    "demo",
		Payload:  payload,
	})
	require.NoError(t, err)

	received := make(chan string, 1)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		assert.Equal(t, "true", r.Header.Get("X-Qrok-Replay"))
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	listenDone := make(chan error, 1)
	go func() {
		listenDone <- devcli.Run(ctx, &config.Listen{
			Listen: config.ListenConn{
				Gateway:  stack.GRPCAddr,
				TunnelID: tunnelID,
				Forward:  stub.URL,
			},
		}, nil)
	}()

	time.Sleep(200 * time.Millisecond)

	result, err := stack.Replay.Replay(ctx, &models.Subject{AllowAll: true}, eventID, "*")
	require.NoError(t, err)
	require.NotEmpty(t, result.DeliveryID)

	select {
	case body := <-received:
		assert.Equal(t, string(payload), body)
	case <-ctx.Done():
		t.Fatal("timeout waiting for localhost delivery")
	}

	require.Eventually(t, func() bool {
		rec, err := stack.Delivery.GetByID(ctx, result.DeliveryID)
		return err == nil && rec.Status == models.DeliveryStatusDelivered
	}, 3*time.Second, 100*time.Millisecond, "delivery status not delivered")

	cancel()
	<-listenDone
}
