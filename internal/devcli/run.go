package devcli

import (
	"context"
	"log/slog"
	"time"

	"qrok/internal/config"
	"qrok/internal/devcli/credentials"
	httpsink "qrok/internal/devcli/sink/http"
	"qrok/internal/devcli/stream"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/tunnel"
	"qrok/pkg/fault"
)

const reconnectBackoff = 2 * time.Second

// Run запускает dev-клиент: ListenStream → HTTP Sink → DeliveryResult.
func Run(ctx context.Context, cfg *config.Listen, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	token, err := credentials.ResolveToken(cfg.Listen.Token)
	if err != nil {
		return err
	}

	sink := httpsink.New(cfg.Listen.Forward, 15*time.Second)

	client, err := stream.NewListenClient(ctx, cfg.Listen.Gateway)
	if err != nil {
		return err
	}
	defer client.Close()

	sub := &qrokv1.Subscribe{
		TunnelId: cfg.Listen.TunnelID,
		Topics:   cfg.Listen.Topics,
		CatchUp:  cfg.Listen.CatchUp,
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		err := client.Run(ctx, token, sub, func(ctx context.Context, ev *qrokv1.EventEnvelope, sendResult func(*qrokv1.DeliveryResult) error) error {
			result := sink.Deliver(ctx, ev)
			deliveryID := ev.GetHeaders()[tunnel.DeliveryIDHeader]
			if deliveryID == "" {
				deliveryID = ev.GetEventId() + "-live"
			}
			delivery := &qrokv1.DeliveryResult{
				EventId:    ev.GetEventId(),
				DeliveryId: deliveryID,
				StatusCode: result.StatusCode,
				Error:      result.Error,
				LatencyMs:  result.LatencyMS,
			}
			if err := sendResult(delivery); err != nil {
				return err
			}

			if result.Error != "" {
				log.Warn("доставка на localhost не удалась",
					"event_id", ev.GetEventId(),
					"status", result.StatusCode,
					"error", result.Error,
					"latency_ms", result.LatencyMS)
			} else {
				log.Info("событие доставлено на localhost",
					"event_id", ev.GetEventId(),
					"topic", ev.GetTopic(),
					"status", result.StatusCode,
					"latency_ms", result.LatencyMS)
			}
			return nil
		})

		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !fault.IsRetryable(err) {
			return err
		}
		log.Warn("ListenStream прерван, реконнект", "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectBackoff):
		}
	}
}
