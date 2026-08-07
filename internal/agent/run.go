package agent

import (
	"context"
	"log/slog"
	"time"

	"qrok/internal/agent/source"
	"qrok/internal/agent/source/kafka"
	"qrok/internal/agent/stream"
	"qrok/internal/config"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/pkg/fault"
)

const (
	eventBuffer    = 64
	ackTimeout     = 30 * time.Second
	maxBackoff     = 30 * time.Second
	initialBackoff = time.Second
)

// Run запускает агента: Kafka → Gateway → ack→commit с реконнектом.
func Run(ctx context.Context, cfg *config.Agent, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	src, err := kafka.New(kafka.Config{
		Brokers:         cfg.Kafka.Brokers,
		TunnelID:        cfg.Agent.TunnelID,
		GroupID:         cfg.Kafka.GroupID,
		StartFromOldest: cfg.Kafka.StartFromOldest,
	})
	if err != nil {
		return err
	}
	defer src.Close()

	events := make(chan *source.Event, eventBuffer)
	subCtx, cancelSubscribe := context.WithCancel(ctx)
	defer cancelSubscribe()

	subErr := make(chan error, 1)
	go func() {
		subErr <- src.Subscribe(subCtx, cfg.Agent.Topics, events)
	}()

	client, err := stream.NewAgentClient(ctx, cfg.Agent.Gateway, cfg.Agent.Token)
	if err != nil {
		return err
	}
	defer client.Close()

	hello := &qrokv1.AgentHello{
		TunnelId:     cfg.Agent.TunnelID,
		AgentVersion: "dev",
		SourceType:   cfg.Agent.SourceType,
		Topics:       cfg.Agent.Topics,
	}

	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		sessionErr := client.RunSession(ctx, cfg.Agent.Token, hello, func(ctx context.Context, send func(context.Context, *qrokv1.EventEnvelope) error) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case err := <-subErr:
					if err != nil && ctx.Err() == nil {
						return err
					}
					return nil
				case ev, ok := <-events:
					if !ok {
						return nil
					}
					if int64(len(ev.Payload)) > cfg.Events.MaxPayloadBytes {
						log.Warn("event skipped: max_payload_bytes exceeded",
							"topic", ev.Topic, "offset", ev.Offset, "size", len(ev.Payload))
						continue
					}

					eventID := NewEventID()
					envelope := ToEnvelope(cfg.Agent.TunnelID, cfg.Agent.SourceType, eventID, ev)

					ackCtx, cancel := context.WithTimeout(ctx, ackTimeout)
					err := send(ackCtx, envelope)
					cancel()
					if err != nil {
						// Событие не ack'нуто в Kafka — будет перечитано после реконнекта.
						return err
					}

					if err := src.Ack(ctx, ev); err != nil {
						return err
					}
					log.Debug("event delivered to cloud",
						"event_id", eventID, "topic", ev.Topic, "offset", ev.Offset)
				}
			}
		})

		if ctx.Err() != nil {
			return nil
		}
		if sessionErr == nil {
			backoff = initialBackoff
			continue
		}

		log.Warn("agent session interrupted, reconnecting",
			"error", sessionErr.Error(), "backoff", backoff.String())

		if !fault.IsRetryable(sessionErr) {
			return sessionErr
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}
