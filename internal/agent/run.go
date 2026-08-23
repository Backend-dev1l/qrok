package agent

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"qrok/internal/agent/source"
	"qrok/internal/agent/source/kafka"
	"qrok/internal/agent/stream"
	"qrok/internal/config"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/tunnel/grpcutil"
	"qrok/pkg/fault"
)

const (
	eventBuffer    = 64
	ackTimeout     = 30 * time.Second
	maxBackoff     = 30 * time.Second
	initialBackoff = time.Second
)

type eventSource interface {
	Subscribe(ctx context.Context, topics []string, out chan<- *source.Event) error
	Ack(ctx context.Context, ev *source.Event) error
	Close() error
}

type agentStream interface {
	RunSession(
		ctx context.Context,
		token string,
		hello *qrokv1.AgentHello,
		handle func(context.Context, func(context.Context, *qrokv1.EventEnvelope) error) error,
	) error
	Close() error
}

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
		TLS:             cfg.Kafka.TLS,
		TLSServerName:   cfg.Kafka.TLSServerName,
		TLSCAFile:       cfg.Kafka.TLSCAFile,
		SASLMechanism:   cfg.Kafka.SASLMechanism,
		SASLUsername:    cfg.Kafka.SASLUsername,
		SASLPassword:    cfg.Kafka.SASLPassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	client, err := stream.NewAgentClient(ctx, cfg.Agent.Gateway, cfg.Agent.Token, grpcutil.Config{
		UseTLS:     cfg.Agent.TLS,
		ServerName: cfg.Agent.TLSServerName,
		CAFile:     cfg.Agent.TLSCAFile,
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	return run(ctx, cfg, log, src, client)
}

func run(ctx context.Context, cfg *config.Agent, log *slog.Logger, src eventSource, client agentStream) error {
	events := make(chan *source.Event, eventBuffer)
	subCtx, cancelSubscribe := context.WithCancel(ctx)
	defer cancelSubscribe()

	subErr := make(chan error, 1)
	go func() {
		subErr <- src.Subscribe(subCtx, cfg.Agent.Topics, events)
	}()

	hello := &qrokv1.AgentHello{
		TunnelId:     cfg.Agent.TunnelID,
		AgentVersion: "dev",
		SourceType:   cfg.Agent.SourceType,
		Topics:       cfg.Agent.Topics,
	}

	var pending *source.Event
	var envelope *qrokv1.EventEnvelope
	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		sessionErr := client.RunSession(ctx, cfg.Agent.Token, hello, func(ctx context.Context, send func(context.Context, *qrokv1.EventEnvelope) error) error {
			for {
				if pending == nil {
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
						pending = ev
					}
				}

				if int64(len(pending.Payload)) > cfg.Events.MaxPayloadBytes {
					return fault.ErrValidation.
						New("event exceeds max_payload_bytes").
						WithOp("agent.deliver").
						WithArg("topic", pending.Topic).
						WithArg("offset", strconv.FormatInt(pending.Offset, 10)).
						WithArg("size", strconv.Itoa(len(pending.Payload)))
				}

				if envelope == nil {
					eventID := SourceEventID(cfg.Agent.TunnelID, cfg.Agent.SourceType, pending)
					envelope = ToEnvelope(cfg.Agent.TunnelID, cfg.Agent.SourceType, eventID, pending)
				}

				ackCtx, cancel := context.WithTimeout(ctx, ackTimeout)
				err := send(ackCtx, envelope)
				cancel()
				if err != nil {
					return err
				}

				if err := src.Ack(ctx, pending); err != nil {
					return err
				}
				log.Debug("event delivered to cloud",
					"event_id", envelope.GetEventId(), "topic", pending.Topic, "offset", pending.Offset)
				pending = nil
				envelope = nil
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
