package gateway

import (
	"context"
	"log/slog"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/controlplane/service"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/pkg/fault"

	"google.golang.org/grpc"
)

// Config — настройки gateway.
type Config struct {
	AllowInsecureListen bool
	MaxPayloadBytes     int64
}

// Server реализует gRPC TunnelService.
type Server struct {
	qrokv1.UnimplementedTunnelServiceServer

	cfg        Config
	bus        inproc.EventBus
	events     eventService
	auth       authService
	deliveries deliveryService
	log        *slog.Logger
}

func NewServer(
	cfg Config,
	eventBus inproc.EventBus,
	events *service.Event,
	authSvc *service.Auth,
	deliveries *service.Delivery,
	log *slog.Logger,
) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cfg:        cfg,
		bus:        eventBus,
		events:     events,
		auth:       authSvc,
		deliveries: deliveries,
		log:        log,
	}
}

func (s *Server) AgentStream(stream grpc.BidiStreamingServer[qrokv1.AgentStreamRequest, qrokv1.AgentStreamResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return fault.ErrBadRequest.Wrap(err, "failed to read hello").WithOp("gateway.agent_stream")
	}
	hello := first.GetHello()
	if hello == nil {
		return fault.ErrBadRequest.New("first message must be AgentHello").WithOp("gateway.agent_stream")
	}

	token, err := bearerToken(stream.Context())
	if err != nil {
		return err
	}
	if s.auth == nil {
		return fault.ErrInternal.New("auth is not configured").WithOp("gateway.agent_stream")
	}
	if hello.GetTunnelId() == "" {
		return fault.ErrValidation.New("tunnel_id is required").WithOp("gateway.agent_stream")
	}

	subject, err := s.auth.AuthenticateAgent(stream.Context(), token, hello.GetTunnelId())
	if err != nil {
		return err
	}

	s.log.Info("agent connected",
		"tunnel_id", hello.GetTunnelId(),
		"source_type", hello.GetSourceType(),
		"topics", hello.GetTopics(),
	)

	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		ev := req.GetEvent()
		if ev == nil {
			continue
		}
		if ev.GetTunnelId() == "" {
			ev.TunnelId = hello.GetTunnelId()
		}

		if s.events == nil {
			return fault.ErrInternal.New("event service is not configured").WithOp("gateway.agent_stream")
		}

		if err := validateEventEnvelope("gateway.agent_stream", ev); err != nil {
			return err
		}
		if s.cfg.MaxPayloadBytes > 0 && int64(len(ev.GetPayload())) > s.cfg.MaxPayloadBytes {
			return fault.ErrValidation.New("payload exceeds limit").WithOp("gateway.agent_stream")
		}

		row, err := eventFromProto(ev)
		if err != nil {
			return err
		}
		inserted, err := s.events.Ingest(stream.Context(), subject, row)
		if err != nil {
			return err
		}

		if inserted {
			if err := s.bus.Publish(stream.Context(), ev); err != nil {
				return err
			}
		}

		if err := stream.Send(&qrokv1.AgentStreamResponse{
			Msg: &qrokv1.AgentStreamResponse_Ack{Ack: &qrokv1.Ack{EventId: ev.GetEventId()}},
		}); err != nil {
			return fault.ErrServiceUnavail.Wrap(err, "failed to send ack").WithOp("gateway.agent_stream")
		}
		if inserted {
			s.log.Debug("event accepted", "event_id", ev.GetEventId(), "tunnel_id", ev.GetTunnelId())
		}
	}
}

func (s *Server) ListenStream(stream grpc.BidiStreamingServer[qrokv1.ListenStreamRequest, qrokv1.ListenStreamResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return fault.ErrBadRequest.Wrap(err, "failed to read subscribe").WithOp("gateway.listen_stream")
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return fault.ErrBadRequest.New("first message must be Subscribe").WithOp("gateway.listen_stream")
	}

	if sub.GetTunnelId() == "" {
		return fault.ErrValidation.New("tunnel_id is required").WithOp("gateway.listen_stream")
	}

	subject := &models.Subject{AllowAll: true}
	if !s.cfg.AllowInsecureListen {
		token, err := bearerToken(stream.Context())
		if err != nil {
			return fault.ErrUnauthorized.
				New("ListenStream requires dev token (qrok login); for local development enable gateway.allow_insecure_listen").
				WithOp("gateway.listen_stream").
				WithHint("run qrok login or enable gateway.allow_insecure_listen for dev only")
		}
		if s.auth == nil {
			return fault.ErrInternal.New("auth is not configured").WithOp("gateway.listen_stream")
		}
		subject, err = s.auth.AuthenticateDev(stream.Context(), token, sub.GetTunnelId())
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	events, unsubBus, err := s.bus.Subscribe(ctx)
	if err != nil {
		return err
	}
	defer unsubBus()

	s.log.Info("dev client subscribed",
		"tunnel_id", sub.GetTunnelId(),
		"topics", sub.GetTopics(),
	)

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			req, err := stream.Recv()
			if err != nil {
				cancel()
				return
			}
			if dr := req.GetDeliveryResult(); dr != nil {
				s.recordDeliveryResult(stream.Context(), subject, dr)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-recvDone:
			return nil
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if ev.GetTunnelId() != sub.GetTunnelId() {
				continue
			}
			if !matchesTopics(sub.GetTopics(), ev.GetTopic()) {
				continue
			}
			if err := stream.Send(&qrokv1.ListenStreamResponse{
				Msg: &qrokv1.ListenStreamResponse_Event{Event: ev},
			}); err != nil {
				return nil
			}
		}
	}
}

func (s *Server) recordDeliveryResult(ctx context.Context, subject *models.Subject, result *qrokv1.DeliveryResult) {
	if s.deliveries == nil {
		return
	}
	if result == nil {
		return
	}
	if result.GetDeliveryId() == "" {
		s.log.LogAttrs(ctx, slog.LevelWarn, "invalid DeliveryResult", fault.LogAttrs(
			fault.ErrValidation.New("delivery_id is required").WithOp("gateway.delivery_result"),
		)...)
		return
	}
	if result.GetEventId() == "" {
		s.log.LogAttrs(ctx, slog.LevelWarn, "invalid DeliveryResult", fault.LogAttrs(
			fault.ErrValidation.New("event_id is required").WithOp("gateway.delivery_result"),
		)...)
		return
	}

	err := s.deliveries.RecordResult(ctx, subject, &models.DeliveryResult{
		DeliveryID: result.GetDeliveryId(),
		EventID:    result.GetEventId(),
		StatusCode: result.GetStatusCode(),
		Error:      result.GetError(),
		LatencyMS:  int32(result.GetLatencyMs()),
	})
	if err != nil {
		s.log.LogAttrs(ctx, slog.LevelWarn, "failed to save DeliveryResult", fault.LogAttrs(err)...)
		return
	}
	s.log.Debug("delivery result",
		"delivery_id", result.GetDeliveryId(),
		"event_id", result.GetEventId(),
		"status_code", result.GetStatusCode(),
	)
}

func matchesTopics(filter []string, topic string) bool {
	if len(filter) == 0 {
		return true
	}
	for _, t := range filter {
		if t == topic {
			return true
		}
	}
	return false
}
