package gateway

import (
	"context"
	"log/slog"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/model"
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
	events     service.EventService
	auth       service.AuthService
	deliveries service.DeliveryService
	log        *slog.Logger
}

func NewServer(cfg Config, eventBus inproc.EventBus, events service.EventService, authSvc service.AuthService, deliveries service.DeliveryService, log *slog.Logger) *Server {
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
		return fault.ErrBadRequest.Wrap(err, "не удалось прочитать hello").WithOp("gateway.agent_stream")
	}
	hello := first.GetHello()
	if hello == nil {
		return fault.ErrBadRequest.New("первое сообщение должно быть AgentHello").WithOp("gateway.agent_stream")
	}

	token, err := bearerToken(stream.Context())
	if err != nil {
		return err
	}
	if s.auth == nil {
		return fault.ErrInternal.New("auth не настроен").WithOp("gateway.agent_stream")
	}
	subject, err := s.auth.AuthenticateAgent(stream.Context(), token, hello.GetTunnelId())
	if err != nil {
		return err
	}

	s.log.Info("агент подключился",
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
			return fault.ErrInternal.New("event service не настроен").WithOp("gateway.agent_stream")
		}

		row, err := service.EventFromProto(ev)
		if err != nil {
			return err
		}
		inserted, err := s.events.Ingest(stream.Context(), subject, row, s.cfg.MaxPayloadBytes)
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
			return fault.ErrServiceUnavail.Wrap(err, "не удалось отправить ack").WithOp("gateway.agent_stream")
		}
		if inserted {
			s.log.Debug("событие принято", "event_id", ev.GetEventId(), "tunnel_id", ev.GetTunnelId())
		}
	}
}

func (s *Server) ListenStream(stream grpc.BidiStreamingServer[qrokv1.ListenStreamRequest, qrokv1.ListenStreamResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return fault.ErrBadRequest.Wrap(err, "не удалось прочитать subscribe").WithOp("gateway.listen_stream")
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return fault.ErrBadRequest.New("первое сообщение должно быть Subscribe").WithOp("gateway.listen_stream")
	}

	if !s.cfg.AllowInsecureListen {
		token, err := bearerToken(stream.Context())
		if err != nil {
			return fault.ErrUnauthorized.
				New("ListenStream требует dev-токен (qrok login); для локальной разработки включите gateway.allow_insecure_listen").
				WithOp("gateway.listen_stream").
				WithHint("выполните qrok login или включите gateway.allow_insecure_listen только для dev")
		}
		if s.auth == nil {
			return fault.ErrInternal.New("auth не настроен").WithOp("gateway.listen_stream")
		}
		if _, err := s.auth.AuthenticateDev(stream.Context(), token, sub.GetTunnelId()); err != nil {
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

	s.log.Info("dev-клиент подписался",
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
				s.recordDeliveryResult(stream.Context(), dr)
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

func (s *Server) recordDeliveryResult(ctx context.Context, result *qrokv1.DeliveryResult) {
	if s.deliveries == nil {
		return
	}
	err := s.deliveries.RecordResult(ctx, &model.DeliveryResult{
		DeliveryID: result.GetDeliveryId(),
		EventID:    result.GetEventId(),
		StatusCode: result.GetStatusCode(),
		Error:      result.GetError(),
		LatencyMS:  int32(result.GetLatencyMs()),
	})
	if err != nil {
		s.log.LogAttrs(ctx, slog.LevelWarn, "не удалось сохранить DeliveryResult", fault.LogAttrs(err)...)
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
