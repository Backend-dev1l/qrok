package service

import (
	"context"
	"crypto/rand"

	"github.com/oklog/ulid/v2"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/model"
)

// ReplayService schedules replay deliveries.
type ReplayService interface {
	Replay(ctx context.Context, subject *model.Subject, eventID, target string) (*ReplayResult, error)
}

// ReplayResult is the outcome of scheduling a replay.
type ReplayResult struct {
	DeliveryID string `json:"delivery_id"`
	EventID    string `json:"event_id"`
	TargetID   string `json:"target_id"`
	Status     string `json:"status"`
}

type replayService struct {
	events     eventstore.Repository
	deliveries delivery.Repository
	auth       auth.Repository
	bus        inproc.EventBus
}

func NewReplayService(events eventstore.Repository, deliveries delivery.Repository, authRepo auth.Repository, eventBus inproc.EventBus) ReplayService {
	return &replayService{
		events:     events,
		deliveries: deliveries,
		auth:       authRepo,
		bus:        eventBus,
	}
}

func (s *replayService) Replay(ctx context.Context, subject *model.Subject, eventID, target string) (*ReplayResult, error) {
	const op = "replay.replay"
	if eventID == "" {
		return nil, validationErr(op, "event_id обязателен")
	}
	if target == "" {
		target = "*"
	}
	if err := authorizeEvent(ctx, s.auth, subject, eventID); err != nil {
		return nil, err
	}

	var (
		ev  *model.Event
		err error
	)
	if subject != nil && !subject.AllowAll && subject.ProjectID != "" {
		ev, err = s.events.GetByIDForProject(ctx, subject.ProjectID, eventID)
	} else {
		ev, err = s.events.GetByID(ctx, eventID)
	}
	if err != nil {
		return nil, notFoundErr(op, "событие не найдено", err)
	}

	payload, err := s.events.GetPayload(ctx, ev)
	if err != nil {
		return nil, notFoundErr(op, "payload отсутствует", err)
	}

	deliveryID := ulid.MustNew(ulid.Now(), rand.Reader).String()
	if err := s.deliveries.CreatePending(ctx, &model.Delivery{
		ID:       deliveryID,
		EventID:  eventID,
		TargetID: target,
		Kind:     model.DeliveryKindReplay,
		Status:   model.DeliveryStatusPending,
	}); err != nil {
		return nil, mapRepoErr(op, err)
	}

	if err := s.bus.Publish(ctx, toReplayEnvelope(ev, payload, deliveryID)); err != nil {
		return nil, mapRepoErr(op, err)
	}

	return &ReplayResult{
		DeliveryID: deliveryID,
		EventID:    eventID,
		TargetID:   target,
		Status:     string(model.DeliveryStatusPending),
	}, nil
}
