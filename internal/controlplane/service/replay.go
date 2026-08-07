package service

import (
	"context"
	"crypto/rand"

	"github.com/oklog/ulid/v2"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/infrastructure/models"
)

// ReplayService schedules replay deliveries.
type ReplayService interface {
	Replay(ctx context.Context, subject *models.Subject, eventID, target string) (*ReplayResult, error)
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

func (s *replayService) Replay(ctx context.Context, subject *models.Subject, eventID, target string) (*ReplayResult, error) {
	const op = "replay.replay"
	if eventID == "" {
		return nil, validationErr(op, "event_id is required")
	}
	if target == "" {
		target = "*"
	}
	if err := authorizeEvent(ctx, s.auth, subject, eventID); err != nil {
		return nil, err
	}

	var (
		ev  *models.Event
		err error
	)
	if subject != nil && !subject.AllowAll && subject.ProjectID != "" {
		ev, err = s.events.GetByIDForProject(ctx, subject.ProjectID, eventID)
	} else {
		ev, err = s.events.GetByID(ctx, eventID)
	}
	if err != nil {
		return nil, notFoundErr(op, "event not found", err)
	}

	payload, err := s.events.GetPayload(ctx, ev)
	if err != nil {
		return nil, notFoundErr(op, "payload missing", err)
	}

	deliveryID := ulid.MustNew(ulid.Now(), rand.Reader).String()
	if err := s.deliveries.CreatePending(ctx, &models.Delivery{
		ID:       deliveryID,
		EventID:  eventID,
		TargetID: target,
		Kind:     models.DeliveryKindReplay,
		Status:   models.DeliveryStatusPending,
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
		Status:     string(models.DeliveryStatusPending),
	}, nil
}
