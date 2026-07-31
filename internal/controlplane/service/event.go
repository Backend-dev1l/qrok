package service

import (
	"context"
	"time"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/model"
)

// EventView is an event with optional inline payload for API responses.
type EventView struct {
	Event          *model.Event
	Payload        []byte
	HasFullPayload bool
	Deliveries     []*model.Delivery
}

// EventListItem is an event row with optional latest delivery status.
type EventListItem struct {
	Event          *model.Event
	LatestDelivery *model.Delivery
}

// EventService handles event queries and ingestion.
type EventService interface {
	ListEvents(ctx context.Context, subject *model.Subject, tunnelID string, limit int) ([]*EventListItem, error)
	GetEvent(ctx context.Context, subject *model.Subject, eventID string) (*EventView, error)
	Ingest(ctx context.Context, subject *model.Subject, ev *model.Event, maxPayloadBytes int64) (inserted bool, err error)
}

type eventService struct {
	events     eventstore.Repository
	deliveries delivery.Repository
	auth       auth.Repository
}

func NewEventService(events eventstore.Repository, deliveries delivery.Repository, authRepo auth.Repository) EventService {
	return &eventService{events: events, deliveries: deliveries, auth: authRepo}
}

func (s *eventService) ListEvents(ctx context.Context, subject *model.Subject, tunnelID string, limit int) ([]*EventListItem, error) {
	const op = "event.list"
	if tunnelID == "" {
		return nil, validationErr(op, "tunnel_id обязателен")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		return nil, validationErr(op, "limit: ожидается 1..200")
	}
	if err := authorizeTunnel(ctx, s.auth, subject, tunnelID); err != nil {
		return nil, err
	}

	events, err := s.events.ListByTunnel(ctx, tunnelID, limit)
	if err != nil {
		return nil, mapRepoErr(op, err)
	}

	eventIDs := make([]string, 0, len(events))
	for _, ev := range events {
		eventIDs = append(eventIDs, ev.ID)
	}
	latest, err := s.deliveries.ListLatestByEventIDs(ctx, eventIDs)
	if err != nil {
		return nil, mapRepoErr(op, err)
	}

	out := make([]*EventListItem, 0, len(events))
	for _, ev := range events {
		out = append(out, &EventListItem{
			Event:          ev,
			LatestDelivery: latest[ev.ID],
		})
	}
	return out, nil
}

func (s *eventService) GetEvent(ctx context.Context, subject *model.Subject, eventID string) (*EventView, error) {
	const op = "event.get"
	if eventID == "" {
		return nil, validationErr(op, "event_id обязателен")
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

	deliveries, err := s.deliveries.ListByEventID(ctx, eventID)
	if err != nil {
		return nil, mapRepoErr(op, err)
	}

	return &EventView{
		Event:          ev,
		Payload:        payload,
		HasFullPayload: len(payload) > 0,
		Deliveries:     deliveries,
	}, nil
}

func (s *eventService) Ingest(ctx context.Context, subject *model.Subject, ev *model.Event, maxPayloadBytes int64) (bool, error) {
	const op = "event.ingest"
	if ev == nil || ev.ID == "" || ev.TunnelID == "" || ev.Topic == "" {
		return false, validationErr(op, "неполное событие")
	}
	if len(ev.Payload) == 0 {
		return false, validationErr(op, "пустой payload")
	}
	if maxPayloadBytes > 0 && int64(len(ev.Payload)) > maxPayloadBytes {
		return false, validationErr(op, "payload превышает лимит").WithArg("event_id", ev.ID)
	}
	if subject != nil && !subject.AllowAll {
		if subject.TunnelID != "" && ev.TunnelID != subject.TunnelID {
			return false, forbiddenErr(op, "tunnel_id события не совпадает с агентом")
		}
		if err := authorizeTunnel(ctx, s.auth, subject, ev.TunnelID); err != nil {
			return false, err
		}
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}

	inserted, err := s.events.Insert(ctx, ev)
	if err != nil {
		return false, mapRepoErr(op, err)
	}
	return inserted, nil
}
