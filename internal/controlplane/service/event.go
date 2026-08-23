package service

import (
	"context"
	"time"

	"qrok/internal/controlplane/infrastructure/models"
)

type eventQuerier interface {
	Insert(ctx context.Context, ev *models.Event) (bool, error)
	GetByID(ctx context.Context, id string) (*models.Event, error)
	GetByIDForProject(ctx context.Context, projectID, eventID string) (*models.Event, error)
	GetPayload(ctx context.Context, ev *models.Event) ([]byte, error)
	ListByTunnel(ctx context.Context, tunnelID string, limit int) ([]*models.Event, error)
}

type eventDeliveryQuerier interface {
	ListByEventID(ctx context.Context, eventID string) ([]*models.Delivery, error)
	ListLatestByEventIDs(ctx context.Context, eventIDs []string) (map[string]*models.Delivery, error)
}

// EventView is an event with optional inline payload for API responses.
type EventView struct {
	Event          *models.Event
	Payload        []byte
	HasFullPayload bool
	Deliveries     []*models.Delivery
}

// EventListItem is an event row with optional latest delivery status.
type EventListItem struct {
	Event          *models.Event
	LatestDelivery *models.Delivery
}

// Event handles event queries and ingestion.
type Event struct {
	events     eventQuerier
	deliveries eventDeliveryQuerier
	scope      scopeQuerier
}

func NewEventService(events eventQuerier, deliveries eventDeliveryQuerier, scope scopeQuerier) *Event {
	return &Event{events: events, deliveries: deliveries, scope: scope}
}

func (s *Event) ListEvents(ctx context.Context, subject *models.Subject, tunnelID string, limit int) ([]*EventListItem, error) {
	const op = "event.list"
	if limit <= 0 {
		limit = 50
	}
	if err := authorizeTunnel(ctx, s.scope, subject, tunnelID); err != nil {
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

func (s *Event) GetEvent(ctx context.Context, subject *models.Subject, eventID string) (*EventView, error) {
	const op = "event.get"
	if err := authorizeEvent(ctx, s.scope, subject, eventID); err != nil {
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

func (s *Event) Ingest(ctx context.Context, subject *models.Subject, ev *models.Event) (bool, error) {
	const op = "event.ingest"
	if subject != nil && !subject.AllowAll {
		if subject.TunnelID != "" && ev.TunnelID != subject.TunnelID {
			return false, forbiddenErr(op, "event tunnel_id does not match agent")
		}
		if err := authorizeTunnel(ctx, s.scope, subject, ev.TunnelID); err != nil {
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
