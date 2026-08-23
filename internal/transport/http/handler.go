package httpapi

import (
	"context"
	"log/slog"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/controlplane/service"
)

type eventService interface {
	ListEvents(ctx context.Context, subject *models.Subject, tunnelID string, limit int) ([]*service.EventListItem, error)
	GetEvent(ctx context.Context, subject *models.Subject, eventID string) (*service.EventView, error)
}

type replayService interface {
	Replay(ctx context.Context, subject *models.Subject, eventID, target string) (*service.ReplayResult, error)
}

type deviceService interface {
	Start(ctx context.Context, projectID, verificationURI string) (*models.DeviceStart, error)
	Approve(ctx context.Context, subject *models.Subject, userCode, projectID string) error
	Poll(ctx context.Context, deviceCode string) (*models.DeviceTokenPoll, error)
}

type authService interface {
	AuthenticateAPI(ctx context.Context, plaintext string) (*models.Subject, error)
	InsecureSubject() *models.Subject
}

// Handler — HTTP handlers for the control plane API.
type Handler struct {
	events eventService
	replay replayService
	device deviceService
	auth   authService
	log    *slog.Logger
}

// NewHandler wires concrete services into HTTP handlers.
func NewHandler(
	events *service.Event,
	replay *service.Replay,
	device *service.Device,
	auth *service.Auth,
	log *slog.Logger,
) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{
		events: events,
		replay: replay,
		device: device,
		auth:   auth,
		log:    log,
	}
}
