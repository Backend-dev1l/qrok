package gateway

import (
	"context"

	"qrok/internal/controlplane/infrastructure/models"
)

type eventService interface {
	Ingest(ctx context.Context, subject *models.Subject, ev *models.Event) (inserted bool, err error)
}

type authService interface {
	AuthenticateAgent(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error)
	AuthenticateDev(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error)
}

type deliveryService interface {
	RecordResult(ctx context.Context, subject *models.Subject, result *models.DeliveryResult) error
}
