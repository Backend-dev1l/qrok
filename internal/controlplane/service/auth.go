package service

import (
	"context"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

// AuthService authenticates callers and checks access scope.
type AuthService interface {
	AuthenticateAPI(ctx context.Context, plaintext string) (*models.Subject, error)
	AuthenticateAgent(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error)
	AuthenticateDev(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error)
	InsecureSubject() *models.Subject
}

type authService struct {
	repo auth.Repository
}

func NewAuthService(repo auth.Repository) AuthService {
	return &authService{repo: repo}
}

func (s *authService) InsecureSubject() *models.Subject {
	return &models.Subject{AllowAll: true}
}

func (s *authService) AuthenticateAPI(ctx context.Context, plaintext string) (*models.Subject, error) {
	const op = "auth.authenticate_api"
	if plaintext == "" {
		return nil, fault.ErrUnauthorized.New("token required").WithOp(op)
	}

	tokenID, projectID, err := s.repo.FindAPIToken(ctx, auth.HashAgentToken(plaintext))
	if err != nil {
		return nil, unauthorizedErr(op, "invalid or revoked token", err)
	}

	return &models.Subject{
		TokenID:   tokenID,
		ProjectID: projectID,
	}, nil
}

func (s *authService) AuthenticateAgent(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error) {
	const op = "auth.authenticate_agent"
	if plaintext == "" || tunnelID == "" {
		return nil, fault.ErrUnauthorized.New("token and tunnel_id required").WithOp(op)
	}

	tokenID, projectID, err := s.repo.FindAgentToken(ctx, auth.HashAgentToken(plaintext), tunnelID)
	if err != nil {
		return nil, unauthorizedErr(op, "invalid agent token or tunnel", err)
	}

	return &models.Subject{
		TokenID:   tokenID,
		ProjectID: projectID,
		TunnelID:  tunnelID,
	}, nil
}

func (s *authService) AuthenticateDev(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error) {
	const op = "auth.authenticate_dev"
	if plaintext == "" || tunnelID == "" {
		return nil, fault.ErrUnauthorized.New("dev token and tunnel_id required").WithOp(op)
	}
	if !auth.IsDevToken(plaintext) {
		return nil, fault.ErrUnauthorized.New("expected dev token (qrok_dev_…)").WithOp(op)
	}

	tokenID, projectID, userID, err := s.repo.FindDevToken(ctx, auth.HashAgentToken(plaintext), tunnelID)
	if err != nil {
		return nil, unauthorizedErr(op, "invalid dev token or no tunnel access", err)
	}

	return &models.Subject{
		TokenID:   tokenID,
		ProjectID: projectID,
		UserID:    userID,
		TunnelID:  tunnelID,
	}, nil
}

func authorizeTunnel(ctx context.Context, repo auth.Repository, subject *models.Subject, tunnelID string) error {
	const op = "auth.authorize_tunnel"
	if subject == nil || subject.AllowAll {
		return nil
	}
	if tunnelID == "" {
		return validationErr(op, "tunnel_id is required")
	}
	if subject.ProjectID == "" {
		return fault.ErrUnauthorized.New("authorization required").WithOp(op)
	}

	owned, err := repo.TunnelOwnedByProject(ctx, subject.ProjectID, tunnelID)
	if err != nil {
		return mapRepoErr(op, err)
	}
	if !owned {
		return forbiddenErr(op, "no access to tunnel")
	}
	return nil
}

func authorizeEvent(ctx context.Context, repo auth.Repository, subject *models.Subject, eventID string) error {
	const op = "auth.authorize_event"
	if subject == nil || subject.AllowAll {
		return nil
	}
	if eventID == "" {
		return validationErr(op, "event_id is required")
	}
	if subject.ProjectID == "" {
		return fault.ErrUnauthorized.New("authorization required").WithOp(op)
	}

	owned, err := repo.EventOwnedByProject(ctx, subject.ProjectID, eventID)
	if err != nil {
		return mapRepoErr(op, err)
	}
	if !owned {
		return forbiddenErr(op, "no access to event")
	}
	return nil
}

func ensureSubject(subject *models.Subject) error {
	if subject == nil || subject.AllowAll || subject.ProjectID != "" {
		return nil
	}
	return fault.ErrUnauthorized.New("authorization required").WithOp("auth.ensure_subject")
}

// Test helpers expose internal error mapping for contract tests.
func MapRepoErrForTest(op string, err error) error       { return mapRepoErr(op, err) }
func NotFoundErrForTest(op, message string, err error) error { return notFoundErr(op, message, err) }
func UnauthorizedErrForTest(op, message string, err error) error {
	return unauthorizedErr(op, message, err)
}
