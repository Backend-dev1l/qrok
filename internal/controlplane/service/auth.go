package service

import (
	"context"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

type authQuerier interface {
	FindAgentToken(ctx context.Context, tokenHash, tunnelID string) (tokenID, projectID string, err error)
	FindAPIToken(ctx context.Context, tokenHash string) (tokenID, projectID string, err error)
	FindDevToken(ctx context.Context, tokenHash, tunnelID string) (tokenID, projectID, userID string, err error)
}

type scopeQuerier interface {
	TunnelOwnedByProject(ctx context.Context, projectID, tunnelID string) (bool, error)
	EventOwnedByProject(ctx context.Context, projectID, eventID string) (bool, error)
}

// Auth authenticates callers and checks access scope.
type Auth struct {
	repo authQuerier
}

func NewAuthService(repo authQuerier) *Auth {
	return &Auth{repo: repo}
}

func (s *Auth) InsecureSubject() *models.Subject {
	return &models.Subject{AllowAll: true}
}

func (s *Auth) AuthenticateAPI(ctx context.Context, plaintext string) (*models.Subject, error) {
	const op = "auth.authenticate_api"

	tokenID, projectID, err := s.repo.FindAPIToken(ctx, auth.HashAgentToken(plaintext))
	if err != nil {
		return nil, unauthorizedErr(op, "invalid or revoked token", err)
	}

	return &models.Subject{
		TokenID:   tokenID,
		ProjectID: projectID,
	}, nil
}

func (s *Auth) AuthenticateAgent(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error) {
	const op = "auth.authenticate_agent"

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

func (s *Auth) AuthenticateDev(ctx context.Context, plaintext, tunnelID string) (*models.Subject, error) {
	const op = "auth.authenticate_dev"
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

func authorizeTunnel(ctx context.Context, repo scopeQuerier, subject *models.Subject, tunnelID string) error {
	const op = "auth.authorize_tunnel"
	if subject != nil && subject.AllowAll {
		return nil
	}
	if subject == nil || subject.ProjectID == "" {
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

func authorizeEvent(ctx context.Context, repo scopeQuerier, subject *models.Subject, eventID string) error {
	const op = "auth.authorize_event"
	if subject != nil && subject.AllowAll {
		return nil
	}
	if subject == nil || subject.ProjectID == "" {
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
	if subject != nil && (subject.AllowAll || subject.ProjectID != "") {
		return nil
	}
	return fault.ErrUnauthorized.New("authorization required").WithOp("auth.ensure_subject")
}

// Test helpers expose internal error mapping for contract tests.
func MapRepoErrForTest(op string, err error) error { return mapRepoErr(op, err) }
func NotFoundErrForTest(op, message string, err error) error {
	return notFoundErr(op, message, err)
}
func UnauthorizedErrForTest(op, message string, err error) error {
	return unauthorizedErr(op, message, err)
}
