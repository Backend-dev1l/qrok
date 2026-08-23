package service

import (
	"context"

	"qrok/internal/controlplane/infrastructure/models"
)

type deliveryQuerier interface {
	UpsertResult(ctx context.Context, rec *models.Delivery) error
}

// Delivery records delivery results from dev clients.
type Delivery struct {
	repo  deliveryQuerier
	scope scopeQuerier
}

func NewDeliveryService(repo deliveryQuerier, scope scopeQuerier) *Delivery {
	return &Delivery{repo: repo, scope: scope}
}

func (s *Delivery) RecordResult(ctx context.Context, subject *models.Subject, result *models.DeliveryResult) error {
	const op = "delivery.record_result"

	if err := authorizeEvent(ctx, s.scope, subject, result.EventID); err != nil {
		return err
	}

	status := models.DeliveryStatusDelivered
	if result.Error != "" || result.StatusCode < 200 || result.StatusCode >= 300 {
		status = models.DeliveryStatusFailed
	}

	rec := &models.Delivery{
		ID:       result.DeliveryID,
		EventID:  result.EventID,
		TargetID: "*",
		Kind:     models.DeliveryKindLive,
		Status:   status,
		Error:    result.Error,
	}
	if result.StatusCode != 0 {
		code := result.StatusCode
		rec.StatusCode = &code
	}
	if result.LatencyMS != 0 {
		ms := result.LatencyMS
		rec.LatencyMS = &ms
	}

	if err := s.repo.UpsertResult(ctx, rec); err != nil {
		return mapRepoErr(op, err)
	}
	return nil
}
