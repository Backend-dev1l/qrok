package service

import (
	"context"

	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/models"
)

// DeliveryService records delivery results from dev clients.
type DeliveryService interface {
	RecordResult(ctx context.Context, result *models.DeliveryResult) error
}

type deliveryService struct {
	repo delivery.Repository
}

func NewDeliveryService(repo delivery.Repository) DeliveryService {
	return &deliveryService{repo: repo}
}

func (s *deliveryService) RecordResult(ctx context.Context, result *models.DeliveryResult) error {
	const op = "delivery.record_result"
	if result == nil || result.DeliveryID == "" || result.EventID == "" {
		return validationErr(op, "incomplete DeliveryResult")
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
