package service

import (
	"context"

	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/model"
)

// DeliveryService records delivery results from dev clients.
type DeliveryService interface {
	RecordResult(ctx context.Context, result *model.DeliveryResult) error
}

type deliveryService struct {
	repo delivery.Repository
}

func NewDeliveryService(repo delivery.Repository) DeliveryService {
	return &deliveryService{repo: repo}
}

func (s *deliveryService) RecordResult(ctx context.Context, result *model.DeliveryResult) error {
	const op = "delivery.record_result"
	if result == nil || result.DeliveryID == "" || result.EventID == "" {
		return validationErr(op, "неполный DeliveryResult")
	}

	status := model.DeliveryStatusDelivered
	if result.Error != "" || result.StatusCode < 200 || result.StatusCode >= 300 {
		status = model.DeliveryStatusFailed
	}

	rec := &model.Delivery{
		ID:       result.DeliveryID,
		EventID:  result.EventID,
		TargetID: "*",
		Kind:     model.DeliveryKindLive,
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
