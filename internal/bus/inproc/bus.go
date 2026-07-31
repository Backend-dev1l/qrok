// Package inproc — in-process реализация внутренней шины событий.
package inproc

import (
	"context"

	qrokv1 "qrok/internal/proto/qrok/v1"
)

// EventBus доставляет EventEnvelope подписчикам внутри процесса.
type EventBus interface {
	// Publish рассылает событие всем активным подписчикам.
	Publish(ctx context.Context, event *qrokv1.EventEnvelope) error

	// Subscribe регистрирует подписчика. Второе значение — функция отмены подписки.
	Subscribe(ctx context.Context) (<-chan *qrokv1.EventEnvelope, func(), error)
}
