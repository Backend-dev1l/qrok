// Package source — общие типы событий брокера для реализаций (kafka, rabbitmq, …).
package source

import "time"

// Event — сообщение, прочитанное из брокера, до обогащения:
// event_id (ULID) и туннельные метаданные назначает слой агента.
type Event struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Headers   map[string]string
	Payload   []byte
	BrokerTS  time.Time
}
