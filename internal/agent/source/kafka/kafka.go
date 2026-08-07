// Package kafka — источник событий Kafka (segmentio/kafka-go).
//
// Ключевое свойство: чтение идёт через собственную consumer group
// (qrok-agent-<tunnel_id>), поэтому боевые консьюмеры клиента не затронуты.
// Оффсет коммитится только в Ack — после подтверждения облака.
package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"qrok/internal/agent/source"
	"qrok/pkg/fault"
)

type Config struct {
	Brokers  []string
	TunnelID string // для имени consumer group по умолчанию
	GroupID  string // переопределяет имя группы (по умолчанию qrok-agent-<tunnel_id>)

	// StartFromOldest — читать топики с самого начала при первом запуске группы.
	// По умолчанию false: туннель стримит только новые события.
	StartFromOldest bool

	MinBytes int           // по умолчанию 1
	MaxBytes int           // по умолчанию 10 МБ
	MaxWait  time.Duration // по умолчанию 500ms
}

// GroupID возвращает итоговое имя consumer group.
func (c Config) groupID() string {
	if c.GroupID != "" {
		return c.GroupID
	}
	return "qrok-agent-" + c.TunnelID
}

// Source — источник событий из Kafka.
//
// Контракт надёжности: реализация не подтверждает сообщение брокеру
// при чтении. Ack вызывается агентом только после ack от облака —
// так событие не может потеряться между брокером и облаком (at-least-once).
type Source interface {
	// Subscribe блокирующе читает события в out до отмены ctx.
	// Отмена контекста — штатное завершение (возвращается nil).
	Subscribe(ctx context.Context, topics []string, out chan<- *source.Event) error

	// Ack подтверждает обработку события (коммит оффсета своей consumer group).
	Ack(ctx context.Context, ev *source.Event) error

	Close() error
}

type client struct {
	cfg Config

	mu     sync.Mutex
	reader *kafkago.Reader
}

var _ Source = (*client)(nil)

func New(cfg Config) (Source, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fault.ErrValidation.
			New("Kafka broker addresses are not set").
			WithOp("agent.kafka.new").
			WithHint("set brokers in agent config, e.g. [\"localhost:9092\"]")
	}
	if cfg.TunnelID == "" && cfg.GroupID == "" {
		return nil, fault.ErrValidation.
			New("tunnel_id or explicit group_id required for consumer group").
			WithOp("agent.kafka.new")
	}
	return &client{cfg: cfg}, nil
}

func (c *client) Subscribe(ctx context.Context, topics []string, out chan<- *source.Event) error {
	if len(topics) == 0 {
		return fault.ErrValidation.
			New("no topics configured for subscription").
			WithOp("agent.kafka.subscribe")
	}

	reader, err := c.startReader(topics)
	if err != nil {
		return err
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // отмена контекста — штатное завершение
			}
			return fault.ErrServiceUnavail.
				Wrap(err, "failed to read message from Kafka").
				WithOp("agent.kafka.fetch").
				WithHint("check broker availability and topic permissions").
				WithArg("group_id", c.cfg.groupID())
		}

		select {
		case out <- toEvent(msg):
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *client) startReader(topics []string) (*kafkago.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reader != nil {
		return nil, fault.ErrConflict.
			New("subscription already active").
			WithOp("agent.kafka.subscribe").
			WithHint("one Source allows one subscription; create a new Source")
	}

	startOffset := kafkago.LastOffset
	if c.cfg.StartFromOldest {
		startOffset = kafkago.FirstOffset
	}

	c.reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     c.cfg.Brokers,
		GroupID:     c.cfg.groupID(),
		GroupTopics: topics,
		StartOffset: startOffset,
		MinBytes:    orDefault(c.cfg.MinBytes, 1),
		MaxBytes:    orDefault(c.cfg.MaxBytes, 10<<20),
		MaxWait:     orDefault(c.cfg.MaxWait, 500*time.Millisecond),
		// CommitInterval = 0: только синхронные явные коммиты в Ack.
	})
	return c.reader, nil
}

// Ack коммитит оффсет события в consumer group агента.
func (c *client) Ack(ctx context.Context, ev *source.Event) error {
	c.mu.Lock()
	reader := c.reader
	c.mu.Unlock()

	if reader == nil {
		return fault.ErrConflict.
			New("Ack до подписки").
			WithOp("agent.kafka.ack")
	}

	err := reader.CommitMessages(ctx, kafkago.Message{
		Topic:     ev.Topic,
		Partition: ev.Partition,
		Offset:    ev.Offset,
	})
	if err != nil {
		return fault.ErrServiceUnavail.
			Wrap(err, "failed to commit offset").
			WithOp("agent.kafka.ack").
			WithArg("topic", ev.Topic).
			WithArg("offset", fmt.Sprintf("%d", ev.Offset))
	}
	return nil
}

func (c *client) Close() error {
	c.mu.Lock()
	reader := c.reader
	c.reader = nil
	c.mu.Unlock()

	if reader == nil {
		return nil
	}
	if err := reader.Close(); err != nil {
		return fault.ErrInternal.
			Wrap(err, "Kafka reader close error").
			WithOp("agent.kafka.close")
	}
	return nil
}

func toEvent(m kafkago.Message) *source.Event {
	var headers map[string]string
	if len(m.Headers) > 0 {
		headers = make(map[string]string, len(m.Headers))
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}
	}
	return &source.Event{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Headers:   headers,
		Payload:   m.Value,
		BrokerTS:  m.Time,
	}
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}
