package inproc

import (
	"context"
	"errors"
	"sync"

	qrokv1 "qrok/internal/proto/qrok/v1"
)

const defaultBuffer = 256

// Bus — fan-out шина на каналах внутри одного процесса.
type Bus struct {
	mu   sync.RWMutex
	subs map[int]chan *qrokv1.EventEnvelope
	next int
	buf  int
}

var _ EventBus = (*Bus)(nil)

func New(buffer int) *Bus {
	if buffer <= 0 {
		buffer = defaultBuffer
	}
	return &Bus{
		subs: make(map[int]chan *qrokv1.EventEnvelope),
		buf:  buffer,
	}
}

func (b *Bus) Publish(_ context.Context, event *qrokv1.EventEnvelope) error {
	if event == nil {
		return errors.New("empty event")
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, ch := range b.subs {
		ch <- event
	}
	return nil
}

func (b *Bus) Subscribe(ctx context.Context) (<-chan *qrokv1.EventEnvelope, func(), error) {
	ch := make(chan *qrokv1.EventEnvelope, b.buf)

	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = ch
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
			close(ch)
		})
	}

	go func() {
		<-ctx.Done()
		cancel()
	}()

	return ch, cancel, nil
}
