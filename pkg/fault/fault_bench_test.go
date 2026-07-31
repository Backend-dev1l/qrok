package fault

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
)

// Стартовые бенчмарки — образец практики для проекта. Основные бенчмарки
// (otter-кэш, PayloadStore/SQL, сериализация envelope) появятся вместе
// с самими компонентами; регрессии сравниваются через benchstat.

var benchErr = errors.New("dial tcp 10.0.1.5:9092: connection refused")

// sink не даёт компилятору выкинуть результат и спрятать аллокации в стек.
var sink any

func BenchmarkWrapChain(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sink = ErrServiceUnavail.
			Wrap(benchErr, "kafka недоступна").
			WithOp("agent.kafka.subscribe").
			WithArg("group_id", "qrok-agent-t1")
	}
}

func BenchmarkFromErrorFault(b *testing.B) {
	err := ErrNotFound.New("event not found")
	b.ReportAllocs()
	for b.Loop() {
		sink = FromError(err)
	}
}

func BenchmarkRenderCLI(b *testing.B) {
	b.Setenv("NO_COLOR", "1")
	err := ErrServiceUnavail.
		Wrap(benchErr, "kafka недоступна").
		WithOp("agent.kafka.subscribe").
		WithHint("проверьте --brokers")
	b.ReportAllocs()
	for b.Loop() {
		sink = RenderCLI(err)
	}
}

func BenchmarkWriteHTTPError(b *testing.B) {
	err := ErrNotFound.New("event not found").WithArg("event_id", "01J000")
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		WriteHTTPError(ctx, httptest.NewRecorder(), err)
	}
}

func BenchmarkLogAttrs(b *testing.B) {
	err := ErrConflict.New("событие уже существует").
		WithOp("eventstore.insert").
		WithArg("event_id", "01J000")
	b.ReportAllocs()
	for b.Loop() {
		sink = LogAttrs(err)
	}
}
