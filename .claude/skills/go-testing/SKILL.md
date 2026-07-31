---
name: go-testing
description: >-
  Go testing conventions for qrok: unit, integration (build tag integration),
  e2e (build tag e2e), testify, compose-backed infra, Makefile targets.
  Use when writing, fixing, or reviewing tests, test coverage, flaky tests,
  or running make test / test-integration / test-e2e.
---

# Go Testing (qrok)

## Пирамида тестов

| Уровень | Build-tag | Команда | Инфраструктура |
|---------|-----------|---------|----------------|
| Unit | нет | `make test` | нет (или недоступный порт) |
| Integration | `integration` | `make test-integration` | `make compose-up` (Postgres, Kafka) |
| E2E | `e2e` | `make test-e2e` | compose + миграции |

`go test ./...` без тегов — только unit; быстрый CI.

## Обязательный стек

- **testify**: `require` — setup/критичные проверки; `assert` — проверки полей
- **race**: `go test -race` в Makefile для unit/integration
- **Без testcontainers** — реальные сервисы из `deploy/docker-compose.yml`
- Общие DSN/брокер: `internal/testutil/integ` (`PostgresDSN`, `KafkaBroker`, `MinIOEndpoint`)
- DSN по умолчанию: `127.0.0.1`, не `localhost` (WSL/Windows IPv6)

## Файлы и теги

```go
//go:build integration

package kafka

import (
    "testing"
    "github.com/stretchr/testify/require"
    "qrok/internal/testutil/integ"
)
```

E2E: `//go:build e2e`, пакет `e2e_test` в `test/e2e/`, хелперы в `setup_test.go`.

Именование: `*_test.go`; интеграционные — `*_integration_test.go` в том же пакете.

## Паттерны

### Unit

- Table-driven где уместно; `t.Parallel()` только если нет shared state / гонок
- Фейки рядом с тестом (`memObjectStore` в `store_test.go`) или в пакете реализации
- Интерфейсы — в пакете реализации, не в отдельном `interfaces.go` (см. `.cursor/rules/interfaces-at-impl.mdc`)

### Integration / E2E

- Инфра недоступна → `t.Skipf(...)`, не `t.Fatal` (чтобы `make test` не ломался без Docker)
- Проверка доступности: dial/ping перед тестом (`requireBroker`, `connectStores`)
- Асинхронность: `require.Eventually` вместо ручных циклов `time.Sleep`
- `t.Helper()` во всех хелперах; `t.Cleanup` для закрытия ресурсов

### Изоляция от `.env`

Unit-тесты не должны зависеть от `make export .env`. Для `QROK_*` — `unsetEnv` / `clearServerEnv` (см. `internal/config/config_test.go`).

## Команды

```bash
make test              # unit + race
make verify            # go build ./... + unit (после рефакторинга)
make test-integration  # нужен compose-up
make test-e2e          # compose-up + migrate-up
make cover             # coverage.html
make mutation          # go-mutesting на критичных пакетах
```

Перед завершением задачи с тестами: `make verify`; при изменении интеграций — `make test-integration` или `make test-e2e` если Docker доступен.

## Что тестировать

- **Да**: бизнес-логика, валидация, fault-коды, ack→commit, replay, HTTP middleware
- **Нет**: тривиальные геттеры, generated protobuf, очевидные one-liner без ветвлений

Каждый баг-фикс — регрессионный тест в том же уровне пирамиды.

## Антипаттерны

- Отдельный пакет только с интерфейсом ради моков
- `testcontainers-go` (не используем)
- Inline `<script>` в тестируемом HTML под CSP `script-src 'self'` — только внешние `.js`
- `-config` в CLI — только `--config` (cobra)
- Жёсткая зависимость integration-тестов от локального `.env`

## Примеры

### Integration: skip если Kafka недоступен

```go
func requireBroker(t *testing.T) string {
    t.Helper()
    addr := integ.KafkaBroker()
    conn, err := kafka.Dial("tcp", addr)
    if err != nil {
        t.Skipf("kafka недоступен (%v); make compose-up", err)
    }
    _ = conn.Close()
    return addr
}
```

### testify

```go
require.NoError(t, err)
require.Eventually(t, func() bool {
    return len(events) > 0
}, 10*time.Second, 200*time.Millisecond)
assert.Equal(t, "demo", ev.Topic)
```

## Ссылки в репо

- `internal/middleware/*_test.go` — образец testify + `t.Run`
- `internal/agent/source/kafka/kafka_integration_test.go` — integration Kafka
- `test/e2e/` — сквозные сценарии
- `docs/TZ.md` § тестирование — roadmap покрытия
