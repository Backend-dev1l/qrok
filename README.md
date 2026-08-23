# qrok

Туннель и реплеер для асинхронных событий — «ngrok для брокеров».

Агент на стейджинге читает топики Kafka/RabbitMQ (собственной consumer group, не мешая боевым консьюмерам) и прокидывает события на localhost разработчика. Каждое событие сохраняется в облаке и может быть переотправлено кнопкой Replay — без повторной генерации сообщений и без риска «сжечь» их в брокере.

## Структура

- `cmd/qrok` — CLI: `qrok agent` (стейджинг), `qrok listen` (локальная машина), `qrok login`, `qrok replay`
- `cmd` — точка входа облачного сервера (`internal/server`)
- `internal/server` — Tunnel Gateway + Control Plane (запуск)
- `internal/middleware` — HTTP middleware (auth, security, cors, …)
- `internal/controlplane/infrastructure/models` — доменные типы control plane (Subject, Event, Delivery, …)
- `internal/controlplane/service` — use cases: auth, device, events, replay, delivery
- `internal/controlplane/infrastructure` — persistence: auth, eventstore, delivery (Postgres/S3)
- `internal/transport/http` — REST API и дашборд (пакет `httpapi`)
- `internal/transport/grpc` — Tunnel Gateway gRPC (пакет `gateway`)
- `pkg/fault` — единый пакет ошибок (HTTP/gRPC/CLI/slog-рендеры)
- `pkg/logger` — настройка slog (формат, уровень, логгер в context)
- `pkg/postgres` — подключение к PostgreSQL через pgxpool
- `proto/` — protobuf-контракт туннеля (buf)
- `migrations/` — goose SQL-миграции
- `deploy/` — docker-compose для локальной разработки

## Быстрый старт (dev)

```bash
make tools        # установить buf, goose, golangci-lint, gremlins
make compose-up   # Kafka + Postgres + Redis + MinIO
make migrate-up   # накатить миграции
make seed-dev     # org/project/tunnel/agent_token для локальных прогонов
make run-server   # HTTP :8080, gRPC :9090
make run-listen   # dev-клиент → localhost (в другом терминале)
make run-agent    # агент → Kafka (на стейджинге / с compose)
```

Перед `run-listen` без `gateway.allow_insecure_listen`: `qrok login --api http://127.0.0.1:8080 --project <project_id из seed-dev>`.

Дашборд (MVP): http://127.0.0.1:8080/dashboard/ — лента событий и Replay.  
**WSL2 + браузер Windows:** если `127.0.0.1` не открывается, используй IP из `hostname -I` (в логе сервера — `dashboard_wsl`).  
Для dev включите `http.allow_insecure_api: true` в конфиге сервера.

```bash
make build        # бинари в ./bin
make test         # unit-тесты
make verify       # сборка + тесты (после рефакторинга)
```

Остальные таргеты: `make help`.

## Agent skills

Скиллы лежат в **`.claude/skills/`** (канон). Cursor подхватывает копию из `.cursor/skills/` после `make sync-skills`.

| Skill                      | Назначение                                                               |
| -------------------------- | ------------------------------------------------------------------------ |
| `go-backend-microservices` | архитектура Go-микросервисов                                             |
| `golang-pro`               | идиоматичный Go ([upstream](https://github.com/Jeffallan/claude-skills)) |
| `go-testing`               | тесты qrok (testify, integration, e2e)                                   |
| `kafka-development`        | Kafka / agent source                                                     |
| `rabbitmq-development`     | RabbitMQ (этап 2)                                                        |

В чате: `/go-testing`, `/golang-pro`, `/kafka-development`, …

## Переменные окружения

Приоритет конфига сервера: дефолты в коде → YAML (`deploy/server.example.yaml` → свой `qrok.yaml`) → env `QROK_*` → флаги.

Для локальной разработки: `cp .env.example .env` — файл подхватывается и docker-compose'ом (креды Postgres/MinIO), и всеми make-таргетами (goose, тесты, запуск сервера). `.env` в git не попадает.

Секрет-менеджер (Doppler и т.п.) подключается без правок кода, когда появится стейджинг: конфиг читает обычные env-переменные, поэтому достаточно `doppler run -- make run-server`.

Для локального compose примеры agent/listen явно используют `tls: false`. Для внешнего gateway TLS завершается на reverse proxy/LB; в agent/listen включите `tls: true` и при необходимости задайте `tls_server_name`/`tls_ca_file`. Kafka поддерживает TLS и SASL PLAIN/SCRAM через секцию `kafka`.

При `http.allow_insecure_api: false` dashboard принимает project/dev token в поле Access token. Подтверждение `qrok login` также требует токен существующего проекта; новый dev token затем подходит и для gRPC listen, и для REST/replay.

## Тестирование

- `make test` — unit
- `make test-integration` — интеграционные (нужен `make compose-up`)
- `make test-e2e` — функциональные, сквозной путь брокер → localhost
- `make mutation` — мутационные (gremlins) на критичных пакетах
