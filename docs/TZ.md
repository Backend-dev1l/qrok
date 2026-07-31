# ТЗ: qrok — Event Tunnel & Replayer («Ngrok для брокеров»)

Продукт: **qrok**. CLI-команда: `qrok`.

---

## 1. Суть продукта

SaaS + CLI-утилита на Go, которая решает боль отладки событийных (event-driven) систем:

- **Туннель**: агент ставится на стейджинг/дев-сервер, подписывается на топики Kafka/RabbitMQ и безопасно прокидывает события на localhost разработчика.
- **Реплей**: каждое пролетевшее событие сохраняется в облаке; из дашборда его можно переотправить на локальный порт сколько угодно раз — не трогая реальный брокер и не «сжигая» сообщения.

### 1.1 Ключевые сценарии (user stories)

| #   | Сценарий                                                                                                             |
| --- | -------------------------------------------------------------------------------------------------------------------- |
| U1  | Разработчик ставит агент на стейджинг одной командой, указывает брокер и топики — события начинают попадать в облако |
| U2  | Разработчик запускает dev-CLI локально — события со стейджа доставляются на его `localhost:PORT` в реальном времени  |
| U3  | В дашборде видна история событий: payload, headers, key, topic, partition/offset, время                              |
| U4  | Кнопка **Replay** — событие повторно улетает на localhost (или выбранному участнику команды)                         |
| U5  | Фильтры при подписке: по топику, по header'ам, по JSON-path в payload                                                |
| U6  | Консьюмер разработчика упал — событие не потеряно, оно в облаке, replay доступен всегда                              |

### 1.2 Что НЕ входит в MVP

- Проксирование обратных ответов (продукт — one-way delivery, не RPC).
- Гарантия exactly-once на доставке в localhost (даём at-least-once + идемпотентный replay).
- Поддержка всех брокеров: MVP = **Kafka**, вторым этапом RabbitMQ, дальше NATS/SQS/Pub/Sub через интерфейс `Source`.

---

## 2. Технологический стек (зафиксировано)

| Область              | Выбор                                                                                                                                                                                                                                                                                                                                                    |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Язык                 | Go (1.25+)                                                                                                                                                                                                                                                                                                                                               |
| Kafka-клиент         | `segmentio/kafka-go`                                                                                                                                                                                                                                                                                                                                     |
| HTTP-роутер          | `go-chi/chi`                                                                                                                                                                                                                                                                                                                                             |
| gRPC / protobuf      | `buf` (генерация, lint, breaking-check) + `google.golang.org/grpc`                                                                                                                                                                                                                                                                                       |
| Миграции             | `pressly/goose` (SQL-миграции)                                                                                                                                                                                                                                                                                                                           |
| PostgreSQL-драйвер   | `jackc/pgx` + `pgxpool`                                                                                                                                                                                                                                                                                                                                  |
| Кэш (in-process)     | `maypok86/otter`                                                                                                                                                                                                                                                                                                                                         |
| TUI                  | `charmbracelet/bubbletea` + `bubbles` + `lipgloss` — де-факто стандарт TUI в Go, компонентная модель (Elm-архитектура) идеально ложится на живую ленту событий с фильтрами и replay по хоткею                                                                                                                                                            |
| Криптография         | `golang.org/x/crypto/argon2` — argon2id для всего, где секрет придумал человек (пароли пользователей); случайные высокоэнтропийные секреты (агентские/API-токены, 256 бит) хранятся как SHA-256 — argon2 там не даёт защиты (перебор невозможен из-за энтропии), а детерминированный хеш обязателен для lookup по уникальному индексу и ключа кэша otter |
| Логирование          | `log/slog` (stdlib): JSON-handler в проде, текстовый в dev                                                                                                                                                                                                                                                                                               |
| Ошибки               | собственный пакет `pkg/fault` (раздел 8)                                                                                                                                                                                                                                                                                                                 |
| Трейсинг             | OpenTelemetry (`otel/trace`), trace_id в ответах ошибок                                                                                                                                                                                                                                                                                                  |
| Интеграционные тесты | `deploy/docker-compose.yml` (Kafka, Postgres, Redis, MinIO) + build-теги `integration` / `e2e`; **без** testcontainers-go (см. `.claude/skills/go-testing/`)                                                                                                                                                                                             |
| Мутационные тесты    | `go-mutesting` (форк avito-tech)                                                                                                                                                                                                                                                                                                                         |
| Object Store         | S3-совместимый (MinIO в dev)                                                                                                                                                                                                                                                                                                                             |

Куда идёт otter: горячие чтения control plane — валидация агентских/API-токенов (hash → tenant/scope), метаданные проектов и туннелей, конфиги подписок. Инвалидация по TTL (короткий, 30–60 с) + явный сброс при mutate-операциях.

---

## 3. Терминология

| Термин             | Значение                                                                                |
| ------------------ | --------------------------------------------------------------------------------------- |
| **Agent**          | CLI-процесс на стейджинге: читает брокер, шлёт события в облако                         |
| **Dev CLI**        | CLI-процесс на машине разработчика: получает события из облака, доставляет на localhost |
| **Tunnel Gateway** | Облачный сервер, держащий постоянные соединения с Agent'ами и Dev CLI                   |
| **Control Plane**  | REST API + дашборд: auth, проекты, история событий, replay                              |
| **Event Store**    | Хранилище событий (метаданные + payload)                                                |
| **Tunnel**         | Логический канал: `источник (broker/topics) → подписчики (dev-машины)`                  |
| **Delivery**       | Одна попытка доставки события на конкретный localhost                                   |

---

## 4. Архитектура (высокий уровень)

Выбранный стиль: **модульный монолит для control plane + отдельно масштабируемый stateless Tunnel Gateway**. Микросервисы на старте не нужны — но границы модулей проектируем так, чтобы каждый модуль позже выносился в отдельный сервис без переписывания (общение между модулями только через интерфейсы, свои таблицы у каждого модуля).

```
СТЕЙДЖИНГ                         ОБЛАКО (SaaS)                          ЛОКАЛЬНАЯ МАШИНА
┌─────────────┐                ┌──────────────────────────────┐        ┌──────────────────┐
│ Kafka /     │                │  ┌────────────────────────┐  │        │                  │
│ RabbitMQ    │◄── consume ────┼──│    Tunnel Gateway      │──┼─ push ─►│    Dev CLI      │
└─────────────┘   (отдельная   │  │ (gRPC bidi-stream, N   │  │ (bidi- │        │         │
      ▲            consumer     │  │  инстансов за LB)      │  │ stream)│        ▼         │
      │            group)       │  └───────────┬────────────┘  │        │ localhost:8080   │
┌─────┴───────┐                │              │ publish        │        │ (HTTP-webhook или│
│   Agent     │── events ──────┼──►┌──────────▼───────────┐   │        │  локальный брокер)│
│  (Go CLI)   │  (bidi-stream) │   │  Internal Event Bus  │   │        └──────────────────┘
└─────────────┘                │   │  (очередь ingest'а)  │   │
                               │   └──────────┬───────────┘   │
                               │              ▼               │
                               │   ┌──────────────────────┐   │
                               │   │    Control Plane     │   │
                               │   │ API / Auth / Replay  │◄──┼──── Dashboard (SPA)
                               │   └──────────┬───────────┘   │
                               │       ┌──────┴──────┐        │
                               │       ▼             ▼        │
                               │  PostgreSQL    Object Store  │
                               │  (метаданные)  (payload'ы)   │
                               │       Redis (routing/presence)│
                               └──────────────────────────────┘
```

### 4.1 Почему так

- **Оба соединения исходящие (outbound)**: и Agent, и Dev CLI сами подключаются к облаку. Не нужно открывать порты/NAT — это же свойство сделало ngrok удобным.
- **Gateway отделён от Control Plane**, потому что у них разный профиль нагрузки: gateway держит тысячи долгоживущих соединений (важны дескрипторы, память, graceful drain при деплое), control plane — обычный request/response. Масштабируются независимо.
- **Внутренняя очередь между Gateway и dev-клиентами**: `EventBus` (MVP: `internal/bus/inproc`) фан-аутит события на `ListenStream` и replay. **Запись в Event Store** для live-событий от агента — **синхронно в `AgentStream` до ack** (см. §6): агент не получает ack, пока событие не durable в Postgres/S3. Replay-публикации идут только в шину (событие уже в store).

---

## 5. Компоненты подробно

### 5.1 Agent (CLI на стейджинге)

**Обязанности:**

1. Подключение к брокеру через интерфейс `Source`:

```go
// internal/agent/source/kafka/kafka.go — интерфейс рядом с реализацией (см. .cursor/rules/interfaces-at-impl.mdc)
type Source interface {
    Subscribe(ctx context.Context, topics []string, out chan<- *source.Event) error
    Ack(ctx context.Context, ev *source.Event) error
    Close() error
}
// Общий тип Event — internal/agent/source/event.go
```

Реализации: `source/kafka` (на `segmentio/kafka-go`), `source/rabbitmq`, позже другие.

2. **Критично: агент никогда не «ворует» сообщения у боевых консьюмеров.**
   - Kafka: агент читает через **собственную consumer group** (`qrok-agent-<tunnel_id>`), коммитит только свои оффсеты. Боевые консьюмеры не затронуты вообще.
   - RabbitMQ: агент декларирует **свою очередь**, привязанную к тому же exchange (для fanout/topic). Для прямых очередей — режим «shovel-copy» через дополнительный binding; прямое чтение чужой очереди запрещаем по умолчанию (destructive) и разрешаем только явным флагом `--dangerous-steal`.
3. Отправка событий в Gateway по одному постоянному bidi-стриму, с локальным буфером (bounded, backpressure) и ретраями.
4. **Оффсет коммитится только после ack от облака** — событие не может потеряться между брокером и облаком.
5. Правила безопасности данных: маскирование полей по конфигу (`redact: ["payload.card_number"]`), лимит размера payload (по умолчанию 1 МБ, хвост — в object store через presigned upload).
6. Конфиг: YAML-файл + флаги + env (приоритет: флаги > env > файл). Один токен агента = один проект.
7. Самодиагностика: `qrok agent doctor` — проверка доступности брокера, облака, прав.

**Устойчивость:** реконнект с экспоненциальным backoff + jitter; при недоступности облака события не ack'аются в брокере и будут перечитаны (at-least-once).

#### 5.1.1 Внедрение на стороне клиента (deployment)

Агент — **отдельный процесс** (`qrok agent start`), не SDK в коде приложения. Разработчик сервиса **не меняет** producer/consumer: агент читает топики параллельно через свою consumer group.

| Способ                                             | Когда                                                                                                                                      |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **systemd / отдельный Deployment** (рекомендуется) | default: один агент на окружение или набор топиков                                                                                         |
| **Helm chart / Docker-образ**                      | K8s-стейджинг: `preStop` → graceful stop                                                                                                   |
| **Sidecar в pod**                                  | только если политика безопасности требует colocation; архитектурно то же самое — агент ходит в **кластер брокеров**, не в сокет приложения |

Исходящий gRPC к облаку должен быть настроен с **keepalive** (§5.3.2) — иначе NAT на стейджинге оборвёт idle `AgentStream`.

Облако **никогда** не получает Kafka-credentials клиента (§7) — агент единственная точка входа/выхода в периметр клиента. Очистка consumer groups и любые операции с брокером — **только на стороне агента**.

#### 5.1.2 Consumer group: lifecycle и cleanup

**Имя группы:** `qrok-agent-<tunnel_id>` (или явный `kafka.group_id` в конфиге). Одна **стабильная** группа на весь жизненный цикл туннеля — **не** на каждую сессию `qrok listen`.

| Событие                                                | Поведение consumer group                                                                                                                       |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Dev закрыл `qrok listen`                               | **Ничего** — группа не трогается; агент продолжает читать Kafka и писать в Event Store (replay)                                                |
| Агент перезапустили                                    | Та же группа, чтение с последнего закоммиченного offset                                                                                        |
| Агент остановили штатно (`SIGTERM`, `qrok agent stop`) | `reader.Close()` → опционально `DeleteConsumerGroups` (см. §5.1.4)                                                                             |
| Туннель удалили / отозвали agent token                 | Control plane шлёт `RevokeTunnel` агенту (§5.3.1) → graceful stop → delete group; если агент офлайн — revoked token не даст переподключиться   |
| Агент умер без cleanup                                 | Kafka сам удалит offset'ы пустой группы через `offsets.retention.minutes` (дефолт брокера, обычно 7 дней) — **страховка**, не primary strategy |

**Что реально занимает место в Kafka:** committed offsets группы — килобайты метаданных в compacted-топике `__consumer_offsets`, а не копия сообщений. Объём диска задаёт `retention.ms` / `retention.bytes` **топиков**, не consumer groups. Тем не менее hygiene важен при сотнях заброшенных туннелей — явный delete при shutdown.

**Не делать:** привязывать delete группы к disconnect Dev CLI — несколько слушателей, агент 24/7, потеря continuity offset'ов.

#### 5.1.3 Pipeline, backpressure и таймауты Kafka

MVP реализует последовательный цикл `read → send → wait ack → commit`. **Этап 2:** развязать чтение и отправку:

```
[Kafka reader] → bounded chan (N) → [stream sender + ack waiter] → commit
                      ↑
         pause/resume по AgentControl и backpressure
```

- Reader и sender — **отдельные goroutine**; reader продолжает `FetchMessage`, пока chan не заполнен (иначе риск `max.poll.interval.ms` — брокер выкинет consumer из группы при долгом ожидании ack облака).
- Буфер **только bounded** (`event_buffer` в конфиге); unbounded RAM запрещён.
- gRPC **keepalive** — см. §5.3.2 (обязательно для долгоживущих стримов за NAT).

**Политика backpressure** (`agent.backpressure`, этап 2):

| Режим                 | Поведение                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------ |
| `block` (default)     | chan full → reader stop → естественный backpressure в Kafka                                                  |
| `pause_on_slow_cloud` | N неacked подряд / ack timeout → локальная пауза чтения; resume по `AgentControl` или при восстановлении ack |
| `drop`                | только с явным флагом — потеря истории для replay; не для prod                                               |

**Дубли при реконнекте** (at-least-once неизбежен):

| Слой          | Защита                                                                              |
| ------------- | ----------------------------------------------------------------------------------- |
| Облако        | dedup по `event_id` в `eventstore.Insert` — повторный ingest без повторного fan-out |
| Dev CLI       | фильтр по `X-Qrok-Event-Id` / LRU последних N id (этап 2)                           |
| Локальный код | идемпотентный обработчик — ответственность разработчика                             |

Рекомендуемый тюнинг Kafka на стейджинге (документация для ops): `max.poll.interval.ms` ≥ worst-case latency облака (например 5–10 мин на staging).

#### 5.1.4 Graceful shutdown

Цепочка при `SIGTERM` / `qrok agent stop` (порядок с отдельными дедлайнами):

1. Перестать читать новые сообщения из Kafka (cancel reader / закрыть `Source`).
2. Drain bounded-очереди: дождаться ack по уже отправленным событиям (с общим deadline, например 25 с из `terminationGracePeriodSeconds`).
3. Закрыть gRPC `AgentStream`.
4. **Опционально** удалить consumer group через Kafka Admin API `DeleteConsumerGroups` — **обязательно** обернуть в `context.WithTimeout` (**3–5 с**). Если брокер недоступен — **не блокировать** exit процесса (иначе K8s убьёт pod по `SIGKILL`).
5. Флаг `agent.stop_keep_group: true` / `qrok agent stop --keep-group` — только отписаться от координатора, **сохранить** offset'ы для следующего запуска.

K8s: `preStop` hook → `qrok agent stop`; `terminationGracePeriodSeconds` ≥ сумма дедлайнов shutdown.

**Статус:** delete group и развязанный pipeline — ⏳ этап 2; сейчас `Close()` только закрывает reader без `DeleteConsumerGroups`.

#### 5.1.5 Режим паузы (`pause_when_no_listeners`, этап 2)

Конфиг `agent.pause_when_no_listeners: true` — экономия трафика на высоконагруженных стейджингах:

| Режим ingest                       | Когда использовать                                              |
| ---------------------------------- | --------------------------------------------------------------- |
| **Всегда пишем историю** (default) | нужен replay событий, пока dev офлайн                           |
| **Пауза без слушателей**           | только live-debug; не копить Event Store, пока никто не слушает |

Механизм: Gateway при connect/disconnect `ListenStream` шлёт агенту `AgentControl` (§5.3.1) — **push по bidi-стриму**, не polling. При паузе: `reader.Close()`, группу **не удаляем** (сохраняем offset). При resume — тот же `qrok-agent-<tunnel_id>`.

Trade-off: пока пауза активна, live-события **не попадают** в облако; replay только из уже сохранённых.

### 5.2 Dev CLI (машина разработчика)

**Обязанности:**

1. `qrok listen --tunnel <name> --forward http://localhost:8080/events` — подключается к Gateway, объявляет подписку (topics + фильтры).
2. **Режимы доставки на localhost** (интерфейс `Sink`):
   - `http` (MVP): POST на локальный URL; тело = payload, метаданные события — в HTTP-заголовках (`X-Qrok-Topic`, `X-Qrok-Key`, `X-Qrok-Offset`, `X-Qrok-Event-Id`, `X-Qrok-Replay: true/false`).
   - `kafka-local` (этап 2): продюсит в локальный брокер разработчика — код консьюмера вообще не меняется, это killer-feature.
   - `stdout`: печать событий для быстрого дебага.
3. Ответ localhost'а (2xx/5xx, тело) отправляется обратно в облако как **DeliveryResult** — в дашборде видно, упал ли локальный код и с чем.
4. Интерактивный TUI-режим на bubbletea (этап 2): живая лента событий (viewport + table из `bubbles`), фильтр по топику, просмотр payload с подсветкой JSON, replay по хоткею `r`.

### 5.3 Tunnel Gateway

- Протокол: **gRPC bidirectional streaming поверх TLS** (HTTP/2). Контракт — protobuf, тулинг `buf` (lint + breaking-check в CI). Один стрим на агента, один на dev-клиента. Fallback на WebSocket — только если появятся жалобы на корпоративные прокси (закладываем в абстракцию `transport`, не реализуем в MVP).
- Полностью **stateless по данным**: всё состояние соединений — в памяти, маршрутизация — через Redis:
  - `presence:agent:{tunnel_id} → gateway_instance_id` (TTL + heartbeat);
  - `presence:listen:{client_id} → gateway_instance_id` (TTL + heartbeat);
  - fan-out события подписчику на другом инстансе — через Redis Pub/Sub (MVP) → выделенный внутренний брокер (при росте).
- Ответственности: аутентификация стрима по токену, ack/flow-control, роутинг событие→подписчики, публикация во внутреннюю шину для персистенса, **доставка control-команд агенту** (этап 2).
- Graceful drain: при деплое инстанс перестаёт принимать новые соединения, живые стримы получают `Goaway` и переподключаются к другому инстансу.

#### 5.3.1 Управляющие команды агенту (control plane → agent)

Команды (пауза ingest, revoke туннеля, смена presence слушателей) идут **по тому же `AgentStream`**, что и события — **исходящий bidi gRPC от агента**, NAT не пробиваем. **Polling не используется** как primary transport (только возможный fallback за корпоративным прокси через абстракцию `transport`).

**Маршрутизация при нескольких gateway-инстансах:**

1. Агент подключается → gateway пишет `presence:agent:{tunnel_id}` в Redis.
2. Control plane (например `DELETE /api/v1/tunnels/{id}`) → revoke token + publish в Redis Pub/Sub канал `gw:{instance_id}`.
3. Gateway-инстанс, держащий стрим, шлёт `AgentStreamResponse{control: ...}`.
4. Агент офлайн → команда теряется; **revoked token** блокирует переподключение; cleanup группы — при следующем `agent stop` или через `offsets.retention.minutes`.

Облако **не имеет** доступа к Kafka клиента для delete групп — только сигнал агенту (§7).

**Расширение protobuf** (`proto/qrok/v1/tunnel.proto`, этап 2):

```protobuf
message AgentStreamResponse {
  oneof msg {
    Ack ack = 1;
    Goaway goaway = 2;
    AgentControl control = 3;
  }
}

message AgentControl {
  oneof cmd {
    PauseConsumption pause = 1;
    ResumeConsumption resume = 2;
    RevokeTunnel revoke = 3;       // graceful stop + delete group
    ListenerPresence presence = 4; // listeners_count для pause_when_no_listeners
  }
}
```

`Goaway` — уже в контракте (graceful drain при деплое gateway); **статус:** ⏳ не шлётся из gateway в MVP.

#### 5.3.2 gRPC Keepalive (долгоживущие стримы за NAT)

Агент и Dev CLI держат **постоянный исходящий** bidi-стрим. В простое (нет событий, `pause_when_no_listeners`, ночь на стейджинге) HTTP/2-соединение выглядит idle — корпоративные NAT, stateful firewall и L7-прокси **молча обрывают** такие сессии (типичный idle timeout 30–120 с). Без keepalive-пингов туннель «отваливается» без ошибки на стороне приложения; агент обнаруживает обрыв только при следующей отправке или по таймауту чтения.

**Обязательно на обеих сторонах** (симметричная настройка client + server):

| Сторона                      | API (`google.golang.org/grpc`)                                                | Назначение                                     |
| ---------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------- |
| **Agent / Dev CLI** (client) | `grpc.WithKeepaliveParams(keepalive.ClientParameters{...})` в `Dial`          | периодические HTTP/2 PING, пока стрим открыт   |
| **Gateway** (server)         | `grpc.KeepaliveParams` + `grpc.KeepaliveEnforcementPolicy` в `grpc.NewServer` | принимать client pings; не рвать долгие стримы |

**Рекомендуемые дефолты** (конфигурируемые, единые для agent/listen/gateway):

```go
// client (agent, dev-cli) — internal/tunnel/grpcutil/dial.go
keepalive.ClientParameters{
    Time:                30 * time.Second, // ping, если нет активности на соединении
    Timeout:             10 * time.Second, // ждать PINGACK
    PermitWithoutStream: true,             // важно для bidi: стрим есть, данных нет
}

// server (gateway)
keepalive.ServerParameters{
    Time:    30 * time.Second,
    Timeout: 10 * time.Second,
}
keepalive.EnforcementPolicy{
    MinTime:             20 * time.Second, // не считать client ping спамом
    PermitWithoutStream: true,
}
```

Правила:

- `PermitWithoutStream: true` — **критично** для bidi `AgentStream` / `ListenStream`: иначе gRPC не шлёт pings в фазе простоя.
- Интервал ping (**30 с**) должен быть **меньше** idle-timeout NAT/LB у клиента (часто 60–120 с); при жалобах из корпоративных сетей — уменьшать до 15 с через конфиг.
- Keepalive **не заменяет** application-level heartbeat (`presence` в Redis, §5.3) — это разные уровни: TCP/HTTP2 vs маршрутизация между gateway-инстансами.
- При обрыве по keepalive — штатный реконнект агента/Dev CLI (backoff, §5.1); Kafka offset не коммитится для неacked событий.

**Статус:** ⏳ не настроено в `grpcutil.Dial` / gateway server (MVP); приоритет раннего hardening (этап 2), до production за NAT.

**Протокол сообщений (envelope), protobuf (`proto/qrok/v1`):**

```protobuf
message EventEnvelope {
  string event_id      = 1;  // ULID, генерирует агент — ключ идемпотентности
  string tunnel_id     = 2;
  string source_type   = 3;  // kafka | rabbitmq
  string topic         = 4;
  bytes  key           = 5;
  map<string,string> headers = 6;
  bytes  payload       = 7;  // если > лимита — payload_ref вместо payload
  string payload_ref   = 8;  // ссылка на object store
  int32  partition     = 9;
  int64  offset        = 10;
  int64  broker_ts_ms  = 11;
  bool   is_replay     = 12;
}

message Ack { string event_id = 1; }

message DeliveryResult {
  string event_id    = 1;
  string delivery_id = 2;
  int32  status_code = 3;  // HTTP-код localhost'а или 0
  string error       = 4;
  int64  latency_ms  = 5;
}
```

### 5.4 Control Plane (API + модули)

Модули монолита (каждый — отдельный пакет со своим интерфейсом и своими таблицами):

| Модуль             | Ответственность                                                  |
| ------------------ | ---------------------------------------------------------------- |
| `auth`             | пользователи, организации, JWT-сессии, API-ключи, токены агентов |
| `project`          | проекты, туннели, участники, роли (owner/dev/viewer)             |
| `eventstore`       | запись/чтение событий, retention, поиск                          |
| `replay`           | постановка replay-задач, статусы доставок                        |
| `billing` (этап 3) | тарифы, лимиты (события/мес, retention, seats)                   |
| `notify` (этап 3)  | алерты (событие не доставлено N раз и т.п.)                      |

API: REST (JSON) на `chi` для дашборда + публичный API для CI/автоматизации. Версионирование с первого дня: `/api/v1/...`.

**Middleware-стек httpapi** (`internal/middleware/`, порядок в `controlplane/http/router.go`):

| #   | Middleware       | Статус | Назначение                                         |
| --- | ---------------- | ------ | -------------------------------------------------- |
| 1   | `RequestID`      | ✅     | `X-Request-Id`                                     |
| 2   | `RealIP`         | ✅     | IP за LB                                           |
| 3   | CORS             | ✅     | allowlist origin'ов                                |
| 4   | Security headers | ✅     | CSP, nosniff, frame deny                           |
| 5   | `BodyLimit`      | ✅     | лимит тела запроса                                 |
| 6   | `RequestLogger`  | ✅     | duration каждого запроса; slow warn > 500ms          |
| 7   | `Recoverer`      | ✅     | panic → `fault.ErrInternal`                        |
| 8   | `Timeout`        | ✅     | дедлайн запроса                                    |
| 9   | `Auth`           | ✅     | Bearer agent token; dev: `http.allow_insecure_api` |
| 10  | OTel-трейсинг    | ⏳     | span на запрос                                     |
| 11  | Rate limit       | ⏳     | Redis token bucket                                 |
| 12  | Compress         | ⏳     | gzip ответов                                       |

Ошибки из хендлеров всегда уходят через `fault.WriteHTTPError` (раздел 8) — единый формат JSON с `trace_id`.

**Replay-флоу:** дашборд → `POST /api/v1/events/{id}/replay {target: dev_client_id}` → модуль replay создаёт `delivery` со статусом `pending` → находит gateway-инстанс подписчика через Redis presence → пушит envelope с `is_replay=true` → DeliveryResult закрывает delivery. Если разработчик офлайн — delivery ставится в очередь и доедет при подключении (флаг `--catch-up`).

### 5.5 Хранение данных

| Хранилище                                      | Что хранит                                                                              | Почему                                            |
| ---------------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **PostgreSQL** (доступ через `pgxpool`)        | users, orgs, projects, tunnels, tokens, events-метаданные, payload < 256 КБ, deliveries | реляционка, транзакции, всем понятна              |
| **Object Store (S3-совместимый, MinIO в dev)** | payload'ы ≥ 256 КБ                                                                      | дешёвый объём, retention через lifecycle-политики |
| **Redis**                                      | presence/routing, rate-limit счётчики, pub/sub фан-аут                                  | эфемерные данные, скорость                        |
| **otter (in-process)**                         | токены, метаданные проектов/туннелей                                                    | снятие горячих чтений с Postgres                  |
| **ClickHouse** (этап 3)                        | аналитика/поиск по millions of events                                                   | когда Postgres-поиск перестанет вывозить          |

**Payload — гибридная стратегия (threshold-based routing), порог 256 КБ:**

- `payload_size < 256 КБ` → колонка `payload BYTEA` в Postgres (читается одним запросом вместе с метаданными, дёшево для 95% событий);
- `payload_size ≥ 256 КБ` → объект в S3, в Postgres только `payload_ref` (ключ объекта) + `payload_size`;
- инвариант: заполнено ровно одно из `payload` / `payload_ref` (CHECK-констрейнт);
- порог — конфиг сервера, чтобы можно было подкрутить без миграции;
- чтение — за единым интерфейсом `PayloadStore.Get(ctx, ev) ([]byte, error)`, вызывающий код о стратегии не знает.

Таблица `events` в Postgres — **партиционирование по времени с первого дня** (`PARTITION BY RANGE (created_at)`, помесячно): retention = drop partition, дёшево и быстро. Миграции — `goose` (SQL-файлы в `migrations/`).

**Схема ключевых таблиц (эскиз):**

```sql
events (
  id            text PK,          -- ULID
  tunnel_id     text NOT NULL,
  topic         text NOT NULL,
  partition     int,
  broker_offset bigint,
  key           bytea,
  headers       jsonb,
  payload       bytea,            -- < 256 КБ, иначе NULL
  payload_ref   text,             -- ключ в S3, иначе NULL
  payload_size  int NOT NULL,
  broker_ts     timestamptz,
  created_at    timestamptz NOT NULL,
  CHECK ((payload IS NULL) <> (payload_ref IS NULL))
) PARTITION BY RANGE (created_at);

deliveries (
  id          text PK,
  event_id    text NOT NULL,
  target_id   text NOT NULL,     -- dev-клиент
  kind        text NOT NULL,     -- live | replay
  status      text NOT NULL,     -- pending | delivered | failed | expired
  status_code int,
  error       text,
  latency_ms  int,
  created_at  timestamptz NOT NULL
);
```

---

## 6. Семантика доставки и гарантии

| Участок                 | Гарантия                        | Механизм                                                                                                        |
| ----------------------- | ------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Брокер → Agent → Облако | at-least-once                   | коммит оффсета только после ack облака                                                                          |
| Облако → Event Store    | durable                         | gateway `Insert` в Postgres/S3 **до** gRPC-ack агенту; затем fan-out в `EventBus` (только для новых `event_id`) |
| Облако → Dev CLI        | at-least-once, best-effort live | live-события при офлайне подписчика не копятся бесконечно (окно catch-up); replay — всегда явный и надёжный     |
| Дедупликация            | по `event_id` (ULID)            | уникальный индекс + upsert-ignore; повторный ingest после реконнекта агента не дублирует fan-out на Dev CLI     |
| Дубли на Dev CLI        | at-least-once                   | тот же `event_id` может прийти повторно; Dev CLI фильтрует (этап 2, §5.1.3)                                     |

Порядок: внутри одной партиции Kafka порядок сохраняется до localhost (один стрим, FIFO). Между партициями порядок не гарантируем (как и сама Kafka).

**Разделение lifecycle:** закрытие `qrok listen` влияет только на live-доставку Dev CLI (§5.2); consumer group Kafka и ingest в Event Store управляются **агентом** независимо (§5.1.2).

---

## 7. Безопасность

1. **Транспорт**: TLS везде; агент/CLI пиннят сертификат облака (опционально).
2. **Токены**: агентские токены — scoped на проект, отзываемые, хранятся как hash (показываем один раз). Формат `qrok_agt_<256 бит случайности>`, в БД — SHA-256 (детерминированный lookup, см. раздел 2). Dev-CLI — OAuth device flow через дашборд (`qrok login`).
   **Пароли пользователей** — argon2id (`golang.org/x/crypto/argon2`), параметры по рекомендации OWASP (19 МиБ / t=2 / p=1), зашиты в PHC-строку хеша — их можно ужесточать, не ломая проверку старых паролей. Реализация: `internal/controlplane/auth`.
3. **Данные**: payload шифруем at rest (SSE object store; для Postgres — на уровне диска/кластера). Маскирование полей на стороне агента (данные с маской вообще не покидают периметр клиента).
4. **Тенантность**: каждый запрос — через `tenant_id` в контексте; ни один SQL без фильтра по tenant (закрепить линтером/код-ревью, RLS в Postgres как страховка — этап 2).
5. **Rate limiting**: на токен и на tenant (Redis token bucket), лимиты события/сек по тарифу.
6. Секреты брокера (SASL-пароли) живут только в конфиге агента у клиента — облако их **никогда не получает**. Control plane **не подключается** к Kafka/RabbitMQ клиента: ни для чтения, ни для удаления consumer groups. Это сокращает Security Review (zero-trust: агент — единственная точка входа в периметр клиента). Очистка групп, пауза чтения, revoke — только через команды агенту по `AgentStream` (§5.3.1).
7. HTTP-периметр — middleware-стек из раздела 5.4 (CORS, security headers, timeout, body limit, panic recovery).

---

## 8. Пакет ошибок `pkg/fault`

Единый пакет для всех бинарей (agent, dev-cli, server). За основу взят согласованный образец (`Code`-центричный API: `Code.New/Newf/Err`, `WithArg`, `WriteHTTPError`), адаптированный под проект:

**Что добавлено к образцу:**

1. **Wrapping**: `Code.Wrap(err, msg)` и поле `cause` + `Unwrap()` — полная совместимость с `errors.Is/As`, цепочка причин не теряется.
2. **Op** (`WithOp("agent.kafka.subscribe")`) — где возникла ошибка; цепочка op'ов вместо стек-трейса в проде.
3. **Hint** (`WithHint(...)`) — подсказка пользователю CLI, что делать.
4. **Коды под наш домен**: добавлены `RATE_LIMITED`, `TIMEOUT`; `FromError` распознаёт `context.DeadlineExceeded`/`context.Canceled`.
5. **gRPC-маппинг** `Code → codes.Code` (симметрично HTTP-маппингу) — для gateway.
6. **slog-интеграция**: `LogAttrs(err) []slog.Attr` — код, op, args, полная цепочка причин для лога.
7. **CLI-рендер**: `RenderCLI(err)` — красивый вывод для терминала (цвет с уважением к `NO_COLOR`), формат:

```text
✗ не удалось подключиться к Kafka (SERVICE_UNAVAILABLE)

  операция:  agent.kafka.subscribe
  причина:   dial tcp 10.0.1.5:9092: connection refused
  подсказка: проверьте --brokers и что порт 9092 доступен с этой машины
```

8. **`IsRetryable(err)`** — `SERVICE_UNAVAILABLE`, `TIMEOUT`, `RATE_LIMITED` ретраятся (агент/CLI используют для backoff-логики).

**Формат HTTP-ответа** (как в образце, отдаётся `WriteHTTPError` из любого хендлера):

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "event not found",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "args": { "event_id": "01J..." }
  }
}
```

Правила использования: каждый слой оборачивает ошибку **один раз** со своим `Op`; `message` пишется на границе (там, где понятен пользовательский контекст); маппинг в HTTP/gRPC-статусы — только внутри пакета `fault`, хендлеры про статусы не знают.

Реализация: `pkg/fault/` (см. код), покрыта unit-тестами.

---

## 9. Тестирование

Пирамида, все уровни в CI:

| Уровень                  | Инструменты                                                                                                           | Что проверяем                                                                                                                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Unit**                 | stdlib `testing`, table-driven                                                                                        | `fault`, фильтры, конфиг, роутинг, PayloadStore-threshold; цель ≥ 80% по core-пакетам                                                                                                                         |
| **Интеграционные**       | `make compose-up` + build-tag `integration`                                                                           | Kafka `Source` (своя consumer group, ack→commit), `eventstore` поверх реального Postgres+goose-миграций, гибридный PayloadStore; graceful shutdown + `DeleteConsumerGroups` (этап 2); Redis presence — этап 2 |
| **Функциональные (e2e)** | docker-compose стек + собранные бинари, tag `e2e`                                                                     | сквозной путь: produce в Kafka → agent → gateway → dev CLI → локальный HTTP-стаб; replay через API; проверка headers `X-Qrok-*`                                                                               |
| **Контрактные**          | `buf lint` + `buf breaking` в CI                                                                                      | protobuf-контракт не ломается между версиями агента и облака                                                                                                                                                  |
| **Мутационные**          | `go-mutesting`, точечно на критичных пакетах (`fault`, `config`, ack/commit-логика агента, threshold-роутинг payload) | что тесты действительно ловят изменение логики, а не только исполняют код; ориентир — mutation score ≥ 0.75 без гонки за косметикой                                                                           |
| **Бенчмарки**            | `testing.B` + `-benchmem`, сравнение прогонов через `benchstat` (`make bench`)                                        | производительность горячих путей: otter-кэш, PayloadStore/тяжёлые SQL, сериализация envelope — добавляются вместе с самими компонентами (этапы 1–2), чтобы видеть регрессии при рефакторинге                  |
| **Нагрузочные** (этап 2) | k6 / собственный producer-генератор                                                                                   | латентность p99 брокер→localhost, деградация при 10k событий/с                                                                                                                                                |

Правила: интеграционные и e2e отделены build-тегами (`go test -tags integration ./...`), чтобы `go test ./...` оставался быстрым; каждый баг-фикс сопровождается регрессионным тестом; фикстуры событий — golden files в `testdata/`.

---

## 10. Масштабирование (заложено сейчас, включается потом)

| Точка роста             | Решение                                                                                                                    |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Много соединений        | Gateway stateless → добавляем инстансы за L4 LB; presence в Redis уже готов к этому                                        |
| Поток событий в persist | внутренняя шина: интерфейс `EventBus`, MVP-реализация in-process → замена на NATS/Kafka без правок вызывающего кода        |
| Поиск по событиям       | вынос в ClickHouse, у `eventstore` уже свой интерфейс чтения                                                               |
| Регионы                 | gateway-пулы per-region, control plane общий; event_id глобально уникальны (ULID)                                          |
| Выделение сервисов      | модули монолита общаются через интерфейсы и не лезут в чужие таблицы — вынос = замена реализации интерфейса на gRPC-клиент |
| Payload-объёмы          | гибридная стратегия с порогом 256 КБ уже в схеме; порог — конфиг                                                           |

Правило: **не строим распределённое сейчас — строим интерфейсы, за которыми распределённое появится потом.**

---

## 11. Структура репозитория

Монорепо (один go.mod, module `qrok`), два бинаря:

```
qrok/
├── cmd/
│   ├── qrok/          # CLI: agent / listen / login / replay (cobra)
│   ├── main.go        # облако: gateway + control plane → internal/server
│   └── seed/          # make seed-dev
├── pkg/
│   ├── fault/         # пакет ошибок (раздел 8)
│   ├── logger/        # slog (json/text)
│   ├── postgres/      # pgxpool
│   └── objectstore/   # S3/MinIO для крупных payload
├── internal/
│   ├── proto/         # buf generate
│   ├── server/        # Run(): сборка gateway + HTTP API
│   ├── middleware/    # HTTP middleware (auth, security, cors, …) — не в controlplane
│   ├── agent/         # run.go, stream/, source/kafka (Source + client)
│   ├── devcli/        # listen; sink/http (Sink + Client)
│   ├── gateway/       # gRPC AgentStream / ListenStream
│   ├── bus/inproc/    # EventBus + Bus (интерфейс рядом с реализацией)
│   ├── controlplane/
│   │   ├── http/      # chi REST + embed-дашборд (web/)
│   │   ├── service/   # replay
│   │   └── infrastructure/
│   │       ├── auth/      # пароли argon2id, токены SHA-256
│   │       ├── eventstore/ # гибрид Postgres/S3, event_dedup
│   │       └── postgres/  # deliveries
│   ├── testutil/integ/ # DSN/брокер для integration/e2e
│   └── config/
├── proto/
├── test/e2e/          # build-tag e2e
├── migrations/
├── deploy/              # docker-compose, example yaml
├── .claude/skills/      # agent skills (канон); .cursor/skills — make sync-skills
└── docs/
```

**Интерфейсы:** контракт (`Source`, `Sink`, `EventBus`) объявляется в пакете реализации; общие типы данных — в минимальном пакете (`source/event.go`). См. `.cursor/rules/interfaces-at-impl.mdc`.

**Дашборд MVP:** `internal/controlplane/http/web/` (embed), не отдельный `web/` в корне.

Единый бинарь `qrok` для агента и dev-клиента: одна инсталляция, меньше путаницы у пользователя (`qrok agent start` на стейджинге, `qrok listen` локально).

---

## 12. Наблюдаемость

- Логи: `slog`, JSON в проде, текст в dev; `request_id`/`event_id` сквозняком через контекст; ошибки логируются через `fault.LogAttrs`.
- **Duration и slow-лог** (реализуется вместе с httpapi/eventstore, этап 1):
  - duration логируется у **каждого** запроса: HTTP (middleware), gRPC (interceptor), SQL (tracer `pgx.QueryTracer` в `pkg/postgres`);
  - всё, что дольше порога — отдельная `warn`-запись `slow request` / `slow query` с деталями (путь/метод или SQL без значений bind-параметров, duration, request_id);
  - порог конфигурируемый (`log.slow_threshold`, по умолчанию **100ms**), общий для HTTP/gRPC/SQL.
- Метрики (Prometheus): события in/out per tunnel, лаг агента (broker ts → облако), активные стримы, ошибки по `fault.Code`, латентность доставки, hit-rate otter-кэшей.
- Трейсинг (OTel): контекст-пропагация в envelope (`headers["traceparent"]`), trace_id в JSON-ошибках API; сами трейсы — этап 2.
- Health/readiness эндпойнты у всех бинарей; у агента — `qrok agent status`.

---

## 13. Roadmap

### Этап 0 — фундамент (первые PR)

1. Скелет репозитория, `fault`, `config`, protobuf-контракт envelope (buf).
2. Docker-compose для dev: Kafka + Postgres + Redis + MinIO; goose-миграция init-схемы; Makefile (build/test/lint/proto/migrate).

### Этап 1 — MVP (вертикальный срез, one happy path)

| #   | Задача                                                                        | Статус                 |
| --- | ----------------------------------------------------------------------------- | ---------------------- |
| 1   | Agent: Kafka `Source`, своя consumer group, стрим в Gateway, ack→commit       | ✅                     |
| 2   | Gateway: in-memory fan-out, персист в Postgres/S3 до ack агенту               | ✅                     |
| 3   | Dev CLI: `qrok listen` + HTTP `Sink`, DeliveryResult                          | ✅                     |
| 4   | REST: список событий, GET/replay; auth middleware (dev: `allow_insecure_api`) | ✅                     |
| 5   | Минимальный дашборд: лента + Replay (`/dashboard/`)                           | ✅                     |
| 6   | Integration + e2e (`make compose-up`, теги `integration`/`e2e`)               | ✅                     |
| 7   | `qrok login` (OAuth device flow)                                              | ✅                     |
| 8   | Полный middleware-стек §5.4 (OTel, rate limit, gzip, slow-log)                | ⏳ slow-log ✅; OTel/rate limit/gzip — этап 2 |
| 9   | Статусы delivery в UI дашборда                                                | ✅                     |
| 10  | otter-кэш, Redis presence                                                     | ⏳ этап 2              |

**Критерий готовности MVP**: событие из стейджинг-Kafka доходит до localhost за < 1 c; replay из дашборда работает; падение localhost видно в дашборде (статусы delivery в ленте и деталях события).

### Этап 2 — надёжность и UX

- RabbitMQ `Source`; `kafka-local` Sink; фильтры подписок; TUI на bubbletea; catch-up доставки офлайн-подписчику; RLS; retention-политики; multi-gateway через Redis presence; нагрузочные тесты.
- **Agent hardening (§5.1.3–5.1.5, §5.3.1–5.3.2):** gRPC keepalive (client + server); развязанный read/send pipeline; `AgentControl` в protobuf; `Goaway` при drain gateway; `pause_when_no_listeners`; graceful shutdown + `DeleteConsumerGroups` с bounded timeout; backpressure-политики; dedup на Dev CLI.
- **Ops:** Helm chart / Docker-образ агента; документация по `max.poll.interval.ms` и `offsets.retention.minutes` для staging Kafka.

### Этап 3 — рост

- Billing и тарифы, ClickHouse-поиск, алерты, командные роли, регионы, аудит-лог, SSO.

---

## 14. Открытые вопросы (решает владелец)

1. Стек дашборда (SPA-фреймворк) и хостинг облака (managed k8s / VM / fly.io и т.п.).
2. CLI-фреймворк для `qrok` — **cobra** (зафиксировано, реализовано).
3. Тарифная сетка и лимиты (этап 3).
