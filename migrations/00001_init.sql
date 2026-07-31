-- +goose Up

CREATE TABLE orgs (
    id         TEXT PRIMARY KEY,           -- ULID
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id            TEXT PRIMARY KEY,
    org_id        TEXT NOT NULL REFERENCES orgs (id),
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role          TEXT NOT NULL DEFAULT 'dev', -- owner | dev | viewer
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE projects (
    id         TEXT PRIMARY KEY,
    org_id     TEXT NOT NULL REFERENCES orgs (id),
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

CREATE TABLE tunnels (
    id          TEXT PRIMARY KEY,
    project_id  TEXT NOT NULL REFERENCES projects (id),
    name        TEXT NOT NULL,
    source_type TEXT NOT NULL,              -- kafka | rabbitmq
    topics      TEXT[] NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

-- Токен агента хранится только как hash; plaintext показывается один раз при создании.
CREATE TABLE agent_tokens (
    id         TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects (id),
    token_hash TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- События: партиционирование по времени с первого дня (retention = drop partition).
-- Payload — гибрид: < 256 КБ в колонке payload, иначе payload_ref на object store.
CREATE TABLE events (
    id            TEXT NOT NULL,            -- ULID (дедупликация)
    tunnel_id     TEXT NOT NULL,
    topic         TEXT NOT NULL,
    partition     INT,
    broker_offset BIGINT,
    key           BYTEA,
    headers       JSONB,
    payload       BYTEA,
    payload_ref   TEXT,
    payload_size  INT NOT NULL DEFAULT 0,
    is_replay     BOOLEAN NOT NULL DEFAULT FALSE,
    broker_ts     TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at),
    CHECK ((payload IS NULL) <> (payload_ref IS NULL))
) PARTITION BY RANGE (created_at);

-- Стартовая партиция; дальше создаются фоновым джобом помесячно.
CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE INDEX idx_events_tunnel_created ON events (tunnel_id, created_at DESC);
CREATE INDEX idx_events_topic ON events (tunnel_id, topic, created_at DESC);

CREATE TABLE deliveries (
    id          TEXT PRIMARY KEY,
    event_id    TEXT NOT NULL,
    target_id   TEXT NOT NULL,              -- dev-клиент
    kind        TEXT NOT NULL,              -- live | replay
    status      TEXT NOT NULL,              -- pending | delivered | failed | expired
    status_code INT,
    error       TEXT,
    latency_ms  INT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_event ON deliveries (event_id);
CREATE INDEX idx_deliveries_target_status ON deliveries (target_id, status);

-- +goose Down
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE agent_tokens;
DROP TABLE tunnels;
DROP TABLE projects;
DROP TABLE users;
DROP TABLE orgs;
