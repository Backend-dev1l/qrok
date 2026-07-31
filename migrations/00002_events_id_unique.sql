-- +goose Up
-- Глобальная дедупликация по event_id (ULID).
-- На партиционированной events уникальный индекс только по id невозможен:
-- Postgres требует включать колонку партиционирования (created_at).
CREATE TABLE IF NOT EXISTS event_dedup (
    id TEXT PRIMARY KEY
);

-- +goose Down
DROP TABLE IF EXISTS event_dedup;
