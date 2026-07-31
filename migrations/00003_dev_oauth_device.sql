-- +goose Up
-- Dev-токены для qrok listen (OAuth 2.0 device flow, RFC 8628).

CREATE TABLE dev_tokens (
    id          TEXT PRIMARY KEY,
    project_id  TEXT NOT NULL REFERENCES projects (id),
    user_id     TEXT REFERENCES users (id),
    token_hash  TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dev_tokens_project ON dev_tokens (project_id);

-- Сессии device flow: device_code хранится только как hash.
CREATE TABLE device_authorizations (
    device_code_hash        TEXT PRIMARY KEY,
    user_code               TEXT NOT NULL UNIQUE,
    status                  TEXT NOT NULL DEFAULT 'pending', -- pending | approved | denied | expired | consumed
    project_id              TEXT REFERENCES projects (id),
    dev_token_id            TEXT REFERENCES dev_tokens (id),
    access_token_plaintext  TEXT, -- одноразовая выдача при poll; NULL после consumed
    poll_interval_sec       INT NOT NULL DEFAULT 5,
    expires_at              TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_device_auth_user_code ON device_authorizations (user_code);
CREATE INDEX idx_device_auth_expires ON device_authorizations (expires_at) WHERE status = 'pending';

-- +goose Down
DROP TABLE device_authorizations;
DROP TABLE dev_tokens;
