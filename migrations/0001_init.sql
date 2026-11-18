CREATE TABLE IF NOT EXISTS tasks (
    id              uuid PRIMARY KEY,
    order_ref       text        NOT NULL,
    idempotency_key text        NOT NULL UNIQUE,
    payload_hash    text        NOT NULL,
    task_type       text        NOT NULL,
    priority        integer     NOT NULL,
    zone            text        NOT NULL,
    source_slot     text        NOT NULL,
    dest_slot       text        NOT NULL,
    quantity        integer     NOT NULL CHECK (quantity > 0),
    state           text        NOT NULL,
    assigned_bot_id text,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS tasks_state_idx ON tasks (state, priority, created_at);
CREATE INDEX IF NOT EXISTS tasks_order_ref_idx ON tasks (order_ref);

CREATE TABLE IF NOT EXISTS task_events (
    seq         bigserial PRIMARY KEY,
    task_id     uuid        NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    order_ref   text        NOT NULL,
    from_state  text        NOT NULL,
    to_state    text        NOT NULL,
    bot_id      text,
    reason      text        NOT NULL DEFAULT '',
    emitted_at  timestamptz NOT NULL DEFAULT now(),
    published   boolean     NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS task_events_unpublished_idx
    ON task_events (seq) WHERE published = false;

CREATE TABLE IF NOT EXISTS bots (
    id         text PRIMARY KEY,
    zone       text        NOT NULL,
    home_slot  text        NOT NULL,
    busy_until timestamptz,
    status     text        NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    id          bigserial PRIMARY KEY,
    subject     text        NOT NULL,
    raw         bytea       NOT NULL,
    reason      text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS peer_heartbeats (
    peer_id      text PRIMARY KEY,
    last_seen_at timestamptz NOT NULL
);
