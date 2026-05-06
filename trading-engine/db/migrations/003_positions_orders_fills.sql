-- 003_positions_orders_fills.sql
-- The execution-layer audit trail: every order we submit, every fill
-- the broker reports back, and a position snapshot table for end-of-
-- day reconciliation.

CREATE TABLE IF NOT EXISTS orders (
    id                BIGSERIAL    PRIMARY KEY,
    -- order_router.idempotency_key — stable per trading day per ticket.
    -- UNIQUE so we can't double-submit, and so retries resolve to the
    -- existing row.
    idempotency_key   TEXT         NOT NULL UNIQUE,
    run_id            BIGINT,
    profile           TEXT         NOT NULL,
    symbol            TEXT         NOT NULL,
    side              TEXT         NOT NULL,           -- 'BUY' | 'SELL'
    qty               INTEGER      NOT NULL,           -- absolute qty
    price             DOUBLE PRECISION,
    order_type        TEXT         NOT NULL DEFAULT 'MKT',
    status            TEXT         NOT NULL DEFAULT 'submitted',  -- submitted|filled|cancelled|error
    broker_order_id   BIGINT,
    broker_perm_id    BIGINT,
    submitted_at      TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload           JSONB,                                       -- ticket + broker response
    error             TEXT
);

CREATE INDEX IF NOT EXISTS ix_orders_profile_submitted ON orders (profile, submitted_at);
CREATE INDEX IF NOT EXISTS ix_orders_symbol           ON orders (symbol);
CREATE INDEX IF NOT EXISTS ix_orders_status           ON orders (status);


CREATE TABLE IF NOT EXISTS fills (
    id                BIGSERIAL    PRIMARY KEY,
    -- exec_id is broker-assigned and globally unique for live IBKR
    -- accounts. UNIQUE so the EOD job can replay safely.
    exec_id           TEXT         NOT NULL UNIQUE,
    -- FK-ish reference to orders.id; nullable so we can still persist
    -- orphan fills observed via fill_monitor before the order row
    -- exists (or in the live profile where the order was placed
    -- outside the engine).
    order_id          BIGINT,
    broker_order_id   BIGINT,
    profile           TEXT         NOT NULL,
    symbol            TEXT         NOT NULL,
    side              TEXT,                                        -- 'BOT' | 'SLD' from broker
    qty               INTEGER      NOT NULL,                       -- signed
    price             DOUBLE PRECISION NOT NULL,
    filled_at         TIMESTAMPTZ  NOT NULL,
    inserted_at       TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload           JSONB
);

CREATE INDEX IF NOT EXISTS ix_fills_profile_filled_at ON fills (profile, filled_at);
CREATE INDEX IF NOT EXISTS ix_fills_symbol            ON fills (symbol);
CREATE INDEX IF NOT EXISTS ix_fills_order_id          ON fills (order_id);


-- Position snapshots — written by the EOD reconciler and the
-- dashboard's positions/refresh endpoint. The snapshot_ts is the
-- "as of" time; we keep history rather than overwriting so we can
-- replay drift over time.
CREATE TABLE IF NOT EXISTS positions (
    id                BIGSERIAL    PRIMARY KEY,
    snapshot_ts       TIMESTAMPTZ  NOT NULL,
    profile           TEXT         NOT NULL,
    symbol            TEXT         NOT NULL,
    qty               INTEGER      NOT NULL,
    avg_cost          DOUBLE PRECISION,
    market_value      DOUBLE PRECISION,
    unrealized_pnl    DOUBLE PRECISION,
    currency          TEXT         NOT NULL DEFAULT 'USD',
    inserted_at       TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_snapshot_profile_sym
    ON positions (snapshot_ts, profile, symbol);

CREATE INDEX IF NOT EXISTS ix_positions_profile_ts ON positions (profile, snapshot_ts);
CREATE INDEX IF NOT EXISTS ix_positions_symbol     ON positions (symbol);
