-- 005_derivatives.sql
-- Placeholder for the derivatives overlay (futures + options hedging).
-- The strategy code in src/strategies/{futures,options}/ exists but
-- has no persistence yet; this migration reserves the table names so
-- a future migration doesn't have to worry about back-compat.

CREATE TABLE IF NOT EXISTS derivative_positions (
    id              BIGSERIAL    PRIMARY KEY,
    snapshot_ts     TIMESTAMPTZ  NOT NULL,
    profile         TEXT         NOT NULL,
    underlying      TEXT         NOT NULL,
    instrument_type TEXT         NOT NULL,            -- 'future' | 'option'
    contract_id     BIGINT,
    expiry          DATE,
    strike          DOUBLE PRECISION,
    right_          TEXT,                              -- 'C' | 'P' | NULL
    qty             INTEGER      NOT NULL,
    avg_cost        DOUBLE PRECISION,
    market_value    DOUBLE PRECISION,
    payload         JSONB,
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_derivative_positions_underlying
    ON derivative_positions (underlying);

CREATE INDEX IF NOT EXISTS ix_derivative_positions_profile_ts
    ON derivative_positions (profile, snapshot_ts);


CREATE TABLE IF NOT EXISTS derivative_signals (
    id              BIGSERIAL    PRIMARY KEY,
    run_id          BIGINT,
    underlying      TEXT         NOT NULL,
    overlay_kind    TEXT         NOT NULL,            -- 'hedge_trigger' | 'overlay_signal' | 'vol_regime'
    as_of           DATE         NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    payload         JSONB,
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_derivative_signals_underlying_asof
    ON derivative_signals (underlying, as_of);
