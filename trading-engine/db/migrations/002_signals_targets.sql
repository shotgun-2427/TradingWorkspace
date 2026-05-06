-- 002_signals_targets.sql
-- Per-model raw signals and the post-aggregation target book that the
-- basket builder actually trades. Both tables key on a run_id so a
-- single pipeline invocation can be reproduced end-to-end.

CREATE TABLE IF NOT EXISTS signals (
    id              BIGSERIAL    PRIMARY KEY,
    run_id          BIGINT,
    model_id        TEXT         NOT NULL,
    symbol          TEXT         NOT NULL,
    as_of           DATE         NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    payload         JSONB,
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One signal per (run, model, symbol, date). Re-running a model for
-- the same date inside the same run is idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_run_model_sym_date
    ON signals (run_id, model_id, symbol, as_of);

CREATE INDEX IF NOT EXISTS ix_signals_symbol_asof
    ON signals (symbol, as_of);


CREATE TABLE IF NOT EXISTS targets (
    id                BIGSERIAL    PRIMARY KEY,
    run_id            BIGINT,
    profile           TEXT         NOT NULL,
    rebalance_date    DATE         NOT NULL,
    symbol            TEXT         NOT NULL,
    target_weight     DOUBLE PRECISION NOT NULL,
    reference_price   DOUBLE PRECISION,
    payload           JSONB,
    inserted_at       TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The basket builder reads "the latest targets for profile=paper" — so
-- we want one row per (run, profile, date, symbol) and a fast index
-- over (profile, rebalance_date) for the latest-date query.
CREATE UNIQUE INDEX IF NOT EXISTS ux_targets_run_profile_date_sym
    ON targets (run_id, profile, rebalance_date, symbol);

CREATE INDEX IF NOT EXISTS ix_targets_profile_date
    ON targets (profile, rebalance_date);
