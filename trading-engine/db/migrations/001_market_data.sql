-- 001_market_data.sql
-- Market bars from IBKR (or any other source). Daily bars use bar_date;
-- intraday bars use bar_ts. The same row can carry both.

CREATE TABLE IF NOT EXISTS market_bars (
    id              BIGSERIAL    PRIMARY KEY,
    symbol          TEXT         NOT NULL,
    bar_date        DATE         NOT NULL,
    bar_ts          TIMESTAMPTZ,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          BIGINT,
    vwap            DOUBLE PRECISION,
    bar_count       INTEGER,
    bar_size        TEXT         NOT NULL DEFAULT '1 day',
    source          TEXT         NOT NULL DEFAULT 'ibkr',
    inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Uniqueness: one row per (symbol, date, bar_size, source). Intraday
-- bars include bar_ts in the index so we don't collide on date alone.
CREATE UNIQUE INDEX IF NOT EXISTS ux_market_bars_symbol_date_size_source
    ON market_bars (symbol, bar_date, bar_size, source);

CREATE INDEX IF NOT EXISTS ix_market_bars_symbol_ts
    ON market_bars (symbol, bar_ts);
