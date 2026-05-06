-- 004_runtime_logs.sql
-- Job-level audit table for the runtime layer (daily_run, EOD
-- reconcile, healthcheck, auto_rebalance). Every job invocation gets
-- one row with timing, status, and a free-form JSON payload.

CREATE TABLE IF NOT EXISTS run_logs (
    id              BIGSERIAL    PRIMARY KEY,
    job             TEXT         NOT NULL,            -- 'daily_run' | 'eod_reconcile' | 'healthcheck' | ...
    profile         TEXT         NOT NULL,            -- 'paper' | 'live'
    status          TEXT         NOT NULL,            -- 'running' | 'ok' | 'error' | 'skipped'
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at        TIMESTAMPTZ,
    duration_ms     BIGINT,
    error           TEXT,
    payload         JSONB
);

-- The dashboard's audit screen reads "latest 50 rows for job=X" and
-- the healthcheck reads "any error rows in the last 24h" — both want
-- (job, started_at).
CREATE INDEX IF NOT EXISTS ix_run_logs_job_started
    ON run_logs (job, started_at);

CREATE INDEX IF NOT EXISTS ix_run_logs_profile_status
    ON run_logs (profile, status);
