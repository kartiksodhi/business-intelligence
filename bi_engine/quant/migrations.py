"""
Quant module DB migrations.

Creates the 6 new tables needed by bi_engine/quant/.
Run via: await run_quant_migrations(pool)
All tables are additive — zero modifications to existing ICIE tables.
"""

from __future__ import annotations

import asyncpg


_DDL = """
CREATE TABLE IF NOT EXISTS ticker_bridge (
    id                  SERIAL          PRIMARY KEY,
    cin                 VARCHAR(21)     NOT NULL REFERENCES master_entities(cin),
    exchange            VARCHAR(5)      NOT NULL CHECK (exchange IN ('NSE', 'BSE')),
    ticker_symbol       VARCHAR(20)     NOT NULL,
    isin                VARCHAR(12),
    bse_code            VARCHAR(10),
    company_name_listed TEXT,
    sector_listed       VARCHAR(100),
    market_cap_cr       NUMERIC(15,2),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    source              VARCHAR(30)     NOT NULL DEFAULT 'BSE_MASTER',
    last_verified_at    TIMESTAMP       NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ticker_bridge_cin_exchange UNIQUE (cin, exchange)
);
CREATE INDEX IF NOT EXISTS idx_ticker_bridge_cin    ON ticker_bridge (cin);
CREATE INDEX IF NOT EXISTS idx_ticker_bridge_symbol ON ticker_bridge (ticker_symbol, exchange);
CREATE INDEX IF NOT EXISTS idx_ticker_bridge_isin   ON ticker_bridge (isin);

CREATE TABLE IF NOT EXISTS price_daily (
    id              BIGSERIAL   PRIMARY KEY,
    ticker_symbol   VARCHAR(20) NOT NULL,
    exchange        VARCHAR(5)  NOT NULL CHECK (exchange IN ('NSE', 'BSE')),
    trade_date      DATE        NOT NULL,
    open_price      NUMERIC(12,2),
    high_price      NUMERIC(12,2),
    low_price       NUMERIC(12,2),
    close_price     NUMERIC(12,2) NOT NULL,
    prev_close      NUMERIC(12,2),
    volume          BIGINT,
    value_cr        NUMERIC(15,4),
    delivery_qty    BIGINT,
    delivery_pct    NUMERIC(6,2),
    CONSTRAINT uq_price_daily_ticker_date UNIQUE (ticker_symbol, exchange, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_price_daily_ticker_date ON price_daily (ticker_symbol, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_price_daily_trade_date  ON price_daily (trade_date DESC);

CREATE TABLE IF NOT EXISTS factor_scores (
    id                          BIGSERIAL   PRIMARY KEY,
    cin                         VARCHAR(21) NOT NULL REFERENCES master_entities(cin),
    score_date                  DATE        NOT NULL,
    health_score_raw            SMALLINT,
    health_score_30d_delta      NUMERIC(6,2),
    health_score_90d_delta      NUMERIC(6,2),
    legal_velocity_score        NUMERIC(8,4),
    director_instability_score  NUMERIC(6,2),
    filing_decay_score          NUMERIC(6,2),
    workforce_momentum          NUMERIC(6,2),
    government_revenue_signal   NUMERIC(6,2),
    leverage_creep_signal       NUMERIC(6,2),
    distress_composite          NUMERIC(6,2),
    promoter_stress_signal      NUMERIC(6,2),
    sector_stress_percentile    NUMERIC(6,2),
    composite_alpha_score       NUMERIC(6,2),
    signal_fired                TEXT[],
    computed_at                 TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_factor_scores_cin_date UNIQUE (cin, score_date)
);
CREATE INDEX IF NOT EXISTS idx_factor_scores_cin       ON factor_scores (cin, score_date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_scores_date      ON factor_scores (score_date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_scores_composite ON factor_scores (composite_alpha_score, score_date DESC);

CREATE TABLE IF NOT EXISTS portfolio_positions (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          VARCHAR(36) NOT NULL,
    cin             VARCHAR(21) REFERENCES master_entities(cin),
    ticker_symbol   VARCHAR(20) NOT NULL,
    exchange        VARCHAR(5)  NOT NULL,
    direction       VARCHAR(5)  NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    entry_date      DATE        NOT NULL,
    exit_date       DATE,
    entry_price     NUMERIC(12,2),
    exit_price      NUMERIC(12,2),
    quantity        INTEGER,
    weight_pct      NUMERIC(6,2),
    kelly_fraction  NUMERIC(6,4),
    entry_composite NUMERIC(6,2),
    entry_signals   TEXT[],
    pnl_abs         NUMERIC(15,2),
    pnl_pct         NUMERIC(8,4),
    is_open         BOOLEAN     NOT NULL DEFAULT TRUE,
    exit_reason     VARCHAR(50),
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_portfolio_positions_open ON portfolio_positions (is_open) WHERE is_open = TRUE;
CREATE INDEX IF NOT EXISTS idx_portfolio_positions_run  ON portfolio_positions (run_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_positions_cin  ON portfolio_positions (cin, entry_date DESC);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id                  SERIAL      PRIMARY KEY,
    run_name            VARCHAR(100) NOT NULL,
    strategy_config     JSONB       NOT NULL,
    universe_size       INTEGER,
    start_date          DATE        NOT NULL,
    end_date            DATE        NOT NULL,
    total_signals       INTEGER,
    long_count          INTEGER,
    short_count         INTEGER,
    annualized_return   NUMERIC(8,4),
    sharpe_ratio        NUMERIC(8,4),
    max_drawdown        NUMERIC(8,4),
    win_rate            NUMERIC(6,4),
    ic_30d              NUMERIC(8,4),
    ic_60d              NUMERIC(8,4),
    ic_90d              NUMERIC(8,4),
    notes               TEXT,
    created_at          TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_returns (
    id              BIGSERIAL   PRIMARY KEY,
    event_id        BIGINT      REFERENCES events(id),
    cin             VARCHAR(21) NOT NULL,
    ticker_symbol   VARCHAR(20),
    signal_type     VARCHAR(50) NOT NULL,
    signal_date     DATE        NOT NULL,
    direction       VARCHAR(5)  NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    price_at_signal NUMERIC(12,2),
    price_30d       NUMERIC(12,2),
    price_60d       NUMERIC(12,2),
    price_90d       NUMERIC(12,2),
    return_30d      NUMERIC(10,6),
    return_60d      NUMERIC(10,6),
    return_90d      NUMERIC(10,6),
    nifty_return_30d  NUMERIC(10,6),
    nifty_return_60d  NUMERIC(10,6),
    nifty_return_90d  NUMERIC(10,6),
    alpha_30d       NUMERIC(10,6),
    alpha_60d       NUMERIC(10,6),
    alpha_90d       NUMERIC(10,6),
    filled_at       TIMESTAMP,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signal_returns_signal_type ON signal_returns (signal_type, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_returns_cin         ON signal_returns (cin, signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signal_returns_unfilled    ON signal_returns (filled_at) WHERE filled_at IS NULL;
"""


async def run_quant_migrations(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_DDL)
