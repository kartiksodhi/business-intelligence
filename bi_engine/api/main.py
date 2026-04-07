"""
FastAPI application entry point.

Creates the asyncpg connection pool at startup via the lifespan context
manager and mounts all routers.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from api.routers.operator import router as operator_router
from features import feature


logger = logging.getLogger(__name__)


async def ensure_operator_tables(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                severity_threshold TEXT NOT NULL
                    CHECK (severity_threshold IN ('WATCH', 'AMBER', 'RED', 'CRITICAL')),
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlists (
                id SERIAL PRIMARY KEY,
                name TEXT,
                cin_list TEXT[],
                state_filter TEXT,
                sector_filter TEXT,
                severity_min TEXT,
                signal_types TEXT[],
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                subscriber_id INTEGER REFERENCES subscribers(id) ON DELETE CASCADE,
                cin VARCHAR(21) REFERENCES master_entities(cin),
                added_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS id SERIAL""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS name TEXT""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS cin_list TEXT[]""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS state_filter TEXT""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS sector_filter TEXT""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS severity_min TEXT""")
        await conn.execute("""ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS signal_types TEXT[]""")
        await conn.execute(
            """
            ALTER TABLE watchlists
            ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE
            """
        )
        await conn.execute(
            """
            ALTER TABLE watchlists
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()
            """
        )
        await conn.execute(
            """
            ALTER TABLE watchlists
            ADD COLUMN IF NOT EXISTS subscriber_id INTEGER
            """
        )
        await conn.execute(
            """
            ALTER TABLE watchlists
            ADD COLUMN IF NOT EXISTS cin VARCHAR(21)
            """
        )
        await conn.execute(
            """
            ALTER TABLE watchlists
            ADD COLUMN IF NOT EXISTS added_at TIMESTAMP NOT NULL DEFAULT NOW()
            """
        )
        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlists_subscriber_cin
            ON watchlists (subscriber_id, cin)
            WHERE subscriber_id IS NOT NULL AND cin IS NOT NULL
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS delivered_alerts (
                id SERIAL PRIMARY KEY,
                subscriber_id INTEGER NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
                cin VARCHAR(21) NOT NULL,
                event_type TEXT NOT NULL,
                delivered_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_delivered_alerts_lookup
            ON delivered_alerts (subscriber_id, cin, event_type, delivered_at DESC)
            """
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Create the asyncpg pool on startup. Tear it down on shutdown.
    Fails fast if DATABASE_URL is not set — no silent misconfiguration.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")

    logger.info("Creating asyncpg connection pool.")
    app.state.pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    await ensure_operator_tables(app.state.pool)

    if feature("QUANT_MODULE"):
        from quant.migrations import run_quant_migrations
        await run_quant_migrations(app.state.pool)
        logger.info("Quant tables ensured.")

    logger.info("Connection pool created.")

    yield

    logger.info("Closing asyncpg connection pool.")
    await app.state.pool.close()
    logger.info("Connection pool closed.")


app = FastAPI(
    title="Business Intelligence — Operator API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(operator_router)

if feature("QUANT_MODULE"):
    from api.routers.quant import router as quant_router
    app.include_router(quant_router)
