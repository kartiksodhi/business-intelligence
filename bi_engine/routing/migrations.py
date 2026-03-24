from __future__ import annotations

import asyncpg


async def ensure_alerts_retry_column(pool: asyncpg.Pool) -> None:
    await pool.execute(
        """
        ALTER TABLE alerts
        ADD COLUMN IF NOT EXISTS retry_count SMALLINT NOT NULL DEFAULT 0
        """
    )
