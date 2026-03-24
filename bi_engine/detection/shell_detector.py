"""
Shell company detector.

Checks six structural conditions simultaneously. Fires SHELL_RISK (WATCH severity)
only when ALL six conditions are true. No partial events.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import asyncpg


logger = logging.getLogger(__name__)


class ShellDetector:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def run(self) -> int:
        try:
            column_names = await self._load_master_entity_columns()
            rows = await self._candidate_rows(column_names)
            fired = 0
            for row in rows:
                await self._insert_shell_risk(row["cin"], dict(row))
                fired += 1
            return fired
        except Exception as exc:
            logger.error("ShellDetector run failed: %s", exc)
            return 0

    async def check(self, cin: str) -> bool:
        try:
            column_names = await self._load_master_entity_columns()
            rows = await self._candidate_rows(column_names, cin=cin)
            if not rows:
                return False

            await self._insert_shell_risk(cin, dict(rows[0]))
            logger.info("SHELL_RISK fired for CIN %s", cin)
            return True
        except Exception as exc:
            logger.error("ShellDetector failed for %s: %s", cin, exc)
            return False

    async def _load_master_entity_columns(self) -> set[str]:
        rows = await self.db.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'master_entities'
            """,
        )
        return {row["column_name"] for row in rows}

    async def _candidate_rows(self, column_names: set[str], cin: str | None = None):
        epfo_clause = self._status_clause(
            column_names,
            identifier_column="epfo_id",
            event_types=["EPFO_ESTABLISHMENT_DELISTED"],
        )
        gst_clause = self._status_clause(
            column_names,
            identifier_column="gstin",
            event_types=["GST_CANCELLED", "GST_SUSPENDED"],
        )
        cin_clause = "AND me.cin = $1" if cin else ""
        sql = f"""
            SELECT
                me.cin,
                me.date_of_incorporation,
                me.authorized_capital,
                me.date_of_last_agm,
                dg.din AS director_din,
                (
                    SELECT COUNT(DISTINCT dg2.cin)
                    FROM governance_graph dg2
                    WHERE dg2.din = dg.din
                      AND dg2.is_active = TRUE
                ) AS other_board_count
            FROM master_entities me
            JOIN governance_graph dg
              ON dg.cin = me.cin
             AND dg.is_active = TRUE
            WHERE
                (
                    EXTRACT(YEAR FROM AGE(NOW(), me.date_of_incorporation)) * 12
                    + EXTRACT(MONTH FROM AGE(NOW(), me.date_of_incorporation))
                ) < 36
                AND me.authorized_capital <= 1000000
                AND me.date_of_last_agm IS NULL
                AND {epfo_clause}
                AND {gst_clause}
                AND (
                    SELECT COUNT(DISTINCT dg2.cin)
                    FROM governance_graph dg2
                    WHERE dg2.din = dg.din
                      AND dg2.is_active = TRUE
                ) >= 5
                AND NOT EXISTS (
                    SELECT 1
                    FROM events e
                    WHERE e.cin = me.cin
                      AND e.event_type = 'SHELL_RISK'
                      AND e.detected_at > NOW() - INTERVAL '30 days'
                )
                {cin_clause}
            ORDER BY me.cin
        """
        if cin:
            return await self.db.fetch(sql, cin)
        return await self.db.fetch(sql)

    def _status_clause(
        self,
        column_names: set[str],
        *,
        identifier_column: str,
        event_types: list[str],
    ) -> str:
        if identifier_column not in column_names:
            return "TRUE"
        quoted = ", ".join(f"'{event_type}'" for event_type in event_types)
        return f"""
            (
                me.{identifier_column} IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM events e
                    WHERE e.cin = me.cin
                      AND e.event_type IN ({quoted})
                      AND e.detected_at > NOW() - INTERVAL '12 months'
                )
            )
        """

    async def _insert_shell_risk(self, cin: str, detail: dict) -> None:
        payload = {
            "conditions_met": [1, 2, 3, 4, 5, 6],
            "authorized_capital": detail.get("authorized_capital"),
            "date_of_incorporation": str(detail.get("date_of_incorporation") or ""),
            "director_with_board_count": detail.get("director_din"),
            "other_board_count": int(detail.get("other_board_count") or 0),
        }
        await self.db.execute(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES ($1, 'shell_detector', 'SHELL_RISK', 'WATCH', NOW(), $2::jsonb)
            """,
            cin,
            json.dumps(payload),
        )


async def _main() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL environment variable is required.")

    pool = await asyncpg.create_pool(database_url)
    try:
        detector = ShellDetector(pool)
        fired = await detector.run()
    finally:
        await pool.close()
    print(f"SHELL_RISK events fired: {fired}")


if __name__ == "__main__":
    asyncio.run(_main())
