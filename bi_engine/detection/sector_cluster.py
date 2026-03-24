"""
Sector cluster detector.

Runs once per OGD diff cycle (monthly). Detects when 5+ active companies
in the same state + NIC code have moved to AMBER/RED within the last 30 days.
"""

from __future__ import annotations

import json
import logging

import asyncpg


logger = logging.getLogger(__name__)


class SectorClusterDetector:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def run(self) -> int:
        try:
            rows = await self.db.fetch(
                """
                SELECT registered_state,
                       industrial_class,
                       COUNT(*) AS stressed_count,
                       ARRAY_AGG(cin) AS affected_cins
                FROM master_entities
                WHERE health_band IN ('AMBER', 'RED')
                  AND last_score_computed_at > NOW() - INTERVAL '30 days'
                  AND status = 'Active'
                GROUP BY registered_state, industrial_class
                HAVING COUNT(*) >= 5
                """
            )

            fired_count = 0
            for row in rows:
                state = row["registered_state"]
                nic = row["industrial_class"]
                already_fired = await self.db.fetchval(
                    """
                    SELECT id FROM events
                    WHERE event_type = 'SECTOR_CLUSTER_ALERT'
                      AND detected_at > NOW() - INTERVAL '30 days'
                      AND data_json->>'registered_state' = $1
                      AND data_json->>'industrial_class' = $2
                    LIMIT 1
                    """,
                    state,
                    nic,
                )
                if already_fired:
                    logger.debug(
                        "Sector cluster for state=%s nic=%s already fired recently, skipping.",
                        state,
                        nic,
                    )
                    continue

                await self.db.execute(
                    """
                    INSERT INTO events (
                      cin, source, event_type, severity, detected_at, data_json
                    ) VALUES (
                      NULL,
                      'SECTOR_CLUSTER_DETECTOR',
                      'SECTOR_CLUSTER_ALERT',
                      'WATCH',
                      NOW(),
                      $1::jsonb
                    )
                    """,
                    json.dumps(
                        {
                            "registered_state": state,
                            "industrial_class": nic,
                            "stressed_count": int(row["stressed_count"]),
                            "affected_cins": list(row["affected_cins"]),
                            "detection_window_days": 30,
                        }
                    ),
                )
                logger.info(
                    "SECTOR_CLUSTER_ALERT fired: state=%s nic=%s count=%d",
                    state,
                    nic,
                    row["stressed_count"],
                )
                fired_count += 1

            return fired_count
        except Exception as exc:
            logger.error("SectorClusterDetector failed: %s", exc)
            return 0
