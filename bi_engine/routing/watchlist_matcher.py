from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List, Optional

import asyncpg


logger = logging.getLogger(__name__)


@dataclass
class EventRow:
    id: int
    cin: str
    event_type: str
    severity: str
    detected_at: str
    data_json: dict
    health_score_before: Optional[int]
    health_score_after: Optional[int]
    contagion_chain: Optional[dict]
    source: str


class WatchlistMatcher:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.last_inserted_alert_ids: List[int] = []

    async def match_event(self, event: EventRow) -> List[int]:
        rows = await self.pool.fetch(
            """
            SELECT w.id
            FROM watchlists w
            JOIN master_entities me ON me.cin = $1
            WHERE w.is_active = TRUE
            AND (w.cin_list IS NULL OR $1 = ANY(w.cin_list))
            AND (w.state_filter IS NULL OR me.registered_state = w.state_filter)
            AND (w.sector_filter IS NULL OR me.industrial_class = w.sector_filter)
            AND (w.severity_min IS NULL OR
                 CASE $2
                   WHEN 'CRITICAL' THEN 4
                   WHEN 'ALERT'    THEN 3
                   WHEN 'WATCH'    THEN 2
                   WHEN 'INFO'     THEN 1
                 END >=
                 CASE w.severity_min
                   WHEN 'CRITICAL' THEN 4
                   WHEN 'ALERT'    THEN 3
                   WHEN 'WATCH'    THEN 2
                   WHEN 'INFO'     THEN 1
                 END)
            AND (w.signal_types IS NULL OR $3 = ANY(w.signal_types))
            """,
            event.cin,
            event.severity,
            event.event_type,
        )

        matched_ids = [int(row["id"]) for row in rows]
        alert_ids: List[int] = []
        payload = {
            "event_id": event.id,
            "cin": event.cin,
            "event_type": event.event_type,
            "severity": event.severity,
            "source": event.source,
            "detected_at": event.detected_at,
            "data_json": event.data_json,
            "health_score_before": event.health_score_before,
            "health_score_after": event.health_score_after,
            "contagion_chain": event.contagion_chain,
        }

        for watchlist_id in matched_ids:
            row = await self.pool.fetchrow(
                """
                INSERT INTO alerts (
                    event_id,
                    watchlist_id,
                    cin,
                    severity,
                    alert_payload,
                    delivery_status,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, 'PENDING', NOW()
                )
                RETURNING id
                """,
                event.id,
                watchlist_id,
                event.cin,
                event.severity,
                json.dumps(payload),
            )
            if row:
                alert_ids.append(int(row["id"]))

        self.last_inserted_alert_ids = alert_ids
        return matched_ids
