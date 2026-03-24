from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg


logger = logging.getLogger(__name__)

COMBINATIONS = [
    {
        "name": "PRE_INSOLVENCY_CLASSIC",
        "severity": "ALERT",
        "conditions": [
            {"event_type": "FILING_DECAY"},
            {"event_type__in": ["ECOURTS_SEC138", "ECOURTS_CIVIL"]},
        ],
        "window_days": 180,
    },
    {
        "name": "NPA_SEIZURE_IMMINENT",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "FILING_DECAY"},
            {"event_type__in": ["SARFAESI_DEMAND", "SARFAESI_POSSESSION"]},
        ],
        "window_days": 90,
    },
    {
        "name": "ZOMBIE_COMPANY",
        "severity": "ALERT",
        "conditions": [
            {"event_type": "GST_CANCELLED"},
            {"source": "mca", "field": "status", "value": "Active"},
        ],
        "window_days": 365,
    },
    {
        "name": "OPERATIONAL_SHUTDOWN",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "EPFO_CONTRIBUTION_DROP"},
            {"event_type": "GEM_HIRING_FREEZE"},
            {"event_type": "FILING_DECAY"},
        ],
        "window_days": 90,
    },
    {
        "name": "GOVERNANCE_COLLAPSE",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "AUDITOR_CHANGED"},
            {"event_type": "CFO_RESIGNED"},
            {"event_type": "FILING_DECAY"},
        ],
        "window_days": 180,
    },
    {
        "name": "CONTAGION_BAD_ACTOR",
        "severity": "ALERT",
        "conditions": [
            {"event_type": "WILFUL_DEFAULT_DIRECTOR"},
        ],
        "window_days": 0,
    },
    {
        "name": "PROMOTER_EXIT_BANK_SEIZURE",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "SEBI_BULK_DEAL_PROMOTER_SELL"},
            {"event_type__in": ["SARFAESI_DEMAND", "SARFAESI_POSSESSION"]},
        ],
        "window_days": 90,
    },
    {
        "name": "FUNDED_GROWTH",
        "severity": "INFO",
        "conditions": [
            {"event_type": "CAPITAL_INCREASE"},
            {"event_type": "EPFO_HIRING_SURGE"},
            {"event_type": "GEM_ORDER_WON"},
        ],
        "window_days": 180,
    },
]


async def check_combinations(cin: str, new_event_type: str, db_conn: asyncpg.Connection) -> list[dict]:
    if not cin:
        return []

    rows = await db_conn.fetch(
        """
        SELECT id, source, event_type, severity, detected_at, data_json
        FROM events
        WHERE cin = $1
          AND detected_at >= NOW() - INTERVAL '365 days'
        ORDER BY detected_at DESC
        """,
        cin,
    )
    if not rows:
        return []

    emitted: list[dict] = []
    events = [dict(row) for row in rows]
    relevant_event_types = {event["event_type"] for event in events}
    now = datetime.now(timezone.utc)

    for combination in COMBINATIONS:
        if not _combination_mentions_event(combination, new_event_type):
            continue
        if combination["name"] in relevant_event_types:
            continue
        if await _recently_fired(db_conn, cin, combination["name"]):
            continue

        matched_ids = await _match_conditions(db_conn, cin, events, combination, now)
        if not matched_ids:
            continue

        payload = {
            "combination": combination["name"],
            "trigger_event_type": new_event_type,
            "constituent_event_ids": matched_ids,
        }
        inserted = await db_conn.fetchrow(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES ($1, 'signal_combiner', $2, $3, NOW(), $4::jsonb)
            RETURNING id
            """,
            cin,
            combination["name"],
            combination["severity"],
            json.dumps(payload),
        )
        emitted.append(
            {
                "id": inserted["id"] if inserted else None,
                "cin": cin,
                "event_type": combination["name"],
                "severity": combination["severity"],
                "notes": matched_ids,
            }
        )

    return emitted


def _combination_mentions_event(combination: dict, event_type: str) -> bool:
    for condition in combination["conditions"]:
        if condition.get("event_type") == event_type:
            return True
        if event_type in condition.get("event_type__in", []):
            return True
    return False


async def _recently_fired(db_conn: asyncpg.Connection, cin: str, combination_name: str) -> bool:
    row = await db_conn.fetchrow(
        """
        SELECT id
        FROM events
        WHERE cin = $1
          AND event_type = $2
          AND detected_at > NOW() - INTERVAL '30 days'
        LIMIT 1
        """,
        cin,
        combination_name,
    )
    return bool(row)


async def _match_conditions(
    db_conn: asyncpg.Connection,
    cin: str,
    events: list[dict[str, Any]],
    combination: dict,
    now: datetime,
) -> list[int]:
    window_days = int(combination.get("window_days", 0))
    window_start = now - timedelta(days=window_days) if window_days else None
    matched_ids: list[int] = []

    for condition in combination["conditions"]:
        event = _find_matching_event(events, condition, window_start)
        if event:
            matched_ids.append(int(event["id"]))
            continue
        if await _matches_entity_state(db_conn, cin, condition):
            continue
        return []

    return matched_ids


def _find_matching_event(
    events: list[dict[str, Any]],
    condition: dict,
    window_start: datetime | None,
) -> dict[str, Any] | None:
    for event in events:
        detected_at = event["detected_at"]
        if isinstance(detected_at, str):
            detected_at = datetime.fromisoformat(detected_at)
        if detected_at.tzinfo is None:
            detected_at = detected_at.replace(tzinfo=timezone.utc)
        if window_start and detected_at < window_start:
            continue
        expected_type = condition.get("event_type")
        expected_types = condition.get("event_type__in", [])
        if expected_type and event["event_type"] == expected_type:
            return event
        if expected_types and event["event_type"] in expected_types:
            return event
    return None


async def _matches_entity_state(db_conn: asyncpg.Connection, cin: str, condition: dict) -> bool:
    field = condition.get("field")
    value = condition.get("value")
    if not field:
        return False
    row = await db_conn.fetchrow(
        f"SELECT {field} FROM master_entities WHERE cin = $1",
        cin,
    )
    if not row:
        return False
    return row[field] == value
