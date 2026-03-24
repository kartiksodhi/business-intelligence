from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import asyncpg


logger = logging.getLogger(__name__)

SEVERITY_RANK = {"INFO": 1, "WATCH": 2, "ALERT": 3, "CRITICAL": 4}


@dataclass
class EventSummary:
    event_id: int
    event_type: str
    source: str
    detected_at: str
    severity: str
    data_json: dict
    health_score_before: Optional[int]
    health_score_after: Optional[int]


@dataclass
class AlertDigest:
    watchlist_id: int
    cin: str
    company_name: str
    company_state: str
    company_sector: str
    events: List[EventSummary]
    health_score_current: int
    health_score_previous: int
    health_band: str
    severity: str
    contagion_result: Optional[dict]
    alert_ids: List[int]


class BatchFlusher:
    def __init__(
        self,
        pool: asyncpg.Pool,
        summarizer: "AlertSummarizer",
        telegram: "TelegramDeliverer",
    ):
        self.pool = pool
        self.summarizer = summarizer
        self.telegram = telegram

    async def flush(self) -> None:
        rows = await self.pool.fetch(
            """
            SELECT
                a.id            AS alert_id,
                a.event_id,
                a.watchlist_id,
                a.cin,
                a.severity,
                a.alert_payload,
                a.created_at,
                a.delivery_status,
                COALESCE(a.retry_count, 0) AS retry_count,
                me.company_name,
                me.registered_state,
                me.industrial_class,
                me.health_score  AS health_score_current,
                me.health_band
            FROM alerts a
            JOIN master_entities me ON me.cin = a.cin
            WHERE (
                a.delivery_status = 'PENDING'
                OR (
                    a.severity = 'CRITICAL'
                    AND a.delivery_status = 'FAILED'
                    AND a.retry_count < 3
                )
            )
            AND (
                a.severity = 'CRITICAL'
                OR (a.severity = 'ALERT' AND a.created_at <= NOW() - INTERVAL '4 hours')
                OR (a.severity = 'WATCH' AND a.created_at <= NOW() - INTERVAL '24 hours')
                OR (a.severity = 'INFO'  AND a.created_at <= NOW() - INTERVAL '7 days')
                OR (
                    a.severity = 'CRITICAL'
                    AND a.delivery_status = 'FAILED'
                    AND a.retry_count < 3
                )
            )
            ORDER BY a.watchlist_id, a.cin, a.created_at
            """
        )

        grouped: dict[tuple[int, str], list[dict]] = {}
        for row in rows:
            row_dict = dict(row)
            if not _is_eligible(row_dict):
                continue
            grouped.setdefault((row_dict["watchlist_id"], row_dict["cin"]), []).append(row_dict)

        for group_rows in grouped.values():
            try:
                digest = _build_digest(group_rows)
                batch_id = str(uuid.uuid4())
                await self.pool.execute(
                    "UPDATE alerts SET batch_id = $1 WHERE id = ANY($2)",
                    batch_id,
                    digest.alert_ids,
                )
                summary = await self.summarizer.summarize(digest)

                if digest.severity == "CRITICAL":
                    sent = await self.telegram.send(digest, summary)
                    if not sent:
                        await self._handle_telegram_failure(digest)
                        continue
                    delivery_channel = "TELEGRAM"
                else:
                    delivery_channel = "DIGEST"

                await self.pool.execute(
                    """
                    UPDATE alerts
                    SET
                        delivery_status = 'DELIVERED',
                        ai_summary = $1,
                        delivered_at = NOW(),
                        delivery_channel = $2,
                        batch_id = $3
                    WHERE id = ANY($4)
                    """,
                    summary,
                    delivery_channel,
                    batch_id,
                    digest.alert_ids,
                )
            except Exception as exc:
                logger.error("Batch flush failed for watchlist=%s cin=%s: %s", group_rows[0]["watchlist_id"], group_rows[0]["cin"], exc)

    async def flush_critical(self, alert_id: int) -> None:
        row = await self.pool.fetchrow(
            """
            SELECT
                a.id            AS alert_id,
                a.event_id,
                a.watchlist_id,
                a.cin,
                a.severity,
                a.alert_payload,
                a.created_at,
                a.delivery_status,
                COALESCE(a.retry_count, 0) AS retry_count,
                me.company_name,
                me.registered_state,
                me.industrial_class,
                me.health_score  AS health_score_current,
                me.health_band
            FROM alerts a
            JOIN master_entities me ON me.cin = a.cin
            WHERE a.id = $1
            """,
            alert_id,
        )
        if not row:
            return

        row_dict = dict(row)
        if row_dict.get("delivery_status") == "DELIVERED":
            return

        digest = _build_digest([row_dict])
        batch_id = str(uuid.uuid4())
        await self.pool.execute(
            "UPDATE alerts SET batch_id = $1 WHERE id = ANY($2)",
            batch_id,
            digest.alert_ids,
        )
        summary = await self.summarizer.summarize(digest)
        sent = await self.telegram.send(digest, summary)
        if not sent:
            await self._handle_telegram_failure(digest)
            return

        await self.pool.execute(
            """
            UPDATE alerts
            SET
                delivery_status = 'DELIVERED',
                ai_summary = $1,
                delivered_at = NOW(),
                delivery_channel = 'TELEGRAM',
                batch_id = $2
            WHERE id = ANY($3)
            """,
            summary,
            batch_id,
            digest.alert_ids,
        )

    async def _handle_telegram_failure(self, digest: AlertDigest) -> None:
        retry_count = await self.pool.fetchval(
            "SELECT COALESCE(MAX(retry_count), 0) FROM alerts WHERE id = ANY($1)",
            digest.alert_ids,
        )
        next_retry_count = int(retry_count or 0) + 1

        if next_retry_count >= 3:
            await self.pool.execute(
                "UPDATE alerts SET delivery_status = 'PERMANENTLY_FAILED' WHERE id = ANY($1)",
                digest.alert_ids,
            )
            await self.pool.execute(
                """
                INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
                VALUES ($1, 'routing', 'TELEGRAM_DELIVERY_FAILED', 'ALERT', NOW(), $2)
                """,
                digest.cin,
                json.dumps(
                    {
                        "watchlist_id": digest.watchlist_id,
                        "alert_ids": digest.alert_ids,
                        "company_name": digest.company_name,
                    }
                ),
            )
            return

        await self.pool.execute(
            """
            UPDATE alerts
            SET
                delivery_status = 'FAILED',
                retry_count = retry_count + 1
            WHERE id = ANY($1)
            """,
            digest.alert_ids,
        )


def _parse_payload(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _parse_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _is_eligible(row: dict) -> bool:
    status = row.get("delivery_status")
    severity = row.get("severity")
    retry_count = int(row.get("retry_count") or 0)

    if severity == "CRITICAL" and status == "FAILED" and retry_count < 3:
        return True
    if status != "PENDING":
        return False
    if severity == "CRITICAL":
        return True

    created_at = _parse_datetime(row["created_at"])
    now = datetime.now(timezone.utc)
    if severity == "ALERT":
        return created_at <= now - timedelta(hours=4)
    if severity == "WATCH":
        return created_at <= now - timedelta(hours=24)
    if severity == "INFO":
        return created_at <= now - timedelta(days=7)
    return False


def _build_digest(rows: list[dict]) -> AlertDigest:
    parsed = []
    for row in rows:
        payload = _parse_payload(row["alert_payload"])
        detected_at = payload["detected_at"]
        parsed.append((row, payload, _parse_datetime(detected_at)))

    parsed.sort(key=lambda item: item[2])
    oldest_payload = parsed[0][1]
    highest = max(
        parsed,
        key=lambda item: (SEVERITY_RANK[item[0]["severity"]], item[2]),
    )

    events = [
        EventSummary(
            event_id=int(payload["event_id"]),
            event_type=payload["event_type"],
            source=payload["source"],
            detected_at=payload["detected_at"],
            severity=payload["severity"],
            data_json=payload["data_json"],
            health_score_before=payload.get("health_score_before"),
            health_score_after=payload.get("health_score_after"),
        )
        for _, payload, _ in parsed
    ]

    health_score_current = int(parsed[-1][0]["health_score_current"] or 0)
    previous = oldest_payload.get("health_score_before")
    if previous is None:
        previous = health_score_current

    return AlertDigest(
        watchlist_id=int(parsed[0][0]["watchlist_id"]),
        cin=parsed[0][0]["cin"],
        company_name=parsed[0][0]["company_name"],
        company_state=parsed[0][0]["registered_state"],
        company_sector=parsed[0][0]["industrial_class"],
        events=events,
        health_score_current=health_score_current,
        health_score_previous=int(previous),
        health_band=parsed[-1][0]["health_band"],
        severity=highest[0]["severity"],
        contagion_result=highest[1].get("contagion_chain"),
        alert_ids=[int(item[0]["alert_id"]) for item in parsed],
    )
