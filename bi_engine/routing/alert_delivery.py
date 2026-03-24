from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

import asyncpg
import httpx
from pydantic import BaseModel, ValidationError, field_validator

try:
    import anthropic
except ImportError:  # pragma: no cover
    anthropic = None


logger = logging.getLogger(__name__)

ALERT_SYNTHESIZER_SYSTEM_PROMPT = (
    "You are a quantitative distressed-asset analyst. You are provided with a JSON array "
    "of verified events for a Corporate Identification Number (CIN).\n"
    "Rule 1: Output exactly three concise bullet points.\n"
    "Rule 2: State the facts, the immediate financial implication, and the contagion risk "
    "based ONLY on the provided JSON.\n"
    "Rule 3: STRICT PROHIBITION: Do not introduce outside knowledge, assume outcomes, or "
    'use emotional adjectives (e.g., "massive", "terrible", "doomed"). Keep the tone '
    "clinical, objective, and financially rigorous."
)

SEVERITY_RANK = {
    "INFO": 0,
    "WATCH": 1,
    "AMBER": 2,
    "ALERT": 3,
    "RED": 3,
    "CRITICAL": 4,
}


def _normalize_severity(severity: str) -> str:
    normalized = severity.upper()
    if normalized == "ALERT":
        return "RED"
    return normalized


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _fallback_summary(
    company_name: str,
    cin: str,
    severity: str,
    event_type: str,
    events: Sequence[dict[str, Any]],
) -> str:
    primary = events[0] if events else {}
    source = primary.get("source", "unknown source")
    return "\n".join(
        [
            f"- {severity} signal recorded for {company_name} ({cin}).",
            f"- Primary event: {event_type} from {source}.",
            "- Review the exposure, counterparties, and recent filings immediately.",
        ]
    )


class BulletSummary(BaseModel):
    bullets: list[str]

    @field_validator("bullets")
    @classmethod
    def validate_bullets(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if len(cleaned) != 3:
            raise ValueError("Expected exactly three bullet points.")
        if any(not item.startswith("- ") for item in cleaned):
            raise ValueError("Each line must start with '- '.")
        return cleaned

    @classmethod
    def from_text(cls, text: str) -> "BulletSummary":
        return cls(bullets=text.splitlines())

    def as_text(self) -> str:
        return "\n".join(self.bullets)


class AlertSynthesizer:
    def __init__(self, client: Any | None = None):
        self.client = client
        if self.client is None and anthropic is not None:
            self.client = anthropic.AsyncAnthropic()

    async def summarize_events(
        self,
        *,
        company_name: str,
        cin: str,
        severity: str,
        event_type: str,
        events: Sequence[dict[str, Any]],
    ) -> str:
        if not self.client:
            return _fallback_summary(company_name, cin, severity, event_type, events)

        try:
            response = await self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                system=ALERT_SYNTHESIZER_SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": json.dumps(list(events), default=_json_default),
                    }
                ],
            )
            raw_text = response.content[0].text.strip()
            return BulletSummary.from_text(raw_text).as_text()
        except ValidationError as exc:
            logger.warning("Alert summary validation failed for %s: %s", cin, exc)
        except Exception as exc:
            logger.error("Alert summary generation failed for %s: %s", cin, exc)

        return _fallback_summary(company_name, cin, severity, event_type, events)


class TelegramChannel:
    def __init__(self, client_factory: type[httpx.AsyncClient] = httpx.AsyncClient):
        self.client_factory = client_factory

    async def send(
        self,
        *,
        company_name: str,
        cin: str,
        severity: str,
        event_type: str,
    ) -> bool:
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            logger.warning("Telegram credentials are not configured.")
            return False

        message = "\n".join(
            [
                f"{severity}: {company_name} ({cin})",
                f"Event: {event_type}",
                "Action: review immediately",
            ]
        )
        try:
            async with self.client_factory(timeout=10.0) as client:
                response = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": message},
                )
                response.raise_for_status()
            return True
        except Exception as exc:
            logger.error("Telegram delivery failed for %s: %s", cin, exc)
            return False


class BrevoEmailChannel:
    def __init__(self, client_factory: type[httpx.AsyncClient] = httpx.AsyncClient):
        self.client_factory = client_factory

    async def send(
        self,
        *,
        to_email: str,
        company_name: str,
        cin: str,
        severity: str,
        summary: str,
    ) -> bool:
        api_key = os.environ.get("BREVO_API_KEY")
        operator_email = os.environ.get("OPERATOR_EMAIL")
        if not api_key or not operator_email:
            logger.warning("Brevo credentials are not configured.")
            return False

        payload = {
            "sender": {"email": operator_email},
            "to": [{"email": operator_email}],
            "subject": f"[BI Alert] {severity} — {company_name} ({cin})",
            "htmlContent": summary.replace("\n", "<br>"),
        }
        headers = {
            "api-key": api_key,
            "Content-Type": "application/json",
        }

        try:
            async with self.client_factory(timeout=10.0) as client:
                response = await client.post(
                    "https://api.brevo.com/v3/smtp/email",
                    json=payload,
                    headers=headers,
                )
                response.raise_for_status()
            return True
        except Exception as exc:
            logger.error("Brevo delivery failed for %s: %s", cin, exc)
            return False


@dataclass(frozen=True)
class SubscriberRecord:
    id: int
    name: str
    email: str
    severity_threshold: str


class AlertDeliveryService:
    def __init__(
        self,
        pool: asyncpg.Pool,
        synthesizer: AlertSynthesizer | None = None,
        telegram: TelegramChannel | None = None,
        email: BrevoEmailChannel | None = None,
    ):
        self.pool = pool
        self.synthesizer = synthesizer or AlertSynthesizer()
        self.telegram = telegram or TelegramChannel()
        self.email = email or BrevoEmailChannel()

    async def ensure_tables(self) -> None:
        async with self.pool.acquire() as conn:
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

    async def was_delivered_recently(
        self,
        *,
        subscriber_id: int,
        cin: str,
        event_type: str,
    ) -> bool:
        await self.ensure_tables()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id
                FROM delivered_alerts
                WHERE subscriber_id = $1
                  AND cin = $2
                  AND event_type = $3
                  AND delivered_at >= NOW() - INTERVAL '24 hours'
                LIMIT 1
                """,
                subscriber_id,
                cin,
                event_type,
            )
        return row is not None

    async def record_delivery(
        self,
        *,
        subscriber_id: int,
        cin: str,
        event_type: str,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO delivered_alerts (subscriber_id, cin, event_type, delivered_at)
                VALUES ($1, $2, $3, NOW())
                """,
                subscriber_id,
                cin,
                event_type,
            )

    async def fetch_company_events(self, cin: str) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT detected_at AS event_date, event_type, severity, source, data_json AS notes
                FROM events
                WHERE cin = $1
                ORDER BY detected_at DESC
                LIMIT 100
                """,
                cin,
            )
        return [dict(row) for row in rows]

    async def deliver_to_subscriber(
        self,
        *,
        subscriber: SubscriberRecord,
        cin: str,
        company_name: str,
        severity: str,
        event_type: str,
        events: Sequence[dict[str, Any]],
    ) -> bool:
        normalized_severity = _normalize_severity(severity)
        normalized_threshold = _normalize_severity(subscriber.severity_threshold)
        if SEVERITY_RANK.get(normalized_severity, -1) < SEVERITY_RANK.get(normalized_threshold, -1):
            return False

        if await self.was_delivered_recently(
            subscriber_id=subscriber.id,
            cin=cin,
            event_type=event_type,
        ):
            return False

        summary = await self.synthesizer.summarize_events(
            company_name=company_name,
            cin=cin,
            severity=normalized_severity,
            event_type=event_type,
            events=events,
        )

        delivered = False
        if normalized_severity == "CRITICAL":
            delivered = await self.telegram.send(
                company_name=company_name,
                cin=cin,
                severity=normalized_severity,
                event_type=event_type,
            ) or delivered
        if normalized_severity in {"RED", "CRITICAL"}:
            delivered = await self.email.send(
                to_email=subscriber.email,
                company_name=company_name,
                cin=cin,
                severity=normalized_severity,
                summary=summary,
            ) or delivered

        if delivered:
            await self.record_delivery(
                subscriber_id=subscriber.id,
                cin=cin,
                event_type=event_type,
            )

        return delivered

    async def deliver_for_event(
        self,
        *,
        cin: str,
        company_name: str,
        severity: str,
        event_type: str,
    ) -> int:
        await self.ensure_tables()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT s.id, s.name, s.email, s.severity_threshold
                FROM subscribers s
                JOIN watchlists w ON w.subscriber_id = s.id
                WHERE w.cin = $1
                """,
                cin,
            )

        events = await self.fetch_company_events(cin)
        delivered_count = 0
        for row in rows:
            subscriber = SubscriberRecord(
                id=int(row["id"]),
                name=row["name"],
                email=row["email"],
                severity_threshold=row["severity_threshold"],
            )
            if await self.deliver_to_subscriber(
                subscriber=subscriber,
                cin=cin,
                company_name=company_name,
                severity=severity,
                event_type=event_type,
                events=events,
            ):
                delivered_count += 1
        return delivered_count
