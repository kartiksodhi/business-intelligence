from __future__ import annotations

import json
import logging

import anthropic
import asyncpg


logger = logging.getLogger(__name__)


class AlertSummarizer:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.client = anthropic.AsyncAnthropic()

    async def summarize(self, digest: "AlertDigest") -> str:
        try:
            spend_today = await self.pool.fetchval(
                """
                SELECT COALESCE(SUM(cost_inr), 0)
                FROM cost_log
                WHERE log_date = CURRENT_DATE AND service = 'claude_api'
                """
            )
            if float(spend_today or 0.0) >= 450:
                logger.warning("Claude API budget guard triggered for %s", digest.cin)
                return _fallback_summary(digest)

            events_list = "\n".join(
                f"- {event.event_type} | Source: {event.source} | {event.detected_at}"
                for event in digest.events
            )
            prompt = f"""You are a corporate intelligence analyst. Summarize this alert in exactly 3 sentences for a credit risk professional. Be factual, cite the specific signals, and state what action is warranted.

Company: {digest.company_name} ({digest.cin}) | Sector: {digest.company_sector} | State: {digest.company_state}
Health score: {digest.health_score_previous} → {digest.health_score_current} ({digest.health_band})
Events detected:
{events_list}

Rules: No hedging language. No "it appears" or "may indicate". State facts only. Max 60 words total."""

            response = await self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            summary = response.content[0].text.strip()

            await self.pool.execute(
                """
                INSERT INTO cost_log (log_date, service, operation, units, cost_inr, metadata)
                VALUES (CURRENT_DATE, 'claude_api', 'alert_summary', 1, 0.10, $1)
                """,
                json.dumps(
                    {
                        "cin": digest.cin,
                        "watchlist_id": digest.watchlist_id,
                        "event_count": len(digest.events),
                    }
                ),
            )
            return summary
        except Exception as exc:
            logger.error("Claude API failed for %s: %s", digest.cin, exc)
            return _fallback_summary(digest)


def _fallback_summary(digest: "AlertDigest") -> str:
    primary_event = digest.events[0]
    return (
        f"{primary_event.event_type} detected for {digest.company_name} ({digest.cin}). "
        f"Source: {primary_event.source}. Operator review recommended."
    )
