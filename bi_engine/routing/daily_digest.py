from __future__ import annotations

import json
import logging
import os
import sys
import types
from datetime import datetime

import asyncpg

try:
    import resend
except ImportError:  # pragma: no cover
    resend = types.ModuleType("resend")

    class _FallbackEmails:
        @staticmethod
        def send(params):
            raise RuntimeError("resend package is not installed")

    resend.Emails = _FallbackEmails  # type: ignore[attr-defined]
    resend.api_key = None  # type: ignore[attr-defined]
    sys.modules["resend"] = resend


logger = logging.getLogger(__name__)


class DailyDigestSender:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        resend.api_key = os.environ["RESEND_API_KEY"]
        self.operator_email = os.environ["OPERATOR_EMAIL"]

    async def send_digest(self) -> None:
        try:
            top_events = await self.pool.fetch(
                """
                SELECT
                    COALESCE(me.company_name, e.cin, 'Unknown') AS company_name,
                    e.event_type,
                    e.severity,
                    e.source,
                    e.health_score_before,
                    e.health_score_after,
                    e.detected_at
                FROM events e
                LEFT JOIN master_entities me ON me.cin = e.cin
                WHERE e.detected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY
                    CASE e.severity
                        WHEN 'CRITICAL' THEN 4
                        WHEN 'ALERT'    THEN 3
                        WHEN 'WATCH'    THEN 2
                        WHEN 'INFO'     THEN 1
                        ELSE 0
                    END DESC,
                    e.detected_at DESC
                LIMIT 5
                """
            )
            scraper_status = await self.pool.fetch(
                """
                SELECT
                    source_id,
                    status,
                    last_pull_at,
                    consecutive_failures,
                    next_pull_at,
                    record_count
                FROM source_state
                ORDER BY
                    CASE
                        WHEN last_pull_at IS NULL                   THEN 3
                        WHEN status = 'blocked'                     THEN 2
                        WHEN consecutive_failures >= 1              THEN 1
                        ELSE 0
                    END DESC,
                    source_id
                """
            )
            cost_rows = await self.pool.fetch(
                """
                SELECT
                    service,
                    operation,
                    SUM(cost_inr)  AS service_total,
                    COUNT(*)       AS call_count
                FROM cost_log
                WHERE log_date = CURRENT_DATE
                GROUP BY service, operation
                ORDER BY service_total DESC
                """
            )
            accuracy = await self.pool.fetchrow(
                """
                SELECT
                    COUNT(*)                                        AS total_red,
                    COUNT(*) FILTER (WHERE confirmed = TRUE)        AS confirmed_count,
                    COUNT(*) FILTER (WHERE false_positive = TRUE)   AS false_positive_count,
                    (
                        SELECT false_positive_reason
                        FROM predictions
                        WHERE fired_at >= NOW() - INTERVAL '30 days'
                          AND false_positive = TRUE
                          AND false_positive_reason IS NOT NULL
                        GROUP BY false_positive_reason
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    )                                               AS top_false_positive_reason
                FROM predictions
                WHERE severity = 'CRITICAL'
                  AND fired_at >= NOW() - INTERVAL '30 days'
                """
            )
            queue_count = await self.pool.fetchval(
                "SELECT COUNT(*) FROM entity_resolution_queue WHERE resolved = FALSE"
            )

            total_today = sum(float(row["service_total"] or 0.0) for row in cost_rows)
            claude_rows = [row for row in cost_rows if row["service"] == "claude_api"]
            claude_cost = sum(float(row["service_total"] or 0.0) for row in claude_rows)
            llm_calls = sum(int(row["call_count"] or 0) for row in claude_rows)

            total_red = int((accuracy or {}).get("total_red", 0) if accuracy else 0)
            confirmed_count = int((accuracy or {}).get("confirmed_count", 0) if accuracy else 0)
            top_false_positive_reason = (
                (accuracy or {}).get("top_false_positive_reason") if accuracy else None
            )
            pct = (confirmed_count / total_red * 100) if total_red > 0 else 0.0

            # Bucket scrapers: problem ones go to detail list, rest are counted
            green = amber = red = grey = 0
            problem_scrapers: list[dict] = []
            for row in scraper_status:
                status = (row["status"] or "").lower()
                failures = row["consecutive_failures"] or 0
                if row["last_pull_at"] is None:
                    grey += 1
                elif status == "blocked":
                    red += 1
                    problem_scrapers.append({"id": row["source_id"], "label": "BLOCKED", "color": "red"})
                elif failures >= 3:
                    red += 1
                    problem_scrapers.append({"id": row["source_id"], "label": f"{failures} failures", "color": "red"})
                elif failures >= 1:
                    amber += 1
                    problem_scrapers.append({"id": row["source_id"], "label": f"{failures} failure(s)", "color": "amber"})
                else:
                    green += 1

            date_str = datetime.now().strftime("%Y-%m-%d")
            threshold = float(os.environ.get("ALERT_THRESHOLD_INR", "500"))
            budget_remaining = threshold - total_today
            from_addr = os.environ.get("DIGEST_FROM_EMAIL", os.environ.get("OPERATOR_EMAIL", "digest@bi-engine.internal"))

            # Plain text version
            lines = [f"BI Engine — Daily Digest {date_str}", ""]
            lines.append("TOP 5 EVENTS (last 24h):")
            if top_events:
                for index, row in enumerate(top_events, start=1):
                    score = f"{row['health_score_before']}→{row['health_score_after']}" if row["health_score_before"] is not None else "n/a"
                    lines.append(f"{index}. [{row['severity']}] {row['company_name']} — {row['event_type']} ({row['source']}) Score: {score}")
            else:
                lines.append("No events in last 24 hours.")
            lines.append("")
            lines.append(f"SCRAPERS: {green} green  {amber} amber  {red} red  {grey} never ran")
            if problem_scrapers:
                lines.append("Problems:")
                for s in problem_scrapers:
                    lines.append(f"  ✗ {s['id']}: {s['label']}")
            lines.append("")
            lines.append("COSTS (today):")
            lines.append(f"  Total: ₹{total_today:.2f}  |  Budget remaining: ₹{budget_remaining:.2f}")
            lines.append(f"  Claude API: ₹{claude_cost:.2f} ({llm_calls} calls)")
            lines.append("")
            lines.append("ACCURACY (last 30d):")
            lines.append(f"  RED alerts: {total_red}  |  Confirmed: {confirmed_count} ({pct:.0f}%)")
            lines.append(f"  Top false positive: {top_false_positive_reason or 'None recorded'}")
            lines.append("")
            lines.append(f"ENTITY RESOLUTION QUEUE: {int(queue_count or 0)} pending")
            plain_text = "\n".join(lines)

            # HTML version
            sev_colors = {"CRITICAL": "#ff4136", "ALERT": "#ff851b", "WATCH": "#ffb700", "INFO": "#aaa"}
            event_rows_html = ""
            if top_events:
                for row in top_events:
                    sc = sev_colors.get(row["severity"], "#aaa")
                    score = f"{row['health_score_before']}→{row['health_score_after']}" if row["health_score_before"] is not None else "n/a"
                    event_rows_html += (
                        f'<tr><td style="color:{sc};font-weight:bold">{row["severity"]}</td>'
                        f'<td>{row["company_name"]}</td>'
                        f'<td>{row["event_type"]}</td>'
                        f'<td>{row["source"]}</td>'
                        f'<td>{score}</td></tr>'
                    )
            else:
                event_rows_html = '<tr><td colspan="5" style="color:#888">No events in last 24 hours.</td></tr>'

            problem_html = ""
            if problem_scrapers:
                for s in problem_scrapers:
                    color = "#ff4136" if s["color"] == "red" else "#ffb700"
                    problem_html += f'<li style="color:{color}">{s["id"]}: {s["label"]}</li>'
                problem_html = f"<ul style='margin:4px 0 0 16px;padding:0'>{problem_html}</ul>"

            budget_color = "#ff4136" if budget_remaining < 50 else "#2ecc40"
            html_content = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="background:#111;color:#ddd;font-family:monospace;padding:20px;margin:0">
<h2 style="color:#fff;margin-bottom:4px">BI Engine — Daily Digest {date_str}</h2>
<p style="color:#888;font-size:12px;margin-top:0">{datetime.now().strftime('%H:%M UTC')}</p>

<h3 style="color:#aaa;border-bottom:1px solid #333;padding-bottom:4px">Top 5 Events (last 24h)</h3>
<table style="width:100%;border-collapse:collapse;font-size:13px">
<thead><tr style="color:#666;font-size:11px">
  <th align="left">SEV</th><th align="left">COMPANY</th><th align="left">EVENT</th><th align="left">SOURCE</th><th align="left">SCORE</th>
</tr></thead>
<tbody>{event_rows_html}</tbody>
</table>

<h3 style="color:#aaa;border-bottom:1px solid #333;padding-bottom:4px;margin-top:24px">Scrapers</h3>
<div style="display:flex;gap:16px;font-size:13px">
  <span style="color:#2ecc40">&#9679; {green} green</span>
  <span style="color:#ffb700">&#9679; {amber} amber</span>
  <span style="color:#ff4136">&#9679; {red} red</span>
  <span style="color:#888">&#9679; {grey} never ran</span>
</div>
{problem_html}

<h3 style="color:#aaa;border-bottom:1px solid #333;padding-bottom:4px;margin-top:24px">Costs Today</h3>
<p style="font-size:13px;margin:4px 0">
  Total: <b>₹{total_today:.2f}</b> &nbsp;|&nbsp;
  Budget remaining: <b style="color:{budget_color}">₹{budget_remaining:.2f}</b><br>
  Claude API: ₹{claude_cost:.2f} ({llm_calls} calls)
</p>

<h3 style="color:#aaa;border-bottom:1px solid #333;padding-bottom:4px;margin-top:24px">Accuracy (last 30d)</h3>
<p style="font-size:13px;margin:4px 0">
  RED alerts: {total_red} &nbsp;|&nbsp; Confirmed: {confirmed_count} ({pct:.0f}%)<br>
  Top false positive: {top_false_positive_reason or 'None recorded'}
</p>

<h3 style="color:#aaa;border-bottom:1px solid #333;padding-bottom:4px;margin-top:24px">Entity Resolution Queue</h3>
<p style="font-size:13px;margin:4px 0">
  <b style="color:#ffb700">{int(queue_count or 0)}</b> signals pending CIN match
</p>
</body></html>"""

            params = {
                "from": from_addr,
                "to": [self.operator_email],
                "subject": f"BI Engine — Daily Digest {date_str}",
                "text": plain_text,
                "html": html_content,
            }
            resend.Emails.send(params)
        except Exception as exc:
            logger.error("Daily digest send failed: %s", exc)
            await self.pool.execute(
                """
                INSERT INTO events (source, event_type, severity, detected_at, data_json)
                VALUES ('routing', 'DIGEST_FAILED', 'ALERT', NOW(), $1)
                """,
                json.dumps(
                    {
                        "error": str(exc),
                        "digest_date": datetime.now().date().isoformat(),
                    }
                ),
            )
