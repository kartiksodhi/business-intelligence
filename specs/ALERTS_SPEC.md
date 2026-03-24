# ALERTS_SPEC.md

## What this is

The routing and delivery layer of the signal intelligence system. After the diff engine fires events into the `events` table, this layer matches them to watchlists, deduplicates within batch windows, builds digest payloads, calls Claude API for summaries at delivery time, and pushes to Telegram (CRITICAL only) and email digest (daily 7am).

Core principle: **one Claude API call per flush, never per event. Deduplicate before AI summary. Never block delivery on summary failure.**

---

## Tables involved (already exist — do NOT recreate schema)

- `events` — source of all detected signals: `cin`, `source`, `event_type`, `severity`, `detected_at`, `data_json`, `health_score_before`, `health_score_after`, `contagion_chain`
- `watchlists` — subscriber filters: `cin_list`, `state_filter`, `sector_filter`, `severity_min`, `signal_types`, `is_active`
- `alerts` — delivery receipts: `event_id`, `watchlist_id`, `cin`, `severity`, `alert_payload`, `ai_summary`, `delivered_at`, `delivery_channel`, `delivery_status`, `batch_id`
- `master_entities` — company golden record: `cin`, `company_name`, `registered_state`, `industrial_class`, `health_score`, `health_band`
- `cost_log` — API cost tracking: `log_date`, `service`, `operation`, `units`, `cost_inr`

---

## File layout

```
routing/
    __init__.py
    watchlist_matcher.py
    batch_flusher.py
    summarizer.py
    telegram_deliverer.py
    daily_digest.py
    scheduler.py
tests/
    test_alerts.py
```

---

## Environment variables required

```
TELEGRAM_BOT_TOKEN          # python-telegram-bot bot token
TELEGRAM_OPERATOR_CHAT_ID   # operator chat ID (integer as string)
ANTHROPIC_API_KEY           # Claude API key
OPERATOR_EMAIL              # digest recipient
RESEND_API_KEY              # Resend email API key
DATABASE_URL                # asyncpg DSN
```

---

## File: `routing/watchlist_matcher.py`

### Purpose

After every event inserted into `events`, run watchlist matching. Returns the list of watchlist IDs that match. For each match, insert one row into `alerts` with `delivery_status='PENDING'`.

### Interface

```python
from dataclasses import dataclass
from typing import List
import asyncpg


@dataclass
class EventRow:
    id: int
    cin: str
    event_type: str
    severity: str          # 'INFO' | 'WATCH' | 'ALERT' | 'CRITICAL'
    detected_at: str       # ISO timestamp
    data_json: dict
    health_score_before: int | None
    health_score_after: int | None
    contagion_chain: dict | None
    source: str


class WatchlistMatcher:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def match_event(self, event: EventRow) -> List[int]:
        """
        Match event against all active watchlists.
        Returns list of matched watchlist_ids.
        Inserts one alert row per match with delivery_status='PENDING'.
        """
        ...
```

### SQL — implement this query exactly

```sql
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
```

Parameters: `$1=event.cin`, `$2=event.severity`, `$3=event.event_type`

### Alert insert (one per matched watchlist_id)

```sql
INSERT INTO alerts (
    event_id,
    watchlist_id,
    cin,
    severity,
    alert_payload,
    delivery_status,
    created_at
) VALUES (
    $1,   -- event.id
    $2,   -- watchlist_id
    $3,   -- event.cin
    $4,   -- event.severity
    $5,   -- full event row as JSONB (including data_json, health scores, contagion_chain)
    'PENDING',
    NOW()
)
```

`alert_payload` must contain the full event data: `event_id`, `cin`, `event_type`, `severity`, `source`, `detected_at`, `data_json`, `health_score_before`, `health_score_after`, `contagion_chain`. Serialize as JSONB.

### Caller responsibility

The diff engine (or the event insert path) calls `match_event` after each event insert. CRITICAL severity: also immediately calls `batch_flusher.flush_critical(alert_id)` for each returned alert ID.

```python
matched_ids = await matcher.match_event(event)

if event.severity == 'CRITICAL':
    for alert_id in returned_alert_ids:
        await batch_flusher.flush_critical(alert_id)
```

The `match_event` method returns `watchlist_ids`. The caller retrieves the corresponding inserted `alert_ids` either from the INSERT RETURNING clause or by querying immediately after insert.

**Implementation note:** Use `INSERT ... RETURNING id` in the alert insert so `alert_ids` are immediately available without a follow-up SELECT.

---

## File: `routing/batch_flusher.py`

### Purpose

Collects PENDING alerts, groups by (watchlist_id, cin), builds AlertDigest per group, calls summarizer, delivers, marks DELIVERED. Run every 30 minutes by scheduler.

### Batch windows by severity

| Severity | Window | Behavior |
|---|---|---|
| CRITICAL | 0 seconds | Flush immediately on insert — `flush_critical()` |
| ALERT | 4 hours | Flush when `created_at <= NOW() - INTERVAL '4 hours'` |
| WATCH | 24 hours | Flush when `created_at <= NOW() - INTERVAL '24 hours'` |
| INFO | 7 days | Flush when `created_at <= NOW() - INTERVAL '7 days'` |

### Dataclasses

```python
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EventSummary:
    event_id: int
    event_type: str
    source: str
    detected_at: str        # ISO timestamp string
    severity: str
    data_json: dict
    health_score_before: int | None
    health_score_after: int | None


@dataclass
class AlertDigest:
    watchlist_id: int
    cin: str
    company_name: str
    company_state: str
    company_sector: str
    events: List[EventSummary]          # all events in this batch for this CIN
    health_score_current: int
    health_score_previous: int
    health_band: str
    severity: str                       # highest severity in batch
    contagion_result: Optional[dict]
    alert_ids: List[int]                # IDs to mark DELIVERED after send
```

Severity ranking for "highest in batch": CRITICAL > ALERT > WATCH > INFO.

### Interface

```python
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
        """
        Called every 30 minutes by scheduler.
        Finds all PENDING alerts whose batch window has expired.
        Groups by (watchlist_id, cin). Builds AlertDigest per group.
        Calls summarizer, delivers, marks DELIVERED.
        """
        ...

    async def flush_critical(self, alert_id: int) -> None:
        """
        Called immediately when a CRITICAL alert is inserted.
        Builds single-event AlertDigest for this alert_id.
        Delivers via Telegram immediately.
        Does NOT wait for 30-minute flush cycle.
        """
        ...
```

### Flush logic — step by step

**Step 1: Fetch eligible PENDING alerts**

```sql
SELECT
    a.id            AS alert_id,
    a.event_id,
    a.watchlist_id,
    a.cin,
    a.severity,
    a.alert_payload,
    a.created_at,
    me.company_name,
    me.registered_state,
    me.industrial_class,
    me.health_score  AS health_score_current,
    me.health_band
FROM alerts a
JOIN master_entities me ON me.cin = a.cin
WHERE a.delivery_status = 'PENDING'
AND (
    a.severity = 'CRITICAL'
    OR (a.severity = 'ALERT' AND a.created_at <= NOW() - INTERVAL '4 hours')
    OR (a.severity = 'WATCH' AND a.created_at <= NOW() - INTERVAL '24 hours')
    OR (a.severity = 'INFO'  AND a.created_at <= NOW() - INTERVAL '7 days')
)
ORDER BY a.watchlist_id, a.cin, a.created_at
```

**Step 2: Group rows by `(watchlist_id, cin)`**

Use Python `itertools.groupby` or a dict keyed on `(watchlist_id, cin)`. All rows in a group belong to one AlertDigest.

**Step 3: Build AlertDigest per group**

- `events`: one `EventSummary` per row in the group, sorted by `detected_at` ascending
- `health_score_previous`: take `health_score_before` from the oldest event in the group (`alert_payload->health_score_before`). If NULL, use `health_score_current`.
- `severity`: highest severity across all rows in the group
- `contagion_result`: take `contagion_chain` from the highest-severity event in the group. If multiple CRITICAL, take the most recent.
- `alert_ids`: list of all `alert_id` values in the group

**Step 4: Generate batch_id**

```python
import uuid
batch_id = str(uuid.uuid4())
```

Assign same `batch_id` to all `alert_ids` in this group (UPDATE alerts SET batch_id=$1 WHERE id = ANY($2)).

**Step 5: Summarize**

```python
summary = await self.summarizer.summarize(digest)
```

**Step 6: Deliver**

- If `digest.severity == 'CRITICAL'`: deliver via Telegram (`telegram.send(digest, summary)`)
- All severities: mark DELIVERED (daily digest pulls from DB, no immediate email per non-CRITICAL alert)

**Step 7: Mark DELIVERED**

```sql
UPDATE alerts
SET
    delivery_status = 'DELIVERED',
    ai_summary      = $1,
    delivered_at    = NOW(),
    delivery_channel = $2,
    batch_id        = $3
WHERE id = ANY($4)
```

`delivery_channel`: `'TELEGRAM'` for CRITICAL, `'DIGEST'` for all others.

**Step 8: On any unhandled exception per group**

Log the error. Do not mark DELIVERED. The group remains PENDING and will be retried on the next 30-minute flush cycle. Never let one group failure abort processing of other groups — wrap each group in its own try/except.

### flush_critical logic

1. Fetch the single alert row by `alert_id` (same columns as Step 1 query, filtered by `a.id = $1`).
2. Build a single-event AlertDigest from that row.
3. Call `summarizer.summarize(digest)`.
4. Call `telegram.send(digest, summary)`.
5. Mark DELIVERED with `delivery_channel='TELEGRAM'`.

If the alert is already DELIVERED (race condition from flush cycle), skip silently.

---

## File: `routing/summarizer.py`

### Purpose

One Claude API call per AlertDigest. Never pre-generate. On failure, use fallback — never block delivery.

### Interface

```python
import anthropic
import asyncpg


class AlertSummarizer:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.client = anthropic.AsyncAnthropic()  # reads ANTHROPIC_API_KEY from env

    async def summarize(self, digest: "AlertDigest") -> str:
        """
        Returns 3-sentence summary string.
        On API failure: returns fallback string, does not raise.
        """
        ...
```

### Prompt template — use this exactly

```python
events_list = "\n".join(
    f"- {e.event_type} | Source: {e.source} | {e.detected_at}"
    for e in digest.events
)

prompt = f"""You are a corporate intelligence analyst. Summarize this alert in exactly 3 sentences for a credit risk professional. Be factual, cite the specific signals, and state what action is warranted.

Company: {digest.company_name} ({digest.cin}) | Sector: {digest.company_sector} | State: {digest.company_state}
Health score: {digest.health_score_previous} → {digest.health_score_current} ({digest.health_band})
Events detected:
{events_list}

Rules: No hedging language. No "it appears" or "may indicate". State facts only. Max 60 words total."""
```

### API call parameters

```python
model = "claude-haiku-4-5-20251001"
max_tokens = 150
```

```python
response = await self.client.messages.create(
    model=model,
    max_tokens=max_tokens,
    messages=[{"role": "user", "content": prompt}],
)
summary = response.content[0].text.strip()
```

### Cost logging — after every successful API call

```sql
INSERT INTO cost_log (log_date, service, operation, units, cost_inr, metadata)
VALUES (CURRENT_DATE, 'claude_api', 'alert_summary', 1, 0.10, $1)
```

`metadata` JSONB: `{"cin": digest.cin, "watchlist_id": digest.watchlist_id, "event_count": len(digest.events)}`

### Fallback on any exception

```python
except Exception as e:
    logger.error(f"Claude API failed for {digest.cin}: {e}")
    primary_event = digest.events[0]
    return (
        f"{primary_event.event_type} detected for {digest.company_name} ({digest.cin}). "
        f"Source: {primary_event.source}. Operator review recommended."
    )
```

Do not log cost on failure. Do not re-raise. Return fallback string so delivery proceeds.

### Daily budget guard

Before calling the API, check today's Claude API spend:

```sql
SELECT COALESCE(SUM(cost_inr), 0)
FROM cost_log
WHERE log_date = CURRENT_DATE AND service = 'claude_api'
```

If total >= 450 (₹50 buffer below the ₹500 operator review threshold): skip API call, use fallback, log a WARNING. This prevents the daily budget guard in `CLAUDE.md` from triggering via runaway alert summarization.

---

## File: `routing/telegram_deliverer.py`

### Purpose

Deliver CRITICAL digests via Telegram. Retry up to 3 times across flush cycles. After 3 failures, mark permanently failed and log operator event.

### Interface

```python
from telegram import Bot
import asyncpg


class TelegramDeliverer:
    def __init__(self, pool: asyncpg.Pool):
        import os
        self.bot = Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])
        self.chat_id = int(os.environ["TELEGRAM_OPERATOR_CHAT_ID"])
        self.pool = pool

    async def send(self, digest: "AlertDigest", summary: str) -> bool:
        """
        Send Telegram message for CRITICAL digest.
        Returns True on success, False on failure.
        Does not raise — caller always gets a bool.
        """
        ...
```

### Message format — implement exactly

```python
primary_event = digest.events[0]  # most recent event by detected_at

text = (
    f"🚨 CRITICAL ALERT\n"
    f"{digest.company_name}\n"
    f"CIN: {digest.cin}\n\n"
    f"{summary}\n\n"
    f"Score: {digest.health_score_previous}→{digest.health_score_current} ({digest.health_band})\n"
    f"Source: {primary_event.source}\n"
    f"Detected: {primary_event.detected_at}"
)
```

### Send logic

```python
await self.bot.send_message(chat_id=self.chat_id, text=text)
return True
```

### On failure

1. Log the exception with `logger.error`.
2. Return `False`.
3. Do **not** retry in the same flush cycle.

Retry tracking is handled by the caller (BatchFlusher). The alert remains `PENDING` (or `FAILED` — see retry column below).

### Retry tracking

Add a `retry_count` column to `alerts` for tracking. Since the schema spec does not include this column, the implementation must add it via migration:

```sql
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS retry_count SMALLINT NOT NULL DEFAULT 0;
```

Include this migration in the module's `__init__.py` or a dedicated `routing/migrations.py` that runs at startup.

**Retry logic in BatchFlusher:**

When Telegram send returns `False`:

```sql
UPDATE alerts
SET
    delivery_status = 'FAILED',
    retry_count     = retry_count + 1
WHERE id = ANY($1)
```

On next flush cycle: alerts with `delivery_status='FAILED'` AND `retry_count < 3` are re-queued (treat same as PENDING for CRITICAL).

After `retry_count = 3`:

```sql
UPDATE alerts
SET delivery_status = 'PERMANENTLY_FAILED'
WHERE id = ANY($1)
```

Then insert operator notification into `events`:

```sql
INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
VALUES (
    $1,           -- digest.cin
    'routing',
    'TELEGRAM_DELIVERY_FAILED',
    'ALERT',
    NOW(),
    $2            -- {"watchlist_id": ..., "alert_ids": ..., "company_name": ...}
)
```

**Fetch query for retry-eligible CRITICAL alerts** — add to `flush()` Step 1 query:

```sql
OR (
    a.severity = 'CRITICAL'
    AND a.delivery_status = 'FAILED'
    AND a.retry_count < 3
)
```

---

## File: `routing/daily_digest.py`

### Purpose

Send daily email digest at 7am. Pulls last 24h delivered alerts plus any PENDING from prior days. Uses Resend SDK. On failure, logs to `events` table.

### Interface

```python
import resend
import asyncpg
import os
from datetime import datetime, timedelta


class DailyDigestSender:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        resend.api_key = os.environ["RESEND_API_KEY"]
        self.operator_email = os.environ["OPERATOR_EMAIL"]

    async def send_digest(self) -> None:
        """Called by scheduler at 7am daily."""
        ...
```

### Data queries

**Top 5 events (last 24h):**

```sql
SELECT
    me.company_name,
    e.event_type,
    e.severity,
    e.health_score_before,
    e.health_score_after,
    e.detected_at
FROM events e
JOIN master_entities me ON me.cin = e.cin
WHERE e.detected_at >= NOW() - INTERVAL '24 hours'
ORDER BY
    CASE e.severity
        WHEN 'CRITICAL' THEN 4
        WHEN 'ALERT'    THEN 3
        WHEN 'WATCH'    THEN 2
        WHEN 'INFO'     THEN 1
    END DESC,
    e.detected_at DESC
LIMIT 5
```

**Scraper status:**

```sql
SELECT
    source_id,
    status,
    last_pull_at,
    consecutive_failures,
    next_pull_at
FROM source_state
ORDER BY source_id
```

**Costs today:**

```sql
SELECT
    service,
    operation,
    SUM(cost_inr)  AS service_total,
    COUNT(*)       AS call_count
FROM cost_log
WHERE log_date = CURRENT_DATE
GROUP BY service, operation
ORDER BY service_total DESC
```

Also compute `total_today = SUM(cost_inr)` across all services for the day.

Claude API cost breakdown: filter rows where `service = 'claude_api'`.

**Accuracy (last 30 days):**

```sql
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
```

**Entity resolution queue count:**

```sql
SELECT COUNT(*) FROM entity_resolution_queue WHERE resolved = FALSE
```

### Email content

Render both plain text and HTML. The plain-text version is the canonical format; HTML wraps the same content.

**Plain text template:**

```
BI Engine — Daily Digest {date}

TOP 5 EVENTS (last 24h):
1. {company_name} — {event_type} — Score: {health_score_before}→{health_score_after}
2. ...
(If fewer than 5 events: show all available. If zero: "No events in last 24 hours.")

SCRAPER STATUS:
✓ {source_id}: OK, last pull {last_pull_at}
⚠ {source_id}: DEGRADED, {consecutive_failures} failures
✗ {source_id}: UNREACHABLE
(Status symbol mapping: OK=✓, DEGRADED=⚠, UNREACHABLE=✗, SCRAPER_BROKEN=✗)

COSTS (today):
Total: ₹{total_today:.2f}
Claude API: ₹{claude_cost:.2f} ({llm_calls} calls)
Budget remaining: ₹{500 - total_today:.2f}

ACCURACY (last 30d):
RED alerts: {total_red} | Confirmed: {confirmed_count} ({pct:.0f}%)
Top false positive: {top_false_positive_reason or "None recorded"}

ENTITY RESOLUTION QUEUE: {queue_count} items pending operator review
```

**Percentage calculation:**

```python
pct = (confirmed_count / total_red * 100) if total_red > 0 else 0.0
```

**HTML version:** Wrap the same content in a minimal `<html><body><pre>` block. No external CSS, no images. Plain monospace is sufficient.

### Resend send call

```python
params: resend.Emails.SendParams = {
    "from": "digest@yourdomain.com",
    "to": [self.operator_email],
    "subject": f"BI Engine — Daily Digest {datetime.now().strftime('%Y-%m-%d')}",
    "text": plain_text,
    "html": html_content,
}
resend.Emails.send(params)
```

### On failure

```python
except Exception as e:
    logger.error(f"Daily digest send failed: {e}")
    await self.pool.execute(
        """
        INSERT INTO events (source, event_type, severity, detected_at, data_json)
        VALUES ('routing', 'DIGEST_FAILED', 'ALERT', NOW(), $1)
        """,
        json.dumps({"error": str(e), "digest_date": datetime.now().date().isoformat()}),
    )
```

No re-raise. Log and continue.

---

## File: `routing/scheduler.py`

### Purpose

APScheduler setup. Integrated into FastAPI lifespan. Two jobs: 30-minute batch flush and 7am daily digest.

### Implementation

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncpg
import os


def create_scheduler(
    pool: asyncpg.Pool,
    batch_flusher: "BatchFlusher",
    daily_digest: "DailyDigestSender",
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        batch_flusher.flush,
        trigger="interval",
        minutes=30,
        id="batch_flush",
        replace_existing=True,
        misfire_grace_time=120,
    )

    scheduler.add_job(
        daily_digest.send_digest,
        trigger="cron",
        hour=7,
        minute=0,
        id="daily_digest",
        replace_existing=True,
        misfire_grace_time=300,
    )

    return scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])

    from routing.watchlist_matcher import WatchlistMatcher
    from routing.summarizer import AlertSummarizer
    from routing.telegram_deliverer import TelegramDeliverer
    from routing.batch_flusher import BatchFlusher
    from routing.daily_digest import DailyDigestSender

    summarizer = AlertSummarizer(pool)
    telegram = TelegramDeliverer(pool)
    flusher = BatchFlusher(pool, summarizer, telegram)
    digest = DailyDigestSender(pool)

    scheduler = create_scheduler(pool, flusher, digest)
    scheduler.start()

    app.state.pool = pool
    app.state.matcher = WatchlistMatcher(pool)
    app.state.flusher = flusher

    yield

    scheduler.shutdown(wait=False)
    await pool.close()
```

`misfire_grace_time`: if the scheduler was down during the scheduled run time, APScheduler will still execute the job up to this many seconds late. Set conservatively — 2 minutes for flush, 5 minutes for digest.

---

## Dependencies to install

```
python-telegram-bot>=20.0
anthropic>=0.25.0
resend>=0.7.0
apscheduler>=3.10.0
asyncpg>=0.29.0
```

Add to `requirements.txt`. All are free-tier compatible. No paid services beyond existing Resend and Anthropic accounts.

---

## Tests: `tests/test_alerts.py`

All delivery calls (Telegram send, Resend send, Claude API) must be mocked. Use `pytest-asyncio` and `unittest.mock.AsyncMock`.

### Setup pattern

```python
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from routing.watchlist_matcher import WatchlistMatcher, EventRow
from routing.batch_flusher import BatchFlusher, AlertDigest, EventSummary
from routing.summarizer import AlertSummarizer
from routing.telegram_deliverer import TelegramDeliverer
from routing.daily_digest import DailyDigestSender
```

Use a real asyncpg connection to a test database, or mock `pool.fetch` / `pool.fetchrow` / `pool.execute` with `AsyncMock` returning fixture data. Either approach is acceptable. Mock approach is preferred to avoid test DB dependency.

---

### Test 1: Watchlist match — CIN filter matches correctly

```python
@pytest.mark.asyncio
async def test_watchlist_cin_filter_matches():
    """
    Watchlist with cin_list=['U12345MH2001PTC123456'] matches event for that CIN.
    """
    pool = MagicMock()
    # mock pool.fetch to return one watchlist row with matching cin_list
    # mock pool.executemany / execute for alert insert
    # assert match_event returns [watchlist_id]
    ...
```

**Assert:** `match_event` returns a list containing exactly the watchlist ID whose `cin_list` includes the event CIN.

---

### Test 2: Watchlist match — state filter excludes non-matching event

```python
@pytest.mark.asyncio
async def test_watchlist_state_filter_excludes():
    """
    Watchlist with state_filter='MH' does not match event for a company in 'DL'.
    """
    # mock master_entities.registered_state = 'DL'
    # watchlist.state_filter = 'MH'
    # assert match_event returns []
    ...
```

**Assert:** `match_event` returns an empty list.

---

### Test 3: Watchlist match — severity_min=ALERT excludes INFO event

```python
@pytest.mark.asyncio
async def test_watchlist_severity_min_excludes_info():
    """
    Watchlist with severity_min='ALERT' does not match an INFO-severity event.
    """
    # event.severity = 'INFO'
    # watchlist.severity_min = 'ALERT'
    # assert match_event returns []
    ...
```

**Assert:** `match_event` returns an empty list. The severity numeric mapping must exclude INFO (rank 1) from ALERT threshold (rank 3).

---

### Test 4: Batch window — ALERT event not flushed until 4h window expires

```python
@pytest.mark.asyncio
async def test_alert_batch_window_not_expired():
    """
    PENDING ALERT created 2 hours ago is NOT included in flush output.
    """
    # mock pool.fetch to return one ALERT-severity alert created 2 hours ago
    # call batch_flusher.flush()
    # assert summarizer.summarize was NOT called
    # assert no UPDATE to alerts table
    ...
```

**Assert:** No delivery or DB update occurs for an ALERT alert within its 4-hour window.

---

### Test 5: CRITICAL flushed immediately, not batched

```python
@pytest.mark.asyncio
async def test_critical_flushed_immediately():
    """
    flush_critical(alert_id) delivers immediately without waiting for batch window.
    """
    # mock pool.fetchrow to return a CRITICAL alert just inserted (created_at=NOW())
    # mock summarizer.summarize to return a summary string
    # mock telegram.send to return True
    # call batch_flusher.flush_critical(alert_id=1)
    # assert telegram.send was called once
    # assert alerts UPDATE sets delivery_status='DELIVERED'
    ...
```

**Assert:** `telegram.send` called exactly once. `delivery_status` set to `'DELIVERED'`.

---

### Test 6: Same CIN, two events in window — grouped into one digest

```python
@pytest.mark.asyncio
async def test_same_cin_grouped_into_one_digest():
    """
    Two WATCH-severity events for same CIN in same 24h window → one AlertDigest.
    """
    # mock pool.fetch to return two WATCH alerts, same cin, same watchlist_id,
    # both created 25 hours ago (past window)
    # mock summarizer.summarize to return summary
    # call batch_flusher.flush()
    # assert summarizer.summarize called exactly once
    # assert the digest passed to summarizer has len(events) == 2
    ...
```

**Assert:** `summarizer.summarize` called once, not twice. The `AlertDigest.events` list has length 2.

---

### Test 7: Summarizer — Claude API called once per digest

```python
@pytest.mark.asyncio
async def test_summarizer_calls_api_once():
    """
    AlertSummarizer.summarize makes exactly one Anthropic API call per digest.
    """
    with patch("anthropic.AsyncAnthropic") as mock_anthropic:
        mock_client = AsyncMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create = AsyncMock(return_value=MagicMock(
            content=[MagicMock(text="Summary sentence one. Sentence two. Sentence three.")]
        ))

        pool = MagicMock()
        pool.execute = AsyncMock()  # cost_log insert
        pool.fetchval = AsyncMock(return_value=10.0)  # today's spend check

        summarizer = AlertSummarizer(pool)
        digest = _make_digest()  # helper that returns a valid AlertDigest

        result = await summarizer.summarize(digest)

        mock_client.messages.create.assert_called_once()
        assert len(result) > 0
```

---

### Test 8: Summarizer — fallback used when API fails, delivery not blocked

```python
@pytest.mark.asyncio
async def test_summarizer_fallback_on_api_failure():
    """
    When Claude API raises an exception, summarize returns fallback string without raising.
    """
    with patch("anthropic.AsyncAnthropic") as mock_anthropic:
        mock_client = AsyncMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create = AsyncMock(side_effect=Exception("API timeout"))

        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=0.0)

        summarizer = AlertSummarizer(pool)
        digest = _make_digest()

        result = await summarizer.summarize(digest)

        # must not raise, must return non-empty string
        assert isinstance(result, str)
        assert len(result) > 0
        # fallback must mention event_type and company name
        assert digest.events[0].event_type in result
        assert digest.company_name in result
```

---

### Test 9: Telegram — message format correct

```python
@pytest.mark.asyncio
async def test_telegram_message_format():
    """
    TelegramDeliverer sends message containing required fields in correct order.
    """
    with patch("telegram.Bot") as mock_bot_class:
        mock_bot = AsyncMock()
        mock_bot_class.return_value = mock_bot
        mock_bot.send_message = AsyncMock()

        with patch.dict("os.environ", {
            "TELEGRAM_BOT_TOKEN": "fake-token",
            "TELEGRAM_OPERATOR_CHAT_ID": "12345",
        }):
            pool = MagicMock()
            deliverer = TelegramDeliverer(pool)
            digest = _make_digest(severity="CRITICAL")
            summary = "Fact one. Fact two. Action required."

            result = await deliverer.send(digest, summary)

        assert result is True
        call_args = mock_bot.send_message.call_args
        text = call_args.kwargs["text"] if "text" in call_args.kwargs else call_args.args[1]
        assert "🚨 CRITICAL ALERT" in text
        assert digest.company_name in text
        assert digest.cin in text
        assert summary in text
        assert f"{digest.health_score_previous}→{digest.health_score_current}" in text
```

---

### Test 10: Telegram — after 3 failures → PERMANENTLY_FAILED status set

```python
@pytest.mark.asyncio
async def test_telegram_permanent_failure_after_3_retries():
    """
    After retry_count reaches 3, alert is marked PERMANENTLY_FAILED
    and an operator event is inserted.
    """
    # Setup: mock pool.fetch to return one CRITICAL alert with
    # delivery_status='FAILED', retry_count=2 (so this is the 3rd attempt)
    # Mock telegram.send to return False (failure)
    # Call batch_flusher.flush()
    # Assert: UPDATE alerts SET delivery_status='PERMANENTLY_FAILED' was called
    # Assert: INSERT INTO events with event_type='TELEGRAM_DELIVERY_FAILED' was called
    ...
```

**Assert:** After third consecutive failure, `delivery_status` is set to `'PERMANENTLY_FAILED'` and an `events` row with `event_type='TELEGRAM_DELIVERY_FAILED'` and `severity='ALERT'` is inserted.

---

### Test 11: Daily digest — pulls correct 24h window

```python
@pytest.mark.asyncio
async def test_daily_digest_24h_window():
    """
    send_digest queries events with detected_at >= NOW() - 24 hours.
    """
    with patch("resend.Emails.send") as mock_send:
        pool = MagicMock()
        # mock pool.fetch for top events query
        # mock pool.fetch for scraper status query
        # mock pool.fetch for cost query
        # mock pool.fetchrow for accuracy query
        # mock pool.fetchval for entity queue count

        sender = DailyDigestSender(pool)
        await sender.send_digest()

        # verify the events query was called with a 24h interval parameter
        # verify resend.Emails.send was called once
        mock_send.assert_called_once()
        call_args = mock_send.call_args[0][0]
        assert "Daily Digest" in call_args["subject"]
```

---

### Test 12: Daily digest — email failure logged to events table

```python
@pytest.mark.asyncio
async def test_daily_digest_failure_logged_to_events():
    """
    When Resend raises an exception, DailyDigestSender inserts DIGEST_FAILED event.
    """
    with patch("resend.Emails.send", side_effect=Exception("Resend timeout")):
        pool = MagicMock()
        # mock all pool.fetch calls to return empty results
        pool.execute = AsyncMock()

        sender = DailyDigestSender(pool)
        await sender.send_digest()  # must not raise

        # verify pool.execute was called with DIGEST_FAILED insert
        calls = [str(c) for c in pool.execute.call_args_list]
        assert any("DIGEST_FAILED" in c for c in calls)
```

---

## Helper: `_make_digest` for tests

```python
from routing.batch_flusher import AlertDigest, EventSummary

def _make_digest(severity: str = "ALERT") -> AlertDigest:
    return AlertDigest(
        watchlist_id=1,
        cin="U12345MH2001PTC123456",
        company_name="Acme Pvt Ltd",
        company_state="MH",
        company_sector="6201",
        events=[
            EventSummary(
                event_id=1,
                event_type="NCLT_FILING",
                source="nclt",
                detected_at="2026-03-16T10:00:00",
                severity=severity,
                data_json={"case_number": "IB/123/2026"},
                health_score_before=65,
                health_score_after=30,
            )
        ],
        health_score_current=30,
        health_score_previous=65,
        health_band="RED",
        severity=severity,
        contagion_result=None,
        alert_ids=[1],
    )
```

---

## Error handling summary

| Failure | Behavior |
|---|---|
| Watchlist SQL error | Log, raise — caller decides retry |
| Alert insert failure | Log, raise — event remains unmatched |
| Batch flush group failure | Log, skip group, continue to next group |
| Claude API failure | Return fallback string, log error, do not raise |
| Claude API budget guard | Return fallback string, log WARNING, do not call API |
| Telegram send failure | Return False, increment retry_count |
| Telegram 3rd failure | Mark PERMANENTLY_FAILED, insert operator event |
| Resend failure | Log error, insert DIGEST_FAILED event, do not raise |
| Scheduler job exception | APScheduler logs and continues — individual job failure does not stop scheduler |

---

## Constraints (from CLAUDE.md — never violate)

- **Algorithm before LLM.** Watchlist matching is pure SQL — no LLM involvement.
- **AI summary only at alert delivery.** Never pre-generate. `summarize()` is called inside `flush()` only.
- **Deduplicate before AI summary.** Group by (watchlist_id, cin) before calling `summarize()`. One call per group.
- **Max 500 Claude API calls/month.** The ₹450/day budget guard in `summarizer.py` is the enforcement mechanism.
- **Any day over ₹500 external API cost = operator review triggered.** The ₹450 guard triggers before this threshold.
- **Diff not reprocess.** This layer only processes events that the diff engine has already inserted — no re-scanning of sources.
- **Health score recomputes on event only.** `health_score_current` in AlertDigest is pulled from `master_entities.health_score`, which the health score engine has already updated. This layer does not recompute scores.
