# OPERATOR_CLI_SPEC.md

## Spec owner: Claude Code
## Implementor: Codex
## Status: Ready for implementation
## Last updated: 2026-03-16

---

## Overview

The Operator CLI is the internal control plane for the signal intelligence system. It is a FastAPI application that exposes all `/op/` endpoints for direct operator interaction: monitoring source health, reviewing events, checking company health scores, managing watchlists, triggering enrichment, resolving entity queue items, and tracking costs.

There is no subscriber auth in Phase 1. All `/op/` endpoints are open to any caller on the internal network. Auth is deferred to Phase 2. Every endpoint must leave a placeholder comment marking where auth will be inserted.

This file is the complete implementation spec. Codex must implement exactly what is described here. Every file path, function signature, SQL query, and Pydantic model is specified. Raise any uncertainty with Claude Code before implementation begins. Do not make architectural decisions not specified here.

---

## Dependencies

Add these to `requirements.txt` if not already present:

```
fastapi>=0.111.0
uvicorn[standard]>=0.29.0
asyncpg>=0.29.0
pydantic>=2.7.0
python-dotenv>=1.0.1
pytest>=8.1.0
pytest-asyncio>=0.23.6
httpx>=0.27.0
```

---

## File structure

```
api/
  main.py               # FastAPI app init, lifespan, router registration
  dependencies.py       # DB pool dependency (get_db)
  models.py             # All Pydantic request/response models
  routers/
    operator.py         # All /op/ endpoints
tests/
  test_operator_api.py  # All 9 pytest cases, mocked DB
```

---

## Environment variables

```
DATABASE_URL=postgresql://user:password@localhost:5432/bi
ALERT_THRESHOLD_INR=500
```

`DATABASE_URL` is required. App must fail at startup with a clear error if it is not set.

---

## Full implementation: `api/dependencies.py`

```python
"""
FastAPI dependencies.

Provides the asyncpg connection pool via get_db().
Pool is created once at startup via the lifespan context manager in main.py
and stored on app.state.pool.
"""

from __future__ import annotations

import logging
from typing import AsyncGenerator

import asyncpg
from fastapi import Request

logger = logging.getLogger(__name__)


async def get_db(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Yield a single asyncpg connection from the pool.
    Released automatically after the request completes.
    """
    async with request.app.state.pool.acquire() as conn:
        yield conn
```

---

## Full implementation: `api/models.py`

```python
"""
Pydantic request and response models for all /op/ endpoints.

All models use Pydantic v2 (model_config, not class Config).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# GET /op/status
# ---------------------------------------------------------------------------

class SourceStatusItem(BaseModel):
    source_id: str
    status: str
    last_pull_at: Optional[datetime]
    record_count: Optional[int]
    consecutive_failures: int
    next_pull_at: Optional[datetime]
    lag_hours: Optional[float]
    """
    None  → source is on time (next_pull_at is in the future, or null).
    float → hours behind cadence (next_pull_at is in the past).
    """


# ---------------------------------------------------------------------------
# GET /op/events/today
# ---------------------------------------------------------------------------

class EventItem(BaseModel):
    id: int
    cin: Optional[str]
    source: str
    event_type: str
    severity: str
    detected_at: datetime
    health_score_before: Optional[int]
    health_score_after: Optional[int]
    data_json_summary: str
    """First 200 characters of the raw data_json, cast to string."""


# ---------------------------------------------------------------------------
# GET /op/health/{cin}
# ---------------------------------------------------------------------------

class HealthComponent(BaseModel):
    raw: float
    weighted: float


class HealthComponents(BaseModel):
    filing_freshness: HealthComponent
    director_stability: HealthComponent
    legal_risk: HealthComponent
    financial_health: HealthComponent
    capital_trajectory: HealthComponent


class DirectorItem(BaseModel):
    din: str
    director_name: Optional[str]
    designation: Optional[str]
    date_of_appointment: Optional[Any]
    is_active: bool


class LegalCaseItem(BaseModel):
    id: int
    case_type: str
    case_number: Optional[str]
    court: Optional[str]
    filing_date: Optional[Any]
    status: Optional[str]
    amount_involved: Optional[int]


class CompanyHealthResponse(BaseModel):
    cin: str
    company_name: str
    health_score: int
    health_band: Optional[str]
    last_computed_at: Optional[datetime]
    components: HealthComponents
    recent_events: list[EventItem]
    active_legal_cases: list[LegalCaseItem]
    directors: list[DirectorItem]


# ---------------------------------------------------------------------------
# GET /op/sources/lag
# ---------------------------------------------------------------------------

class SourceLagItem(BaseModel):
    source_id: str
    expected_next_pull: Optional[datetime]
    actual_lag_hours: float
    status: str


# ---------------------------------------------------------------------------
# GET /op/accuracy
# ---------------------------------------------------------------------------

class FalsePositiveCause(BaseModel):
    event_type: str
    count: int


class AccuracyResponse(BaseModel):
    window_days: int
    total_red_alerts: int
    confirmed: int
    false_positives: int
    expired_unconfirmed: int
    accuracy_pct: float
    top_false_positive_causes: list[FalsePositiveCause]


# ---------------------------------------------------------------------------
# GET /op/costs/today
# ---------------------------------------------------------------------------

class CostBreakdownItem(BaseModel):
    service: str
    operation: Optional[str]
    units: Optional[int]
    cost_inr: float


class CostsTodayResponse(BaseModel):
    date: str
    total_inr: float
    breakdown: list[CostBreakdownItem]
    alert_threshold_inr: float
    threshold_breached: bool


# ---------------------------------------------------------------------------
# POST /op/watchlist
# ---------------------------------------------------------------------------

class WatchlistCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    cin_list: Optional[list[str]] = None
    state_filter: Optional[str] = None
    sector_filter: Optional[str] = None
    severity_min: Optional[str] = "WATCH"
    signal_types: Optional[list[str]] = None


class WatchlistCreateResponse(BaseModel):
    id: int
    name: str
    cin_list: Optional[list[str]]
    state_filter: Optional[str]
    sector_filter: Optional[str]
    severity_min: str
    signal_types: Optional[list[str]]
    is_active: bool
    created_at: datetime


# ---------------------------------------------------------------------------
# POST /op/enrich/{cin}
# ---------------------------------------------------------------------------

class EnrichResponse(BaseModel):
    cin: str
    status: str
    message: str


# ---------------------------------------------------------------------------
# POST /op/resolve
# ---------------------------------------------------------------------------

class ResolveRequest(BaseModel):
    queue_id: int
    resolved_cin: str


class ResolveResponse(BaseModel):
    queue_id: int
    resolved_cin: str
    message: str


# ---------------------------------------------------------------------------
# POST /op/recalibrate
# ---------------------------------------------------------------------------

class RecalibrateResponse(BaseModel):
    status: str
    message: str


# ---------------------------------------------------------------------------
# POST /op/captcha/solve
# ---------------------------------------------------------------------------

class CaptchaSolveRequest(BaseModel):
    source_id: str
    solution: str


class CaptchaSolveResponse(BaseModel):
    status: str
    source_id: str
```

---

## Full implementation: `api/main.py`

```python
"""
FastAPI application entry point.

Creates the asyncpg connection pool at startup via the lifespan context
manager and mounts all routers.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from api.routers.operator import router as operator_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Create the asyncpg pool on startup. Tear it down on shutdown.
    Fails fast if DATABASE_URL is not set — no silent misconfiguration.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")

    logger.info("Creating asyncpg connection pool.")
    app.state.pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    logger.info("Connection pool created.")

    yield

    logger.info("Closing asyncpg connection pool.")
    await app.state.pool.close()
    logger.info("Connection pool closed.")


app = FastAPI(
    title="Business Intelligence — Operator API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(operator_router)
```

---

## Full implementation: `api/routers/operator.py`

```python
"""
Operator CLI endpoints.

All routes are under /op/ prefix. No authentication in Phase 1.
# TODO Phase 2: add operator token auth here

In-memory CAPTCHA solution store:
  _captcha_solutions: dict[source_id, solution_string]
Scrapers poll this dict via a shared import. When a solution is present,
the scraper reads it, clears the entry, and resumes.
"""

from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Annotated, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_db
from api.models import (
    AccuracyResponse,
    CaptchaSolveRequest,
    CaptchaSolveResponse,
    CompanyHealthResponse,
    CostsTodayResponse,
    DirectorItem,
    EnrichResponse,
    EventItem,
    FalsePositiveCause,
    HealthComponent,
    HealthComponents,
    LegalCaseItem,
    RecalibrateResponse,
    ResolveRequest,
    ResolveResponse,
    SourceLagItem,
    SourceStatusItem,
    WatchlistCreateRequest,
    WatchlistCreateResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/op", tags=["operator"])

# ---------------------------------------------------------------------------
# In-memory CAPTCHA solution store.
# Keyed by source_id. Scrapers import and poll this dict.
# ---------------------------------------------------------------------------
_captcha_solutions: dict[str, str] = {}


def _db_error(e: Exception) -> HTTPException:
    """
    Log the full traceback, return a sanitized 500.
    Never expose raw SQL errors or internal detail to the response.
    """
    logger.error("DB error:\n%s", traceback.format_exc())
    return HTTPException(status_code=500, detail={"error": "Internal server error."})


def _lag_hours(next_pull_at: Optional[datetime]) -> Optional[float]:
    """
    Return positive float if next_pull_at is in the past (source is behind).
    Return None if next_pull_at is null or in the future.
    """
    if next_pull_at is None:
        return None
    now = datetime.now(timezone.utc)
    # asyncpg returns naive datetimes from TIMESTAMP columns — make aware.
    if next_pull_at.tzinfo is None:
        next_pull_at = next_pull_at.replace(tzinfo=timezone.utc)
    delta = (now - next_pull_at).total_seconds() / 3600
    return round(delta, 2) if delta > 0 else None


# ---------------------------------------------------------------------------
# GET /op/status
# ---------------------------------------------------------------------------

@router.get("/status", response_model=list[SourceStatusItem])
async def get_status(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Returns health of all monitored sources from source_state.
    lag_hours is null if the source is on schedule, positive float if behind.
    """
    # TODO Phase 2: add operator token auth here
    try:
        rows = await db.fetch(
            """
            SELECT
                source_id,
                status,
                last_pull_at,
                record_count,
                consecutive_failures,
                next_pull_at
            FROM source_state
            ORDER BY source_id
            """
        )
    except Exception as e:
        raise _db_error(e)

    return [
        SourceStatusItem(
            source_id=row["source_id"],
            status=row["status"],
            last_pull_at=row["last_pull_at"],
            record_count=row["record_count"],
            consecutive_failures=row["consecutive_failures"],
            next_pull_at=row["next_pull_at"],
            lag_hours=_lag_hours(row["next_pull_at"]),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /op/events/today
# ---------------------------------------------------------------------------

@router.get("/events/today", response_model=list[EventItem])
async def get_events_today(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    severity: Optional[str] = Query(default=None, description="Filter by severity: INFO, WATCH, ALERT, CRITICAL"),
    source: Optional[str] = Query(default=None, description="Filter by source_id"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    Returns all events detected in the last 24 hours.
    Optionally filtered by severity and/or source. Limit max 500.
    data_json_summary is the first 200 characters of data_json cast to text.
    """
    # TODO Phase 2: add operator token auth here
    conditions = ["detected_at >= NOW() - INTERVAL '24 hours'"]
    args: list = []
    idx = 1

    if severity:
        conditions.append(f"severity = ${idx}")
        args.append(severity)
        idx += 1

    if source:
        conditions.append(f"source = ${idx}")
        args.append(source)
        idx += 1

    args.append(limit)
    where_clause = " AND ".join(conditions)

    try:
        rows = await db.fetch(
            f"""
            SELECT
                id,
                cin,
                source,
                event_type,
                severity,
                detected_at,
                health_score_before,
                health_score_after,
                LEFT(data_json::text, 200) AS data_json_summary
            FROM events
            WHERE {where_clause}
            ORDER BY detected_at DESC
            LIMIT ${idx}
            """,
            *args,
        )
    except Exception as e:
        raise _db_error(e)

    return [
        EventItem(
            id=row["id"],
            cin=row["cin"],
            source=row["source"],
            event_type=row["event_type"],
            severity=row["severity"],
            detected_at=row["detected_at"],
            health_score_before=row["health_score_before"],
            health_score_after=row["health_score_after"],
            data_json_summary=row["data_json_summary"] or "",
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /op/health/{cin}
# ---------------------------------------------------------------------------

@router.get("/health/{cin}", response_model=CompanyHealthResponse)
async def get_health(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Full health snapshot for a single CIN.
    Returns 404 if the CIN is not in master_entities.

    Health score components are computed from the stored health_score.
    The component breakdown uses the fixed weights from PIPELINE.md Step 6:
      filing_freshness  25%
      director_stability 20%
      legal_risk         25%
      financial_health   20%
      capital_trajectory 10%

    The raw component scores are not stored individually — they are stored
    only in data_json on the event that triggered the last recompute.
    For this endpoint, pull the last health-score event for this CIN
    and extract components from its data_json if available.
    If no component breakdown is stored, return zeroed components with a note.
    The raw score per component is read from
    events.data_json->>'components' written by the health scorer.
    """
    # TODO Phase 2: add operator token auth here

    # --- Fetch master entity ---
    try:
        entity = await db.fetchrow(
            """
            SELECT cin, company_name, health_score, health_band, last_score_computed_at
            FROM master_entities
            WHERE cin = $1
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    if entity is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found.")

    # --- Fetch last scoring event to extract component breakdown ---
    try:
        scoring_event = await db.fetchrow(
            """
            SELECT data_json
            FROM events
            WHERE cin = $1
              AND event_type = 'HEALTH_SCORE_COMPUTED'
            ORDER BY detected_at DESC
            LIMIT 1
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    # Build component objects. Fall back to zeros if no scoring event stored.
    weights = {
        "filing_freshness": 0.25,
        "director_stability": 0.20,
        "legal_risk": 0.25,
        "financial_health": 0.20,
        "capital_trajectory": 0.10,
    }

    raw_components: dict[str, float] = {}
    if scoring_event and scoring_event["data_json"]:
        dj = scoring_event["data_json"]
        stored = dj.get("components", {}) if isinstance(dj, dict) else {}
        for key in weights:
            raw_components[key] = float(stored.get(key, 0))
    else:
        for key in weights:
            raw_components[key] = 0.0

    components = HealthComponents(
        filing_freshness=HealthComponent(
            raw=raw_components["filing_freshness"],
            weighted=round(raw_components["filing_freshness"] * weights["filing_freshness"], 4),
        ),
        director_stability=HealthComponent(
            raw=raw_components["director_stability"],
            weighted=round(raw_components["director_stability"] * weights["director_stability"], 4),
        ),
        legal_risk=HealthComponent(
            raw=raw_components["legal_risk"],
            weighted=round(raw_components["legal_risk"] * weights["legal_risk"], 4),
        ),
        financial_health=HealthComponent(
            raw=raw_components["financial_health"],
            weighted=round(raw_components["financial_health"] * weights["financial_health"], 4),
        ),
        capital_trajectory=HealthComponent(
            raw=raw_components["capital_trajectory"],
            weighted=round(raw_components["capital_trajectory"] * weights["capital_trajectory"], 4),
        ),
    )

    # --- Fetch last 10 events for this CIN ---
    try:
        event_rows = await db.fetch(
            """
            SELECT
                id,
                cin,
                source,
                event_type,
                severity,
                detected_at,
                health_score_before,
                health_score_after,
                LEFT(data_json::text, 200) AS data_json_summary
            FROM events
            WHERE cin = $1
            ORDER BY detected_at DESC
            LIMIT 10
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    recent_events = [
        EventItem(
            id=r["id"],
            cin=r["cin"],
            source=r["source"],
            event_type=r["event_type"],
            severity=r["severity"],
            detected_at=r["detected_at"],
            health_score_before=r["health_score_before"],
            health_score_after=r["health_score_after"],
            data_json_summary=r["data_json_summary"] or "",
        )
        for r in event_rows
    ]

    # --- Fetch active legal cases ---
    try:
        legal_rows = await db.fetch(
            """
            SELECT id, case_type, case_number, court, filing_date, status, amount_involved
            FROM legal_events
            WHERE cin = $1
            ORDER BY filing_date DESC NULLS LAST
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    active_legal_cases = [
        LegalCaseItem(
            id=r["id"],
            case_type=r["case_type"],
            case_number=r["case_number"],
            court=r["court"],
            filing_date=r["filing_date"],
            status=r["status"],
            amount_involved=r["amount_involved"],
        )
        for r in legal_rows
    ]

    # --- Fetch directors from governance_graph ---
    try:
        director_rows = await db.fetch(
            """
            SELECT din, director_name, designation, date_of_appointment, is_active
            FROM governance_graph
            WHERE cin = $1
            ORDER BY is_active DESC, date_of_appointment DESC NULLS LAST
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    directors = [
        DirectorItem(
            din=r["din"],
            director_name=r["director_name"],
            designation=r["designation"],
            date_of_appointment=r["date_of_appointment"],
            is_active=r["is_active"],
        )
        for r in director_rows
    ]

    return CompanyHealthResponse(
        cin=entity["cin"],
        company_name=entity["company_name"],
        health_score=entity["health_score"],
        health_band=entity["health_band"],
        last_computed_at=entity["last_score_computed_at"],
        components=components,
        recent_events=recent_events,
        active_legal_cases=active_legal_cases,
        directors=directors,
    )


# ---------------------------------------------------------------------------
# GET /op/sources/lag
# ---------------------------------------------------------------------------

@router.get("/sources/lag", response_model=list[SourceLagItem])
async def get_sources_lag(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Returns only sources that are currently behind their cadence.
    A source is behind if next_pull_at < NOW().
    Returns an empty list if all sources are on time.
    """
    # TODO Phase 2: add operator token auth here
    try:
        rows = await db.fetch(
            """
            SELECT
                source_id,
                next_pull_at,
                status,
                EXTRACT(EPOCH FROM (NOW() - next_pull_at)) / 3600 AS lag_hours_raw
            FROM source_state
            WHERE next_pull_at IS NOT NULL
              AND next_pull_at < NOW()
            ORDER BY lag_hours_raw DESC
            """
        )
    except Exception as e:
        raise _db_error(e)

    return [
        SourceLagItem(
            source_id=row["source_id"],
            expected_next_pull=row["next_pull_at"],
            actual_lag_hours=round(float(row["lag_hours_raw"]), 2),
            status=row["status"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# GET /op/accuracy
# ---------------------------------------------------------------------------

@router.get("/accuracy", response_model=AccuracyResponse)
async def get_accuracy(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Rolling prediction accuracy for the last 30 days.
    Queries the predictions table.
    accuracy_pct = confirmed / (confirmed + false_positives) * 100.
    Returns 0.0 if no confirmed or false_positive rows exist.
    """
    # TODO Phase 2: add operator token auth here
    window_days = 30

    try:
        summary = await db.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE severity = 'RED' OR health_score_at_firing <= 33)
                    AS total_red_alerts,
                COUNT(*) FILTER (WHERE confirmed = TRUE)
                    AS confirmed,
                COUNT(*) FILTER (WHERE false_positive = TRUE)
                    AS false_positives,
                COUNT(*) FILTER (WHERE expired = TRUE AND confirmed IS NULL)
                    AS expired_unconfirmed
            FROM predictions
            WHERE fired_at >= NOW() - ($1 || ' days')::INTERVAL
            """,
            window_days,
        )
    except Exception as e:
        raise _db_error(e)

    # Compute accuracy_pct
    confirmed = summary["confirmed"] or 0
    false_positives = summary["false_positives"] or 0
    denominator = confirmed + false_positives
    accuracy_pct = round((confirmed / denominator) * 100, 1) if denominator > 0 else 0.0

    # Top false positive causes: join predictions -> events via event_combination
    # event_combination is TEXT[] — unnest to find most common event_type among FPs
    try:
        fp_rows = await db.fetch(
            """
            SELECT
                unnested_event AS event_type,
                COUNT(*) AS cnt
            FROM (
                SELECT UNNEST(event_combination) AS unnested_event
                FROM predictions
                WHERE false_positive = TRUE
                  AND fired_at >= NOW() - ($1 || ' days')::INTERVAL
            ) sub
            GROUP BY unnested_event
            ORDER BY cnt DESC
            LIMIT 5
            """,
            window_days,
        )
    except Exception as e:
        raise _db_error(e)

    top_fp_causes = [
        FalsePositiveCause(event_type=r["event_type"], count=r["cnt"])
        for r in fp_rows
    ]

    return AccuracyResponse(
        window_days=window_days,
        total_red_alerts=summary["total_red_alerts"] or 0,
        confirmed=confirmed,
        false_positives=false_positives,
        expired_unconfirmed=summary["expired_unconfirmed"] or 0,
        accuracy_pct=accuracy_pct,
        top_false_positive_causes=top_fp_causes,
    )


# ---------------------------------------------------------------------------
# GET /op/costs/today
# ---------------------------------------------------------------------------

@router.get("/costs/today", response_model=CostsTodayResponse)
async def get_costs_today(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Token and API cost summary for today (based on log_date = CURRENT_DATE).
    Reads cost_log table. Returns breakdown by service + operation.
    threshold_breached = true if total_inr > ALERT_THRESHOLD_INR env var (default 500).
    """
    # TODO Phase 2: add operator token auth here
    threshold = float(os.environ.get("ALERT_THRESHOLD_INR", "500"))

    try:
        rows = await db.fetch(
            """
            SELECT
                service,
                operation,
                SUM(units)    AS total_units,
                SUM(cost_inr) AS total_cost_inr
            FROM cost_log
            WHERE log_date = CURRENT_DATE
            GROUP BY service, operation
            ORDER BY total_cost_inr DESC
            """
        )
    except Exception as e:
        raise _db_error(e)

    breakdown = [
        {
            "service": r["service"],
            "operation": r["operation"],
            "units": int(r["total_units"]) if r["total_units"] is not None else None,
            "cost_inr": float(r["total_cost_inr"] or 0),
        }
        for r in rows
    ]

    total_inr = round(sum(item["cost_inr"] for item in breakdown), 2)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return CostsTodayResponse(
        date=today_str,
        total_inr=total_inr,
        breakdown=breakdown,
        alert_threshold_inr=threshold,
        threshold_breached=total_inr > threshold,
    )


# ---------------------------------------------------------------------------
# POST /op/watchlist
# ---------------------------------------------------------------------------

@router.post("/watchlist", response_model=WatchlistCreateResponse, status_code=201)
async def create_watchlist(
    body: WatchlistCreateRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Create a new watchlist directly from the CLI without using the UI.
    All filters except name are optional.
    Returns the created watchlist with its assigned id.
    """
    # TODO Phase 2: add operator token auth here
    severity_min = body.severity_min or "WATCH"

    try:
        row = await db.fetchrow(
            """
            INSERT INTO watchlists (
                name,
                cin_list,
                state_filter,
                sector_filter,
                severity_min,
                signal_types,
                is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, TRUE)
            RETURNING id, name, cin_list, state_filter, sector_filter,
                      severity_min, signal_types, is_active, created_at
            """,
            body.name,
            body.cin_list,
            body.state_filter,
            body.sector_filter,
            severity_min,
            body.signal_types,
        )
    except Exception as e:
        raise _db_error(e)

    return WatchlistCreateResponse(
        id=row["id"],
        name=row["name"],
        cin_list=row["cin_list"],
        state_filter=row["state_filter"],
        sector_filter=row["sector_filter"],
        severity_min=row["severity_min"],
        signal_types=row["signal_types"],
        is_active=row["is_active"],
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------------
# POST /op/enrich/{cin}
# ---------------------------------------------------------------------------

@router.post("/enrich/{cin}", response_model=EnrichResponse)
async def enrich_cin(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Manually queue a CompData enrichment for a CIN.
    Returns 404 if the CIN is not in master_entities.
    The actual enrichment is async. This endpoint only queues the task.
    The enrichment worker reads from a queue table or task broker —
    the mechanism for that is defined in the enrichment module, not here.
    For Phase 1: insert a row into enrichment_queue (TEXT table, not in schema yet —
    Codex must create this table as part of this spec):

      CREATE TABLE IF NOT EXISTS enrichment_queue (
        id         SERIAL      PRIMARY KEY,
        cin        VARCHAR(21) NOT NULL,
        queued_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
        processed  BOOLEAN     NOT NULL DEFAULT FALSE
      );

    The enrichment worker polls enrichment_queue WHERE processed = FALSE.
    """
    # TODO Phase 2: add operator token auth here

    # Verify CIN exists
    try:
        entity = await db.fetchrow(
            "SELECT cin FROM master_entities WHERE cin = $1",
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    if entity is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found in master_entities.")

    # Ensure enrichment_queue table exists (idempotent, safe to run each call)
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS enrichment_queue (
                id         SERIAL      PRIMARY KEY,
                cin        VARCHAR(21) NOT NULL,
                queued_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
                processed  BOOLEAN     NOT NULL DEFAULT FALSE
            )
            """
        )
        await db.execute(
            "INSERT INTO enrichment_queue (cin) VALUES ($1)",
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    return EnrichResponse(
        cin=cin,
        status="queued",
        message=f"Enrichment queued. Check /op/health/{cin} in 30 seconds.",
    )


# ---------------------------------------------------------------------------
# POST /op/resolve
# ---------------------------------------------------------------------------

@router.post("/resolve", response_model=ResolveResponse)
async def resolve_queue_item(
    body: ResolveRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Manually resolve an entity_resolution_queue row.
    Validates resolved_cin exists in master_entities — 422 if not.
    Updates: resolved=true, resolved_cin=..., operator_reviewed=true, resolved_at=NOW().
    Returns 404 if queue_id is not found.
    """
    # TODO Phase 2: add operator token auth here

    # 1. Verify queue_id exists
    try:
        queue_row = await db.fetchrow(
            "SELECT id FROM entity_resolution_queue WHERE id = $1",
            body.queue_id,
        )
    except Exception as e:
        raise _db_error(e)

    if queue_row is None:
        raise HTTPException(status_code=404, detail=f"Queue item {body.queue_id} not found.")

    # 2. Verify resolved_cin exists in master_entities — 422 if not
    try:
        entity = await db.fetchrow(
            "SELECT cin FROM master_entities WHERE cin = $1",
            body.resolved_cin,
        )
    except Exception as e:
        raise _db_error(e)

    if entity is None:
        raise HTTPException(
            status_code=422,
            detail=f"CIN {body.resolved_cin} does not exist in master_entities. Cannot resolve.",
        )

    # 3. Update the queue row
    try:
        await db.execute(
            """
            UPDATE entity_resolution_queue
            SET
                resolved          = TRUE,
                resolved_cin      = $1,
                operator_reviewed = TRUE,
                resolved_at       = NOW()
            WHERE id = $2
            """,
            body.resolved_cin,
            body.queue_id,
        )
    except Exception as e:
        raise _db_error(e)

    return ResolveResponse(
        queue_id=body.queue_id,
        resolved_cin=body.resolved_cin,
        message="Queue item resolved and marked as operator_reviewed.",
    )


# ---------------------------------------------------------------------------
# POST /op/recalibrate
# ---------------------------------------------------------------------------

@router.post("/recalibrate", response_model=RecalibrateResponse)
async def trigger_recalibrate(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Queue a monthly health score weight recalibration job.
    This endpoint does not block — it queues and returns immediately.
    The actual recalibration logic is owned by the health scorer module.
    For Phase 1: insert a row into recalibration_queue (idempotent table creation):

      CREATE TABLE IF NOT EXISTS recalibration_queue (
        id          SERIAL    PRIMARY KEY,
        queued_at   TIMESTAMP NOT NULL DEFAULT NOW(),
        processed   BOOLEAN   NOT NULL DEFAULT FALSE,
        result_json JSONB
      );

    The health scorer worker polls recalibration_queue WHERE processed = FALSE.
    """
    # TODO Phase 2: add operator token auth here

    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS recalibration_queue (
                id          SERIAL    PRIMARY KEY,
                queued_at   TIMESTAMP NOT NULL DEFAULT NOW(),
                processed   BOOLEAN   NOT NULL DEFAULT FALSE,
                result_json JSONB
            )
            """
        )
        await db.execute("INSERT INTO recalibration_queue DEFAULT VALUES")
    except Exception as e:
        raise _db_error(e)

    return RecalibrateResponse(
        status="started",
        message="Recalibration job queued. Results in daily digest.",
    )


# ---------------------------------------------------------------------------
# POST /op/captcha/solve
# ---------------------------------------------------------------------------

@router.post("/captcha/solve", response_model=CaptchaSolveResponse)
async def submit_captcha_solution(
    body: CaptchaSolveRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """
    Accept a manual CAPTCHA solution from the operator.
    Stores solution in _captcha_solutions dict keyed by source_id.
    The scraper module imports _captcha_solutions and polls it.
    When a solution is present, the scraper reads it, deletes the key, and resumes.
    Logs the solve to captcha_log with method='MANUAL', success=true, cost_inr=0.
    """
    # TODO Phase 2: add operator token auth here

    # Store in in-memory dict — scrapers poll this
    _captcha_solutions[body.source_id] = body.solution

    # Log to captcha_log
    try:
        await db.execute(
            """
            INSERT INTO captcha_log (source, method, success, cost_inr)
            VALUES ($1, 'MANUAL', TRUE, 0)
            """,
            body.source_id,
        )
    except Exception as e:
        raise _db_error(e)

    return CaptchaSolveResponse(
        status="accepted",
        source_id=body.source_id,
    )
```

---

## Notes on scraper integration for CAPTCHA polling

Scraper modules that may hit a CAPTCHA must import `_captcha_solutions` from `api.routers.operator` and poll it. The pattern is:

```python
from api.routers.operator import _captcha_solutions

async def wait_for_manual_captcha(source_id: str, timeout_seconds: int = 300) -> str | None:
    """
    Poll _captcha_solutions for up to timeout_seconds.
    Returns the solution string when the operator submits it, or None on timeout.
    Removes the key from the dict after reading (consume-once semantics).
    """
    import asyncio
    waited = 0
    while waited < timeout_seconds:
        if source_id in _captcha_solutions:
            solution = _captcha_solutions.pop(source_id)
            return solution
        await asyncio.sleep(5)
        waited += 5
    return None
```

This is a Phase 1 implementation. Phase 2 should replace the in-memory dict with a Redis key with TTL so the solution store survives process restarts and works across multiple uvicorn workers.

---

## Full implementation: `tests/test_operator_api.py`

```python
"""
Operator API tests.

Uses httpx.AsyncClient with FastAPI's TestClient via ASGITransport.
All DB calls are mocked — no real PostgreSQL required.
asyncpg.Connection is patched at the dependency level.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.main import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_async_mock_connection(fetch_return=None, fetchrow_return=None, execute_return=None):
    """
    Build a mock asyncpg connection with .fetch(), .fetchrow(), .execute() mocked.
    fetch_return   : list of dict-like rows (will be wrapped in AsyncMock)
    fetchrow_return: single dict-like row or None
    execute_return : ignored (execute returns None)
    """
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_return or [])
    conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    conn.execute = AsyncMock(return_value=None)
    return conn


class FakeRow(dict):
    """Dict subclass that also supports attribute access, mimicking asyncpg Record."""
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)


def row(**kwargs) -> FakeRow:
    return FakeRow(kwargs)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client():
    """
    Async httpx client wrapping the FastAPI app.
    Bypasses lifespan (no real DB pool needed — pool is mocked per test).
    """
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


def patch_db(mock_conn):
    """
    Patch the get_db dependency to yield mock_conn.
    Returns the context manager for use with `with patch_db(...):`.
    """
    async def _override():
        yield mock_conn

    return patch.object(app, "dependency_overrides", {})  # placeholder — see usage below


def override_db(mock_conn):
    """
    Direct override of the get_db FastAPI dependency.
    Must be cleaned up after each test.
    """
    from api.dependencies import get_db

    async def _get_mock_db():
        yield mock_conn

    app.dependency_overrides[get_db] = _get_mock_db


def clear_overrides():
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 1: GET /op/status — returns all sources, lag_hours computed correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_status_lag_hours_computed(client):
    """
    Source A: next_pull_at = 2 hours ago → lag_hours = ~2.0
    Source B: next_pull_at = 1 hour in future → lag_hours = null
    """
    now = datetime.now(timezone.utc)
    past = (now - timedelta(hours=2)).replace(tzinfo=None)   # naive, as asyncpg returns
    future = (now + timedelta(hours=1)).replace(tzinfo=None)

    mock_rows = [
        row(
            source_id="mca_ogd",
            status="OK",
            last_pull_at=past,
            record_count=1823456,
            consecutive_failures=0,
            next_pull_at=past,   # 2 hours overdue
        ),
        row(
            source_id="nclt",
            status="OK",
            last_pull_at=past,
            record_count=50000,
            consecutive_failures=0,
            next_pull_at=future,  # still upcoming
        ),
    ]

    conn = make_async_mock_connection(fetch_return=mock_rows)
    override_db(conn)

    try:
        resp = await client.get("/op/status")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

        mca = next(d for d in data if d["source_id"] == "mca_ogd")
        nclt = next(d for d in data if d["source_id"] == "nclt")

        # mca_ogd should have positive lag_hours
        assert mca["lag_hours"] is not None
        assert mca["lag_hours"] > 1.9

        # nclt next_pull is in the future — lag_hours must be null
        assert nclt["lag_hours"] is None
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 2: GET /op/events/today — severity filter works
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_events_today_severity_filter(client):
    """
    When severity=CRITICAL is passed, the query should include CRITICAL rows only.
    We verify the query parameter reaches the DB (mock checks call args).
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_rows = [
        row(
            id=1,
            cin="U27100GJ2015PTC082456",
            source="nclt",
            event_type="NCLT_SEC7_FILED",
            severity="CRITICAL",
            detected_at=now,
            health_score_before=45,
            health_score_after=20,
            data_json_summary='{"case_number": "CP/12/2026"}',
        )
    ]

    conn = make_async_mock_connection(fetch_return=mock_rows)
    override_db(conn)

    try:
        resp = await client.get("/op/events/today?severity=CRITICAL&limit=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["severity"] == "CRITICAL"
        assert data[0]["event_type"] == "NCLT_SEC7_FILED"

        # Verify the DB was called with 'CRITICAL' as an argument
        call_args = conn.fetch.call_args
        assert "CRITICAL" in call_args.args or "CRITICAL" in str(call_args)
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 3: GET /op/health/{cin} — returns full object
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_health_full_object(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cin = "U27100GJ2015PTC082456"

    entity_row = row(
        cin=cin,
        company_name="Acme Steel Pvt Ltd",
        health_score=28,
        health_band="RED",
        last_score_computed_at=now,
    )

    scoring_event_row = row(
        data_json={
            "components": {
                "filing_freshness": 40,
                "director_stability": 80,
                "legal_risk": 5,
                "financial_health": 50,
                "capital_trajectory": 60,
            }
        }
    )

    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "master_entities" in query and "last_score" not in query:
            return entity_row
        if "HEALTH_SCORE_COMPUTED" in query:
            return scoring_event_row
        # For resolve/enrich — not used here
        return None

    conn.fetchrow = mock_fetchrow
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)

    override_db(conn)

    try:
        resp = await client.get(f"/op/health/{cin}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cin"] == cin
        assert data["health_score"] == 28
        assert data["health_band"] == "RED"
        assert data["components"]["filing_freshness"]["raw"] == 40
        assert data["components"]["filing_freshness"]["weighted"] == pytest.approx(10.0, abs=0.01)
        assert data["components"]["legal_risk"]["raw"] == 5
        assert data["components"]["legal_risk"]["weighted"] == pytest.approx(1.25, abs=0.01)
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 4: GET /op/health/{cin} — 404 for unknown CIN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_health_404_unknown_cin(client):
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)

    override_db(conn)

    try:
        resp = await client.get("/op/health/XXXXXXXXXXXXXXXXXX001")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 5: GET /op/accuracy — accuracy_pct computed correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_accuracy_pct_computed(client):
    """
    confirmed=32, false_positives=8 → accuracy_pct = 32/(32+8)*100 = 80.0
    """
    summary_row = row(
        total_red_alerts=45,
        confirmed=32,
        false_positives=8,
        expired_unconfirmed=5,
    )

    fp_rows = [
        row(event_type="SEC138_FILED", cnt=4),
        row(event_type="STATUS_CHANGE", cnt=2),
    ]

    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=summary_row)
    conn.fetch = AsyncMock(return_value=fp_rows)
    conn.execute = AsyncMock(return_value=None)

    override_db(conn)

    try:
        resp = await client.get("/op/accuracy")
        assert resp.status_code == 200
        data = resp.json()
        assert data["confirmed"] == 32
        assert data["false_positives"] == 8
        assert data["accuracy_pct"] == pytest.approx(80.0, abs=0.1)
        assert data["total_red_alerts"] == 45
        assert len(data["top_false_positive_causes"]) == 2
        assert data["top_false_positive_causes"][0]["event_type"] == "SEC138_FILED"
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 6: GET /op/costs/today — threshold_breached=true when total > 500
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_costs_today_threshold_breached(client):
    """
    total_inr = 8.40 + 1.50 + 600.00 = 609.90 → threshold_breached = True
    """
    cost_rows = [
        row(service="claude_api", operation="entity_resolution", total_units=12, total_cost_inr=8.40),
        row(service="2captcha", operation="captcha_solve", total_units=3, total_cost_inr=1.50),
        row(service="compdata", operation="enrichment", total_units=20, total_cost_inr=600.00),
    ]

    conn = make_async_mock_connection(fetch_return=cost_rows)
    override_db(conn)

    import os
    with patch.dict(os.environ, {"ALERT_THRESHOLD_INR": "500"}):
        try:
            resp = await client.get("/op/costs/today")
            assert resp.status_code == 200
            data = resp.json()
            assert data["threshold_breached"] is True
            assert data["total_inr"] == pytest.approx(609.90, abs=0.01)
            assert data["alert_threshold_inr"] == 500.0
            assert len(data["breakdown"]) == 3
        finally:
            clear_overrides()


# ---------------------------------------------------------------------------
# Test 7: POST /op/watchlist — creates with all optional filters
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_watchlist_with_all_filters(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    created_row = row(
        id=42,
        name="My Portfolio",
        cin_list=["U27100GJ2015PTC082456"],
        state_filter="GJ",
        sector_filter="27100",
        severity_min="ALERT",
        signal_types=["NCLT_SEC7_FILED", "SARFAESI_DEMAND_NOTICE"],
        is_active=True,
        created_at=now,
    )

    conn = make_async_mock_connection(fetchrow_return=created_row)
    override_db(conn)

    payload = {
        "name": "My Portfolio",
        "cin_list": ["U27100GJ2015PTC082456"],
        "state_filter": "GJ",
        "sector_filter": "27100",
        "severity_min": "ALERT",
        "signal_types": ["NCLT_SEC7_FILED", "SARFAESI_DEMAND_NOTICE"],
    }

    try:
        resp = await client.post("/op/watchlist", json=payload)
        assert resp.status_code == 201
        data = resp.json()
        assert data["id"] == 42
        assert data["name"] == "My Portfolio"
        assert data["state_filter"] == "GJ"
        assert data["severity_min"] == "ALERT"
        assert "NCLT_SEC7_FILED" in data["signal_types"]
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 8: POST /op/resolve — updates queue row, 422 on bad CIN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_queue_item_422_on_bad_cin(client):
    """
    queue_id=123 exists. resolved_cin does not exist in master_entities.
    Expect 422.
    """
    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "entity_resolution_queue" in query:
            return row(id=123)          # queue item found
        if "master_entities" in query:
            return None                  # CIN not found
        return None

    conn.fetchrow = mock_fetchrow
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)

    override_db(conn)

    payload = {"queue_id": 123, "resolved_cin": "BADCIN00000000000000X"}

    try:
        resp = await client.post("/op/resolve", json=payload)
        assert resp.status_code == 422
        assert "does not exist" in resp.json()["detail"]
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_resolve_queue_item_success(client):
    """
    queue_id=123 exists. resolved_cin exists. Expect 200 and confirmation message.
    """
    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "entity_resolution_queue" in query:
            return row(id=123)
        if "master_entities" in query:
            return row(cin="U27100GJ2015PTC082456")
        return None

    conn.fetchrow = mock_fetchrow
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)

    override_db(conn)

    payload = {"queue_id": 123, "resolved_cin": "U27100GJ2015PTC082456"}

    try:
        resp = await client.post("/op/resolve", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["queue_id"] == 123
        assert data["resolved_cin"] == "U27100GJ2015PTC082456"
        # Verify UPDATE was called
        assert conn.execute.called
    finally:
        clear_overrides()


# ---------------------------------------------------------------------------
# Test 9: POST /op/captcha/solve — stores solution, logs to captcha_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_captcha_solve_stores_solution_and_logs(client):
    """
    Submitting a CAPTCHA solution must:
    1. Store it in _captcha_solutions dict under source_id key.
    2. Insert a row in captcha_log via db.execute.
    3. Return {"status": "accepted", "source_id": "nclt"}.
    """
    from api.routers.operator import _captcha_solutions

    conn = make_async_mock_connection()
    override_db(conn)

    # Clear any existing entry for this source_id before test
    _captcha_solutions.pop("nclt", None)

    payload = {"source_id": "nclt", "solution": "XY7Z2"}

    try:
        resp = await client.post("/op/captcha/solve", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["source_id"] == "nclt"

        # Solution must be in the in-memory store
        assert _captcha_solutions.get("nclt") == "XY7Z2"

        # captcha_log INSERT must have been called
        assert conn.execute.called
        call_args_str = str(conn.execute.call_args)
        assert "captcha_log" in call_args_str
        assert "MANUAL" in call_args_str
    finally:
        _captcha_solutions.pop("nclt", None)
        clear_overrides()
```

---

## Additional tables required by this spec

Codex must add the following two tables to the DB schema (idempotent via `CREATE TABLE IF NOT EXISTS`). These are created inline by the endpoints themselves on first call — no separate migration needed for Phase 1. In Phase 2, migrate these into the main schema file.

### enrichment_queue

```sql
CREATE TABLE IF NOT EXISTS enrichment_queue (
    id         SERIAL      PRIMARY KEY,
    cin        VARCHAR(21) NOT NULL,
    queued_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    processed  BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_enrichment_queue_unprocessed
    ON enrichment_queue (processed)
    WHERE processed = FALSE;
```

### recalibration_queue

```sql
CREATE TABLE IF NOT EXISTS recalibration_queue (
    id          SERIAL    PRIMARY KEY,
    queued_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    processed   BOOLEAN   NOT NULL DEFAULT FALSE,
    result_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_recalibration_queue_unprocessed
    ON recalibration_queue (processed)
    WHERE processed = FALSE;
```

---

## Constraints Codex must not violate

1. All endpoints live in `api/routers/operator.py`. No endpoint logic in `main.py`.
2. All Pydantic models live in `api/models.py`. No inline models in the router file.
3. DB queries use positional `$N` parameters. No f-string interpolation of user input into SQL.
4. The `_captcha_solutions` dict is module-level in `api/routers/operator.py`. Scrapers import it from there. It is never moved to a separate module in Phase 1.
5. `get_db` in `api/dependencies.py` must use `request.app.state.pool` — the pool is set by the lifespan in `main.py`, not imported directly.
6. Every 500 response must log the full traceback via the `logger` before returning the sanitized message.
7. Health score components are always returned — fall back to `raw=0.0, weighted=0.0` if no `HEALTH_SCORE_COMPUTED` event exists for the CIN. Never return a 500 because component data is missing.
8. `lag_hours` in `/op/status` is `None` when next_pull_at is null or in the future. It is a positive float only when the source is overdue.
9. `threshold_breached` in `/op/costs/today` reads `ALERT_THRESHOLD_INR` from env. Default 500 if env var is not set. The threshold comparison is strictly greater than (`>`), not `>=`.
10. Tests must not require a real PostgreSQL connection. All DB interactions are mocked at the dependency level.

---

## What Codex does NOT implement in this spec

- The actual enrichment logic (CompData API calls, scraper invocation). The enrichment worker polls `enrichment_queue`. Its implementation is out of scope here.
- The recalibration algorithm. The health scorer module owns that. This spec only queues the job.
- Auth. All `# TODO Phase 2: add operator token auth here` comments are placeholders only.
- The daily digest email or Telegram bot. Those are separate modules.
- The WebSocket dashboard. That is Gemini's domain.
