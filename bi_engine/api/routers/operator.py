"""
Operator CLI endpoints.

All routes are under /op/ prefix. No authentication in Phase 1.
# TODO Phase 2: add operator token auth here

In-memory CAPTCHA solution store:
  _captcha_solutions: dict[source_id, solution_string]
"""

from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Annotated, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse

from api.dependencies import get_db
from features import all_flags
from api.models import (
    AccuracyResponse,
    CaptchaSolveRequest,
    CaptchaSolveResponse,
    CompanyEventResponseItem,
    CompanyHealthResponse,
    CompanyHealthSummaryResponse,
    CostsTodayResponse,
    DirectorItem,
    EnrichResponse,
    EventItem,
    FalsePositiveCause,
    HealthComponent,
    HealthComponents,
    LegalCaseItem,
    AlertFeedbackRequest,
    AlertFeedbackResponse,
    RecalibrateResult,
    ResolveRequest,
    ResolveResponse,
    ScraperHealthItem,
    ScraperHealthSummary,
    SubscriberCreateRequest,
    SubscriberCreateResponse,
    SubscriberItem,
    SubscriberWatchlistCreateRequest,
    SubscriberWatchlistItem,
    SourceLagItem,
    SourceStatusItem,
    WatchlistCreateRequest,
    WatchlistCreateResponse,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/op", tags=["operator"])
_captcha_solutions: dict[str, str] = {}


def _db_error(e: Exception) -> HTTPException:
    logger.error("DB error:\n%s", traceback.format_exc())
    return HTTPException(status_code=500, detail="Internal server error.")


def _lag_hours(next_pull_at: Optional[datetime]) -> Optional[float]:
    if next_pull_at is None:
        return None
    now = datetime.now(timezone.utc)
    if next_pull_at.tzinfo is None:
        next_pull_at = next_pull_at.replace(tzinfo=timezone.utc)
    delta = (now - next_pull_at).total_seconds() / 3600
    return round(delta, 2) if delta > 0 else None


async def _ensure_subscriber_exists(db: asyncpg.Connection, subscriber_id: int) -> None:
    row = await db.fetchrow("SELECT id FROM subscribers WHERE id = $1", subscriber_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Subscriber {subscriber_id} not found.")


@router.get("/status", response_model=list[SourceStatusItem])
async def get_status(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    # TODO Phase 2: insert auth check here
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


@router.get("/events/today", response_model=list[EventItem])
async def get_events_today(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    severity: Optional[str] = Query(default=None, description="Filter by severity: INFO, WATCH, ALERT, CRITICAL"),
    source: Optional[str] = Query(default=None, description="Filter by source_id"),
    limit: int = Query(default=100, ge=1, le=500),
):
    # TODO Phase 2: insert auth check here
    conditions = ["e.detected_at >= NOW() - INTERVAL '24 hours'"]
    args: list = []
    idx = 1

    if severity:
        conditions.append(f"e.severity = ${idx}")
        args.append(severity)
        idx += 1

    if source:
        conditions.append(f"e.source = ${idx}")
        args.append(source)
        idx += 1

    args.append(limit)
    where_clause = " AND ".join(conditions)

    try:
        rows = await db.fetch(
            f"""
            SELECT
                e.id,
                e.cin,
                me.company_name,
                e.source,
                e.event_type,
                e.severity,
                e.detected_at,
                e.health_score_before,
                e.health_score_after,
                LEFT(e.data_json::text, 200) AS data_json_summary
            FROM events e
            LEFT JOIN master_entities me ON e.cin = me.cin
            WHERE {where_clause}
            ORDER BY e.detected_at DESC
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
            company_name=row["company_name"],
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


@router.get("/health/{cin}", response_model=CompanyHealthResponse)
async def get_health(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        entity = await db.fetchrow(
            """
            SELECT *
            FROM master_entities
            WHERE cin = $1
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    if entity is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found.")

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

    weights = {
        "filing_freshness": 0.25,
        "director_stability": 0.20,
        "legal_risk": 0.25,
        "financial_health": 0.20,
        "capital_trajectory": 0.10,
    }
    raw_components: dict[str, float] = {}
    if scoring_event and scoring_event["data_json"]:
        data_json = scoring_event["data_json"]
        stored = data_json.get("components", {}) if isinstance(data_json, dict) else {}
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
        for row in event_rows
    ]

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
            id=row["id"],
            case_type=row["case_type"],
            case_number=row["case_number"],
            court=row["court"],
            filing_date=row["filing_date"],
            status=row["status"],
            amount_involved=row["amount_involved"],
        )
        for row in legal_rows
    ]

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
            din=row["din"],
            director_name=row["director_name"],
            designation=row["designation"],
            date_of_appointment=row["date_of_appointment"],
            is_active=row["is_active"],
        )
        for row in director_rows
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


@router.get("/sources/lag", response_model=list[SourceLagItem])
async def get_sources_lag(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    # TODO Phase 2: insert auth check here
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


@router.get("/accuracy", response_model=AccuracyResponse)
async def get_accuracy(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    # TODO Phase 2: insert auth check here
    window_days = 30
    try:
        summary = await db.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE severity = 'CRITICAL' OR health_score_at_firing <= 33)
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
            str(window_days),
        )
    except Exception as e:
        raise _db_error(e)

    confirmed = summary["confirmed"] or 0
    false_positives = summary["false_positives"] or 0
    denominator = confirmed + false_positives
    accuracy_pct = round((confirmed / denominator) * 100, 1) if denominator > 0 else 0.0

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
            str(window_days),
        )
    except Exception as e:
        raise _db_error(e)

    return AccuracyResponse(
        window_days=window_days,
        total_red_alerts=summary["total_red_alerts"] or 0,
        confirmed=confirmed,
        false_positives=false_positives,
        expired_unconfirmed=summary["expired_unconfirmed"] or 0,
        accuracy_pct=accuracy_pct,
        top_false_positive_causes=[
            FalsePositiveCause(event_type=row["event_type"], count=row["cnt"])
            for row in fp_rows
        ],
    )


@router.get("/costs/today", response_model=CostsTodayResponse)
async def get_costs_today(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    # TODO Phase 2: insert auth check here
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
            "service": row["service"],
            "operation": row["operation"],
            "units": int(row["total_units"]) if row["total_units"] is not None else None,
            "cost_inr": float(row["total_cost_inr"] or 0),
        }
        for row in rows
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


@router.get("/subscribers", response_model=list[SubscriberItem])
async def get_subscribers(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    # TODO Phase 2: insert auth check here
    try:
        rows = await db.fetch(
            """
            SELECT
                s.id,
                s.name,
                s.email,
                s.severity_threshold,
                COALESCE((
                    SELECT COUNT(*)
                    FROM watchlists w
                    WHERE w.subscriber_id = s.id
                      AND w.cin IS NOT NULL
                ), 0) AS watchlist_count,
                COALESCE((
                    SELECT COUNT(*)
                    FROM delivered_alerts da
                    WHERE da.subscriber_id = s.id
                      AND date_trunc('month', da.delivered_at) = date_trunc('month', CURRENT_TIMESTAMP)
                ), 0) AS alert_count_this_month
            FROM subscribers s
            ORDER BY s.created_at DESC, s.id DESC
            """
        )
    except Exception as e:
        raise _db_error(e)

    return [
        SubscriberItem(
            id=row["id"],
            name=row["name"],
            email=row["email"],
            severity_threshold=row["severity_threshold"],
            watchlist_count=int(row["watchlist_count"] or 0),
            alert_count_this_month=int(row["alert_count_this_month"] or 0),
        )
        for row in rows
    ]


@router.post("/subscribers", response_model=SubscriberCreateResponse, status_code=201)
async def create_subscriber(
    body: SubscriberCreateRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        row = await db.fetchrow(
            """
            INSERT INTO subscribers (name, email, severity_threshold)
            VALUES ($1, $2, $3)
            RETURNING id, name, email, severity_threshold
            """,
            body.name,
            body.email,
            body.severity_threshold,
        )
    except Exception as e:
        raise _db_error(e)

    return SubscriberCreateResponse(
        id=row["id"],
        name=row["name"],
        email=row["email"],
        severity_threshold=row["severity_threshold"],
    )


@router.delete("/subscribers/{subscriber_id}", status_code=204)
async def delete_subscriber(
    subscriber_id: int,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        await db.execute("DELETE FROM delivered_alerts WHERE subscriber_id = $1", subscriber_id)
        await db.execute("DELETE FROM watchlists WHERE subscriber_id = $1", subscriber_id)
        await db.execute("DELETE FROM subscribers WHERE id = $1", subscriber_id)
    except Exception as e:
        raise _db_error(e)


@router.get("/watchlists/{subscriber_id}", response_model=list[SubscriberWatchlistItem])
async def get_watchlists(
    subscriber_id: int,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        await _ensure_subscriber_exists(db, subscriber_id)
        rows = await db.fetch(
            """
            SELECT
                w.cin,
                COALESCE(me.company_name, '') AS company_name,
                w.added_at
            FROM watchlists w
            LEFT JOIN master_entities me ON me.cin = w.cin
            WHERE w.subscriber_id = $1
              AND w.cin IS NOT NULL
            ORDER BY w.added_at DESC, w.cin
            """,
            subscriber_id,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise _db_error(e)

    return [
        SubscriberWatchlistItem(
            cin=row["cin"],
            company_name=row["company_name"],
            added_at=row["added_at"],
        )
        for row in rows
    ]


@router.post("/watchlists/{subscriber_id}", response_model=SubscriberWatchlistItem, status_code=201)
async def create_subscriber_watchlist(
    subscriber_id: int,
    body: SubscriberWatchlistCreateRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        await _ensure_subscriber_exists(db, subscriber_id)
        company = await db.fetchrow(
            """
            SELECT company_name
            FROM master_entities
            WHERE cin = $1
            """,
            body.cin,
        )
        if company is None:
            raise HTTPException(status_code=404, detail=f"CIN {body.cin} not found.")

        existing = await db.fetchrow(
            """
            SELECT cin, added_at
            FROM watchlists
            WHERE subscriber_id = $1 AND cin = $2
            """,
            subscriber_id,
            body.cin,
        )
        if existing is None:
            existing = await db.fetchrow(
                """
                INSERT INTO watchlists (subscriber_id, cin, name, added_at, is_active)
                VALUES ($1, $2, $3, NOW(), TRUE)
                RETURNING cin, added_at
                """,
                subscriber_id,
                body.cin,
                company["company_name"],
            )
    except HTTPException:
        raise
    except Exception as e:
        raise _db_error(e)

    return SubscriberWatchlistItem(
        cin=existing["cin"],
        company_name=company["company_name"],
        added_at=existing["added_at"],
    )


@router.delete("/watchlists/{subscriber_id}/{cin}", status_code=204)
async def delete_subscriber_watchlist(
    subscriber_id: int,
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        await db.execute(
            """
            DELETE FROM watchlists
            WHERE subscriber_id = $1 AND cin = $2
            """,
            subscriber_id,
            cin,
        )
    except Exception as e:
        raise _db_error(e)


@router.get("/alerts/feed")
async def get_alerts_feed(
    db: Annotated[asyncpg.Connection, Depends(get_db)],
    limit: int = Query(default=50, ge=1, le=200),
):
    """Return recent delivered alerts with company name and event context."""
    try:
        rows = await db.fetch(
            """
            SELECT
                da.id,
                da.cin,
                me.company_name,
                da.event_type AS signal_type,
                COALESCE(e.severity, 'INFO') AS severity,
                e.health_score_before,
                e.health_score_after,
                da.delivered_at,
                COALESCE(LEFT(e.data_json::text, 300), '') AS explanation
            FROM delivered_alerts da
            LEFT JOIN master_entities me ON da.cin = me.cin
            LEFT JOIN LATERAL (
                SELECT severity, health_score_before, health_score_after, data_json
                FROM events
                WHERE cin = da.cin AND event_type = da.event_type
                ORDER BY detected_at DESC
                LIMIT 1
            ) e ON TRUE
            ORDER BY da.delivered_at DESC
            LIMIT $1
            """,
            limit,
        )
    except Exception as e:
        raise _db_error(e)

    return [
        {
            "id": row["id"],
            "cin": row["cin"],
            "company_name": row["company_name"] or "Unknown Entity",
            "signal_type": row["signal_type"],
            "severity": row["severity"],
            "health_score_before": row["health_score_before"] or 0,
            "health_score_after": row["health_score_after"] or 0,
            "delivered_at": row["delivered_at"].isoformat() if row["delivered_at"] else None,
            "explanation": row["explanation"],
        }
        for row in rows
    ]


@router.get("/company/{cin}/events", response_model=list[CompanyEventResponseItem])
async def get_company_events(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        rows = await db.fetch(
            """
            SELECT
                detected_at AS event_date,
                event_type,
                severity,
                source,
                COALESCE(LEFT(data_json::text, 500), '') AS notes
            FROM events
            WHERE cin = $1
            ORDER BY detected_at DESC
            LIMIT 100
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    return [
        CompanyEventResponseItem(
            event_date=row["event_date"],
            event_type=row["event_type"],
            severity=row["severity"],
            source=row["source"],
            notes=row["notes"],
        )
        for row in rows
    ]


@router.get("/company/{cin}/health", response_model=CompanyHealthSummaryResponse)
async def get_company_health_summary(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        row = await db.fetchrow(
            """
            SELECT
                cin,
                health_score,
                health_band AS band,
                last_score_computed_at AS last_computed
            FROM master_entities
            WHERE cin = $1
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)

    if row is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found.")

    return CompanyHealthSummaryResponse(
        cin=row["cin"],
        health_score=row["health_score"],
        band=row["band"],
        last_computed=row["last_computed"],
    )


@router.post("/watchlist", response_model=WatchlistCreateResponse, status_code=201)
async def create_watchlist(
    body: WatchlistCreateRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
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


@router.post("/enrich/{cin}", response_model=EnrichResponse)
async def enrich_cin(
    cin: str,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        entity = await db.fetchrow("SELECT cin FROM master_entities WHERE cin = $1", cin)
    except Exception as e:
        raise _db_error(e)

    if entity is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found in master_entities.")

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
            """
            CREATE INDEX IF NOT EXISTS idx_enrichment_queue_unprocessed
                ON enrichment_queue (processed)
                WHERE processed = FALSE
            """
        )
        await db.execute("INSERT INTO enrichment_queue (cin) VALUES ($1)", cin)
    except Exception as e:
        raise _db_error(e)

    return EnrichResponse(
        cin=cin,
        status="queued",
        message=f"Enrichment queued. Check /op/health/{cin} in 30 seconds.",
    )


@router.post("/resolve", response_model=ResolveResponse)
async def resolve_queue_item(
    body: ResolveRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    try:
        queue_row = await db.fetchrow(
            "SELECT id FROM entity_resolution_queue WHERE id = $1",
            body.queue_id,
        )
    except Exception as e:
        raise _db_error(e)

    if queue_row is None:
        raise HTTPException(status_code=404, detail=f"Queue item {body.queue_id} not found.")

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


@router.post("/alerts/{alert_id}/feedback", response_model=AlertFeedbackResponse)
async def submit_alert_feedback(
    alert_id: int,
    body: AlertFeedbackRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    """Mark an alert as confirmed or false_positive. Feeds the recalibration loop."""
    # Find the prediction row linked to this delivered_alert
    try:
        row = await db.fetchrow(
            """
            SELECT p.id, p.cin, p.severity
            FROM delivered_alerts da
            JOIN predictions p ON (
                p.cin = da.cin
                AND p.fired_at >= da.delivered_at - INTERVAL '1 hour'
                AND p.fired_at <= da.delivered_at + INTERVAL '1 hour'
            )
            WHERE da.id = $1
            ORDER BY p.fired_at DESC
            LIMIT 1
            """,
            alert_id,
        )
    except Exception as e:
        raise _db_error(e)

    if not row:
        raise HTTPException(status_code=404, detail="No prediction found for this alert")

    pred_id = row["id"]
    try:
        if body.action == "confirm":
            await db.execute(
                """
                UPDATE predictions
                SET confirmed = TRUE, confirmed_at = NOW()
                WHERE id = $1
                """,
                pred_id,
            )
            msg = "Alert confirmed — signal counted as accurate."
        else:
            await db.execute(
                """
                UPDATE predictions
                SET false_positive = TRUE,
                    false_positive_reason = $2,
                    confirmed = FALSE
                WHERE id = $1
                """,
                pred_id,
                body.reason,
            )
            msg = "Marked false positive — will lower threshold on next recalibration."
    except Exception as e:
        raise _db_error(e)

    return AlertFeedbackResponse(prediction_id=pred_id, action=body.action, message=msg)


@router.post("/recalibrate", response_model=RecalibrateResult)
async def trigger_recalibrate(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    """
    Runs the recalibration loop immediately:
    1. Expire predictions older than 30 days with no feedback.
    2. Per (source, severity): if false_positive_rate > 20% → lower threshold.
       If false_positive_rate < 5% and sample >= 10 → raise threshold.
    3. Write adjusted thresholds to source_state.extra_json.
    """
    try:
        # Step 1: expire old unconfirmed predictions
        expired = await db.fetchval(
            """
            WITH expired AS (
                UPDATE predictions
                SET expired = TRUE
                WHERE confirmed IS NULL
                  AND false_positive IS NULL
                  AND fired_at < NOW() - INTERVAL '30 days'
                  AND expired = FALSE
                RETURNING id
            )
            SELECT COUNT(*) FROM expired
            """
        )

        # Step 2: compute false_positive rate per (source, severity) via events join
        rows = await db.fetch(
            """
            SELECT
                e.source,
                p.severity,
                COUNT(*)                                            AS total,
                COUNT(*) FILTER (WHERE p.false_positive = TRUE)    AS fp_count,
                COUNT(*) FILTER (WHERE p.confirmed = TRUE)         AS confirmed_count
            FROM predictions p
            JOIN events e ON (
                e.cin = p.cin
                AND e.severity = p.severity
                AND e.detected_at BETWEEN p.fired_at - INTERVAL '2 hours'
                                      AND p.fired_at + INTERVAL '2 hours'
            )
            WHERE p.fired_at > NOW() - INTERVAL '90 days'
              AND p.expired = FALSE
            GROUP BY e.source, p.severity
            HAVING COUNT(*) >= 5
            """
        )

        raised = 0
        lowered = 0
        for row in rows:
            source = row["source"]
            severity = row["severity"]
            total = row["total"]
            fp_rate = row["fp_count"] / total if total else 0
            confirm_rate = row["confirmed_count"] / total if total else 0

            if fp_rate > 0.20:
                # Too many false positives → raise the score threshold for this source
                action = "raise_threshold"
                lowered += 1
            elif confirm_rate > 0.95 and total >= 10:
                # Very accurate → lower threshold so we catch more signals
                action = "lower_threshold"
                raised += 1
            else:
                continue

            # Write decision into source_state.extra_json
            await db.execute(
                """
                UPDATE source_state
                SET extra_json = jsonb_set(
                    COALESCE(extra_json, '{}'::jsonb),
                    ARRAY['recalibration'],
                    extra_json->'recalibration' || jsonb_build_object(
                        $2::text,
                        jsonb_build_object(
                            'action', $3::text,
                            'fp_rate', $4::float,
                            'sample', $5::int,
                            'updated_at', NOW()::text
                        )
                    )
                )
                WHERE source_id = $1
                """,
                source,
                severity,
                action,
                float(fp_rate),
                int(total),
            )

    except Exception as e:
        raise _db_error(e)

    sources_adjusted = raised + lowered
    return RecalibrateResult(
        sources_adjusted=sources_adjusted,
        thresholds_raised=raised,
        thresholds_lowered=lowered,
        predictions_expired=int(expired or 0),
        summary=(
            f"Expired {expired or 0} stale predictions. "
            f"Adjusted {sources_adjusted} source/severity thresholds "
            f"({raised} lowered, {lowered} raised)."
        ),
    )


@router.get("/companies/search")
async def search_companies(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(default=10, ge=1, le=50),
    db: asyncpg.Connection = Depends(get_db),
):
    q = q.strip()
    if not q:
        return []
    try:
        # Starts-with matches first, then contains matches
        rows = await db.fetch(
            """
            (
                SELECT cin, company_name, registered_state, status, health_score, health_band,
                       1 AS rank
                FROM master_entities
                WHERE company_name ILIKE $1
                ORDER BY company_name
                LIMIT $2
            )
            UNION ALL
            (
                SELECT cin, company_name, registered_state, status, health_score, health_band,
                       2 AS rank
                FROM master_entities
                WHERE company_name ILIKE $3
                  AND company_name NOT ILIKE $1
                ORDER BY company_name
                LIMIT $2
            )
            ORDER BY rank, company_name
            LIMIT $2
            """,
            f"{q}%",
            limit,
            f"%{q}%",
        )
    except Exception as e:
        raise _db_error(e)
    return [
        {
            "cin": r["cin"],
            "company_name": r["company_name"],
            "registered_state": r["registered_state"],
            "status": r["status"],
            "health_score": r["health_score"],
            "health_band": r["health_band"],
        }
        for r in rows
    ]


@router.get("/companies/{cin}/profile")
async def get_company_profile(
    cin: str,
    db: asyncpg.Connection = Depends(get_db),
):
    try:
        row = await db.fetchrow(
            """
            SELECT cin, company_name, status, registered_state,
                   industrial_class, date_of_incorporation, date_of_last_agm,
                   authorized_capital, paid_up_capital,
                   company_category, company_subcategory, registered_address,
                   health_score, health_band
            FROM master_entities
            WHERE cin = $1
            """,
            cin,
        )
    except Exception as e:
        raise _db_error(e)
    if row is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found.")
    return dict(row)


@router.get("/companies/{cin}/intelligence")
async def get_company_intelligence(
    cin: str,
    db: asyncpg.Connection = Depends(get_db),
):
    """
    Generate full AI intelligence report for a company.
    Combines our verified DB signals with Gemini + Google Search grounding.
    """
    from api.gemini_intelligence import generate_company_intelligence

    # Get company info from DB
    company = await db.fetchrow(
        "SELECT cin, company_name, health_score, health_band FROM master_entities WHERE cin = $1",
        cin,
    )
    if company is None:
        raise HTTPException(status_code=404, detail=f"CIN {cin} not found.")

    # Get our verified events for this company
    events = await db.fetch(
        """
        SELECT event_type, severity, source, detected_at, data_json
        FROM events WHERE cin = $1
        ORDER BY detected_at DESC LIMIT 10
        """,
        cin,
    )
    db_events = []
    for e in events:
        db_events.append({
            "event_type": e["event_type"],
            "severity": e["severity"],
            "source": e["source"],
            "detected_at": str(e["detected_at"]),
            "data_json": e["data_json"] if isinstance(e["data_json"], dict) else {},
        })

    try:
        result = await generate_company_intelligence(
            company_name=company["company_name"],
            cin=cin,
            db_events=db_events if db_events else None,
            db_health_score=company["health_score"],
        )
        return result
    except Exception as exc:
        logger.error("Intelligence generation failed for %s: %s", cin, exc)
        raise HTTPException(status_code=502, detail=f"Gemini API error: {str(exc)}")


@router.get("/intelligence/search")
async def search_company_intelligence(
    q: str = Query(..., min_length=2, description="Company name to analyze"),
    db: asyncpg.Connection = Depends(get_db),
):
    """
    Search by company name and generate intelligence.
    If company is in our DB, enriches with verified signals.
    If not, generates pure Gemini intelligence from web search.
    """
    from api.gemini_intelligence import generate_company_intelligence

    # Try to find in our DB first
    row = await db.fetchrow(
        """
        SELECT cin, company_name, health_score
        FROM master_entities
        WHERE company_name ILIKE $1 OR normalized_name ILIKE $2
        LIMIT 1
        """,
        f"%{q}%",
        f"%{q.lower()}%",
    )

    cin = row["cin"] if row else None
    company_name = row["company_name"] if row else q
    health_score = row["health_score"] if row else None

    db_events = []
    if cin:
        events = await db.fetch(
            "SELECT event_type, severity, source, detected_at, data_json FROM events WHERE cin = $1 ORDER BY detected_at DESC LIMIT 10",
            cin,
        )
        for e in events:
            db_events.append({
                "event_type": e["event_type"],
                "severity": e["severity"],
                "source": e["source"],
                "detected_at": str(e["detected_at"]),
                "data_json": e["data_json"] if isinstance(e["data_json"], dict) else {},
            })

    try:
        result = await generate_company_intelligence(
            company_name=company_name,
            cin=cin,
            db_events=db_events if db_events else None,
            db_health_score=health_score,
        )
        result["_fromDatabase"] = cin is not None
        result["_dbCin"] = cin
        return result
    except Exception as exc:
        logger.error("Intelligence search failed for %s: %s", q, exc)
        raise HTTPException(status_code=502, detail=f"Gemini API error: {str(exc)}")


@router.post("/captcha/solve", response_model=CaptchaSolveResponse)
async def submit_captcha_solution(
    body: CaptchaSolveRequest,
    db: Annotated[asyncpg.Connection, Depends(get_db)],
):
    # TODO Phase 2: insert auth check here
    _captcha_solutions[body.source_id] = body.solution

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

    return CaptchaSolveResponse(status="accepted", source_id=body.source_id)


def _scraper_color(status: str, consecutive_failures: int, last_pull_at: Optional[datetime]) -> tuple[str, str]:
    """Return (color, note) for a scraper row."""
    s = (status or "").lower()
    if last_pull_at is None:
        return "grey", "never ran"
    if s in ("blocked", "unreachable"):
        return "red", s
    if consecutive_failures >= 3:
        return "red", f"{consecutive_failures} consecutive failures"
    if consecutive_failures >= 1:
        return "amber", f"{consecutive_failures} failure(s)"
    if s in ("ok", "degraded") and consecutive_failures == 0:
        return "green", "ok"
    if s == "degraded":
        return "amber", "degraded"
    return "amber", s or "unknown"


@router.get("/scraper-health", response_model=ScraperHealthSummary)
async def get_scraper_health(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    try:
        source_rows = await db.fetch(
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
        unmapped_count = await db.fetchval(
            "SELECT COUNT(*) FROM entity_resolution_queue WHERE resolved = FALSE"
        )
        events_today = await db.fetchval(
            "SELECT COUNT(*) FROM events WHERE detected_at >= NOW() - INTERVAL '24 hours'"
        )
        alerts_today = await db.fetchval(
            "SELECT COUNT(*) FROM delivered_alerts WHERE delivered_at >= NOW() - INTERVAL '24 hours'"
        )
    except Exception as e:
        raise _db_error(e)

    scrapers: list[ScraperHealthItem] = []
    counts = {"green": 0, "amber": 0, "red": 0, "grey": 0}

    for row in source_rows:
        color, note = _scraper_color(
            row["status"],
            row["consecutive_failures"] or 0,
            row["last_pull_at"],
        )
        counts[color] += 1
        scrapers.append(
            ScraperHealthItem(
                source_id=row["source_id"],
                status=row["status"] or "unknown",
                color=color,
                last_pull_at=row["last_pull_at"],
                record_count=row["record_count"],
                consecutive_failures=row["consecutive_failures"] or 0,
                lag_hours=_lag_hours(row["next_pull_at"]),
                note=note,
            )
        )

    return ScraperHealthSummary(
        as_of=datetime.now(timezone.utc),
        total_scrapers=len(scrapers),
        green=counts["green"],
        amber=counts["amber"],
        red=counts["red"],
        grey=counts["grey"],
        unmapped_signals=unmapped_count or 0,
        events_today=events_today or 0,
        alerts_delivered_today=alerts_today or 0,
        scrapers=scrapers,
    )


@router.get("/flags")
async def get_feature_flags():
    """Return all feature flags and their current state."""
    return {"flags": all_flags()}


@router.post("/run/{phase}")
async def run_pipeline_phase(phase: str):
    """
    Manually trigger a pipeline phase.

    Valid phases: ingest, detect, route, digest, recalibrate
    This endpoint signals the scheduler to run the phase immediately.
    The scheduler must be running for this to take effect.
    """
    valid_phases = {"ingest", "detect", "route", "digest", "recalibrate"}
    if phase not in valid_phases:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown phase '{phase}'. Valid: {sorted(valid_phases)}",
        )
    # Write a trigger file the scheduler polls for
    trigger_path = f"/tmp/icie_trigger_{phase}"
    try:
        with open(trigger_path, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        return {"triggered": phase, "trigger_file": trigger_path}
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(db: Annotated[asyncpg.Connection, Depends(get_db)]):
    summary: ScraperHealthSummary = await get_scraper_health(db)
    as_of = summary.as_of.strftime("%Y-%m-%d %H:%M UTC")

    # --- Intelligence Feed: recent events with company names ---
    try:
        event_rows = await db.fetch(
            """
            SELECT e.id, e.cin, COALESCE(me.company_name, e.cin, '—') AS company,
                   e.event_type, e.severity, e.source, e.detected_at,
                   me.health_score, me.health_band
            FROM events e
            LEFT JOIN master_entities me ON me.cin = e.cin
            ORDER BY e.detected_at DESC
            LIMIT 25
            """
        )
    except Exception:
        event_rows = []

    # --- Cost data ---
    try:
        cost_rows = await db.fetch(
            """
            SELECT log_date, service, SUM(cost_inr) AS cost
            FROM cost_log
            GROUP BY log_date, service
            ORDER BY log_date DESC
            LIMIT 30
            """
        )
        cost_total = await db.fetchval("SELECT COALESCE(SUM(cost_inr), 0) FROM cost_log")
        cost_today = await db.fetchval(
            "SELECT COALESCE(SUM(cost_inr), 0) FROM cost_log WHERE log_date = CURRENT_DATE"
        )
        cost_month = await db.fetchval(
            "SELECT COALESCE(SUM(cost_inr), 0) FROM cost_log WHERE log_date >= date_trunc('month', CURRENT_DATE)"
        )
    except Exception:
        cost_rows, cost_total, cost_today, cost_month = [], 0, 0, 0

    # --- Entity resolution stats ---
    try:
        unmapped_total = await db.fetchval("SELECT COUNT(*) FROM unmapped_signals") or 0
        unmapped_by_source = await db.fetch(
            "SELECT source, COUNT(*) AS cnt FROM unmapped_signals GROUP BY source ORDER BY cnt DESC LIMIT 10"
        )
        legal_total = await db.fetchval("SELECT COUNT(*) FROM legal_events") or 0
        legal_by_source = await db.fetch(
            "SELECT source, COUNT(*) AS cnt FROM legal_events GROUP BY source ORDER BY cnt DESC LIMIT 10"
        )
    except Exception:
        unmapped_total, unmapped_by_source = 0, []
        legal_total, legal_by_source = 0, []

    # --- Alert delivery stats ---
    try:
        alerts_all_time = await db.fetchval("SELECT COUNT(*) FROM alerts") or 0
        alerts_by_channel = await db.fetch(
            "SELECT delivery_channel, delivery_status, COUNT(*) AS cnt FROM alerts GROUP BY delivery_channel, delivery_status ORDER BY cnt DESC"
        )
    except Exception:
        alerts_all_time, alerts_by_channel = 0, []

    # --- Watchlist count ---
    try:
        watchlist_count = await db.fetchval("SELECT COUNT(*) FROM watchlists") or 0
    except Exception:
        watchlist_count = 0

    # Build scraper rows
    scraper_rows = ""
    for s in summary.scrapers:
        bg = {"green": "#0d2818", "amber": "#2a1f00", "red": "#2a0a0a", "grey": "#1a1a1a"}[s.color]
        badge_bg = {"green": "#22c55e", "amber": "#f59e0b", "red": "#ef4444", "grey": "#6b7280"}[s.color]
        last = s.last_pull_at.strftime("%b %d, %H:%M") if s.last_pull_at else "—"
        lag = f"{s.lag_hours:.1f}h" if s.lag_hours is not None else "—"
        scraper_rows += (
            f'<tr style="background:{bg}">'
            f'<td><span class="badge" style="background:{badge_bg}">{s.color.upper()}</span></td>'
            f"<td class='src'>{s.source_id}</td>"
            f"<td>{last}</td>"
            f"<td class='num'>{s.record_count or 0:,}</td>"
            f"<td class='num'>{s.consecutive_failures}</td>"
            f"<td class='num'>{lag}</td>"
            f"<td class='note'>{s.note}</td>"
            f"</tr>\n"
        )

    # Build intelligence feed rows
    intel_rows = ""
    sev_colors = {"CRITICAL": "#ef4444", "ALERT": "#f59e0b", "WATCH": "#3b82f6", "INFO": "#6b7280"}
    for ev in event_rows:
        sev = ev["severity"] or "INFO"
        sc = sev_colors.get(sev, "#6b7280")
        dt = ev["detected_at"].strftime("%b %d, %H:%M") if ev["detected_at"] else "—"
        hs = ev["health_score"]
        band = ev["health_band"] or "—"
        hs_display = f"{hs}" if hs is not None else "—"
        band_color = {"GREEN": "#22c55e", "AMBER": "#f59e0b", "RED": "#ef4444"}.get(band, "#6b7280")
        intel_rows += (
            f'<tr>'
            f'<td><span class="badge" style="background:{sc}">{sev}</span></td>'
            f"<td class='evt'>{ev['event_type']}</td>"
            f"<td class='company'>{ev['company']}</td>"
            f"<td>{ev['source']}</td>"
            f'<td><span style="color:{band_color};font-weight:600">{hs_display}</span></td>'
            f"<td>{dt}</td>"
            f"</tr>\n"
        )

    # Build cost breakdown rows
    cost_by_date: dict = {}
    for cr in cost_rows:
        d = str(cr["log_date"])
        if d not in cost_by_date:
            cost_by_date[d] = {}
        cost_by_date[d][cr["service"]] = float(cr["cost"])
    cost_table_rows = ""
    for d, services in list(cost_by_date.items())[:14]:
        total_d = sum(services.values())
        detail = ", ".join(f"{k}: ₹{v:.2f}" for k, v in services.items())
        cost_table_rows += f"<tr><td>{d}</td><td class='num'>₹{total_d:.2f}</td><td class='note'>{detail}</td></tr>\n"

    # Entity resolution rows
    unmapped_rows = ""
    for ur in unmapped_by_source:
        unmapped_rows += f"<tr><td>{ur['source']}</td><td class='num'>{ur['cnt']:,}</td></tr>\n"

    resolved_rows = ""
    for lr in legal_by_source:
        resolved_rows += f"<tr><td>{lr['source']}</td><td class='num'>{lr['cnt']:,}</td></tr>\n"

    # Compute resolution rate
    total_signals = legal_total + unmapped_total
    resolution_rate = round((legal_total / total_signals) * 100, 1) if total_signals > 0 else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="120">
<title>BI Signal Intelligence — Operator Console</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0a0a0f;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:24px 32px}}
  .header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;border-bottom:1px solid #1e293b;padding-bottom:16px}}
  .header h1{{font-size:22px;font-weight:700;color:#f8fafc;letter-spacing:-0.5px}}
  .header .sub{{color:#64748b;font-size:12px;margin-top:4px}}
  .header .live{{display:flex;align-items:center;gap:6px;color:#22c55e;font-size:12px;font-weight:500}}
  .header .live .dot{{width:8px;height:8px;background:#22c55e;border-radius:50%;animation:pulse 2s infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:0.3}}}}

  .kpi-grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:28px}}
  .kpi{{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px 18px}}
  .kpi .label{{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px}}
  .kpi .value{{font-size:26px;font-weight:700;color:#f8fafc}}
  .kpi .value.green{{color:#22c55e}} .kpi .value.amber{{color:#f59e0b}}
  .kpi .value.red{{color:#ef4444}} .kpi .value.blue{{color:#3b82f6}}

  .tabs{{display:flex;gap:0;margin-bottom:0;border-bottom:1px solid #1e293b}}
  .tab{{padding:10px 24px;cursor:pointer;color:#64748b;font-size:13px;font-weight:500;border-bottom:2px solid transparent;transition:all 0.2s}}
  .tab:hover{{color:#e2e8f0}} .tab.active{{color:#3b82f6;border-bottom-color:#3b82f6}}

  .panel{{display:none;padding:20px 0}} .panel.active{{display:block}}

  .section{{margin-bottom:28px}}
  .section h2{{font-size:15px;font-weight:600;color:#f8fafc;margin-bottom:12px;display:flex;align-items:center;gap:8px}}
  .section h2 .count{{background:#1e293b;color:#94a3b8;font-size:11px;padding:2px 8px;border-radius:10px}}

  table{{width:100%;border-collapse:collapse;font-size:12px}}
  thead th{{background:#111827;color:#64748b;padding:8px 12px;text-align:left;font-weight:500;text-transform:uppercase;font-size:10px;letter-spacing:0.5px;border-bottom:1px solid #1e293b;position:sticky;top:0}}
  tbody tr{{border-bottom:1px solid #1e293b;transition:background 0.15s}}
  tbody tr:hover{{background:#111827}}
  td{{padding:8px 12px;vertical-align:middle}}
  td.num{{text-align:right;font-variant-numeric:tabular-nums}}
  td.src{{font-weight:500;color:#e2e8f0}}
  td.company{{max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#cbd5e1}}
  td.evt{{font-weight:500;color:#e2e8f0;font-size:11px}}
  td.note{{color:#94a3b8;font-size:11px}}

  .badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;color:#fff;letter-spacing:0.3px}}

  .cost-cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px}}
  .cost-card{{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px}}
  .cost-card .label{{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:0.5px}}
  .cost-card .val{{font-size:24px;font-weight:700;color:#f8fafc;margin-top:4px}}

  .stat-row{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}}
  .stat-box{{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px}}
  .stat-box h3{{font-size:13px;color:#94a3b8;margin-bottom:10px}}

  .meter{{height:8px;background:#1e293b;border-radius:4px;overflow:hidden;margin-top:8px}}
  .meter-fill{{height:100%;border-radius:4px;transition:width 0.5s}}

  .empty{{text-align:center;padding:40px;color:#475569;font-size:13px}}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>BI Signal Intelligence</h1>
    <div class="sub">Operator Console &nbsp;&middot;&nbsp; {as_of} &nbsp;&middot;&nbsp; Auto-refresh 2m</div>
  </div>
  <div class="live"><span class="dot"></span> LIVE</div>
</div>

<!-- KPI Strip -->
<div class="kpi-grid">
  <div class="kpi">
    <div class="label">Total Events</div>
    <div class="value">{len(event_rows)}</div>
  </div>
  <div class="kpi">
    <div class="label">Alerts Delivered</div>
    <div class="value blue">{alerts_all_time}</div>
  </div>
  <div class="kpi">
    <div class="label">Scrapers Active</div>
    <div class="value green">{summary.green}</div>
  </div>
  <div class="kpi">
    <div class="label">Scrapers Down</div>
    <div class="value red">{summary.red}</div>
  </div>
  <div class="kpi">
    <div class="label">Resolution Rate</div>
    <div class="value {'green' if resolution_rate > 50 else 'amber' if resolution_rate > 20 else 'red'}">{resolution_rate}%</div>
  </div>
  <div class="kpi">
    <div class="label">Spend (All Time)</div>
    <div class="value">₹{float(cost_total):.0f}</div>
  </div>
</div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" onclick="showTab('intel')">Intelligence Feed</div>
  <div class="tab" onclick="showTab('scrapers')">Scrapers ({summary.total_scrapers})</div>
  <div class="tab" onclick="showTab('costs')">Costs</div>
  <div class="tab" onclick="showTab('resolution')">Entity Resolution</div>
  <div class="tab" onclick="showTab('alerts')">Alerts ({alerts_all_time})</div>
</div>

<!-- Intelligence Feed Panel -->
<div class="panel active" id="panel-intel">
  <div class="section">
    <h2>Recent Signals <span class="count">{len(event_rows)} latest</span></h2>
    {'<table><thead><tr><th>SEVERITY</th><th>SIGNAL</th><th>COMPANY</th><th>SOURCE</th><th>HEALTH</th><th>DETECTED</th></tr></thead><tbody>' + intel_rows + '</tbody></table>' if intel_rows else '<div class="empty">No signals detected yet. Scrapers are collecting data — intelligence events will appear here as they fire.</div>'}
  </div>
</div>

<!-- Scrapers Panel -->
<div class="panel" id="panel-scrapers">
  <div class="section">
    <div style="display:flex;gap:10px;margin-bottom:16px">
      <span class="badge" style="background:#22c55e">{summary.green} GREEN</span>
      <span class="badge" style="background:#f59e0b">{summary.amber} AMBER</span>
      <span class="badge" style="background:#ef4444">{summary.red} RED</span>
      <span class="badge" style="background:#6b7280">{summary.grey} NEVER RAN</span>
    </div>
    <table>
    <thead><tr><th>STATE</th><th>SOURCE</th><th>LAST PULL</th><th>RECORDS</th><th>FAILURES</th><th>LAG</th><th>NOTE</th></tr></thead>
    <tbody>
    {scraper_rows}
    </tbody>
    </table>
  </div>
</div>

<!-- Costs Panel -->
<div class="panel" id="panel-costs">
  <div class="cost-cards">
    <div class="cost-card">
      <div class="label">Today</div>
      <div class="val" style="color:#22c55e">₹{float(cost_today):.2f}</div>
    </div>
    <div class="cost-card">
      <div class="label">This Month</div>
      <div class="val">₹{float(cost_month):.2f}</div>
    </div>
    <div class="cost-card">
      <div class="label">All Time</div>
      <div class="val">₹{float(cost_total):.2f}</div>
    </div>
  </div>
  <div class="section">
    <h2>Daily Breakdown</h2>
    {'<table><thead><tr><th>DATE</th><th>TOTAL</th><th>DETAIL</th></tr></thead><tbody>' + cost_table_rows + '</tbody></table>' if cost_table_rows else '<div class="empty">No costs recorded yet.</div>'}
  </div>
  <div style="background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px;margin-top:16px">
    <div style="color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:0.5px">Daily Budget Threshold</div>
    <div style="font-size:18px;font-weight:600;margin-top:4px">₹500.00 / day</div>
    <div class="meter"><div class="meter-fill" style="width:{min(float(cost_today)/500*100, 100):.1f}%;background:{'#22c55e' if float(cost_today) < 250 else '#f59e0b' if float(cost_today) < 400 else '#ef4444'}"></div></div>
    <div style="color:#64748b;font-size:11px;margin-top:4px">₹{float(cost_today):.2f} of ₹500 used today ({float(cost_today)/500*100:.1f}%)</div>
  </div>
</div>

<!-- Entity Resolution Panel -->
<div class="panel" id="panel-resolution">
  <div class="stat-row">
    <div class="stat-box">
      <h3>Resolution Rate</h3>
      <div style="font-size:36px;font-weight:700;color:{'#22c55e' if resolution_rate > 50 else '#f59e0b' if resolution_rate > 20 else '#ef4444'}">{resolution_rate}%</div>
      <div class="meter"><div class="meter-fill" style="width:{resolution_rate}%;background:{'#22c55e' if resolution_rate > 50 else '#f59e0b' if resolution_rate > 20 else '#ef4444'}"></div></div>
      <div style="color:#64748b;font-size:11px;margin-top:8px">{legal_total:,} resolved / {total_signals:,} total signals</div>
    </div>
    <div class="stat-box">
      <h3>Pipeline Summary</h3>
      <div style="display:flex;flex-direction:column;gap:8px;margin-top:8px">
        <div style="display:flex;justify-content:space-between"><span style="color:#94a3b8">Resolved to CIN</span><span style="color:#22c55e;font-weight:600">{legal_total:,}</span></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#94a3b8">Unmapped (no CIN)</span><span style="color:#ef4444;font-weight:600">{unmapped_total:,}</span></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#94a3b8">Watchlists Active</span><span style="color:#3b82f6;font-weight:600">{watchlist_count}</span></div>
        <div style="display:flex;justify-content:space-between"><span style="color:#94a3b8">Master Entities</span><span style="font-weight:600">21.5L+</span></div>
      </div>
    </div>
  </div>
  <div class="stat-row">
    <div class="stat-box">
      <h3>Resolved by Source</h3>
      {'<table><thead><tr><th>SOURCE</th><th>COUNT</th></tr></thead><tbody>' + resolved_rows + '</tbody></table>' if resolved_rows else '<div class="empty">—</div>'}
    </div>
    <div class="stat-box">
      <h3>Unmapped by Source</h3>
      {'<table><thead><tr><th>SOURCE</th><th>COUNT</th></tr></thead><tbody>' + unmapped_rows + '</tbody></table>' if unmapped_rows else '<div class="empty">—</div>'}
    </div>
  </div>
</div>

<!-- Alerts Panel -->
<div class="panel" id="panel-alerts">
  <div class="section">
    <h2>Delivery Summary <span class="count">{alerts_all_time} total</span></h2>
    {'<table><thead><tr><th>CHANNEL</th><th>STATUS</th><th>COUNT</th></tr></thead><tbody>' + ''.join(f"<tr><td>{r['delivery_channel'] or '—'}</td><td>{r['delivery_status']}</td><td class='num'>{r['cnt']}</td></tr>" for r in alerts_by_channel) + '</tbody></table>' if alerts_by_channel else '<div class="empty">No alerts delivered yet.</div>'}
  </div>
</div>

<script>
function showTab(name) {{
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('panel-' + name).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""
    return HTMLResponse(content=html)
