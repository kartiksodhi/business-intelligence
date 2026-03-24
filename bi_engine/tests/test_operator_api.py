"""
Operator API tests.

Uses httpx.AsyncClient with FastAPI's TestClient via ASGITransport.
All DB calls are mocked — no real PostgreSQL required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.main import app


def make_async_mock_connection(fetch_return=None, fetchrow_return=None, execute_return=None):
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_return or [])
    conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    conn.execute = AsyncMock(return_value=None)
    return conn


class FakeRow(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc


def row(**kwargs) -> FakeRow:
    return FakeRow(kwargs)


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def override_db(mock_conn):
    from api.dependencies import get_db

    async def _get_mock_db():
        yield mock_conn

    app.dependency_overrides[get_db] = _get_mock_db


def clear_overrides():
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_status_lag_hours_computed(client):
    now = datetime.now(timezone.utc)
    past = (now - timedelta(hours=2)).replace(tzinfo=None)
    future = (now + timedelta(hours=1)).replace(tzinfo=None)

    mock_rows = [
        row(
            source_id="mca_ogd",
            status="OK",
            last_pull_at=past,
            record_count=1823456,
            consecutive_failures=0,
            next_pull_at=past,
        ),
        row(
            source_id="nclt",
            status="OK",
            last_pull_at=past,
            record_count=50000,
            consecutive_failures=0,
            next_pull_at=future,
        ),
    ]

    conn = make_async_mock_connection(fetch_return=mock_rows)
    override_db(conn)
    try:
        resp = await client.get("/op/status")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

        mca = next(item for item in data if item["source_id"] == "mca_ogd")
        nclt = next(item for item in data if item["source_id"] == "nclt")
        assert mca["lag_hours"] is not None
        assert mca["lag_hours"] > 1.9
        assert nclt["lag_hours"] is None
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_get_events_today_severity_filter(client):
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
        call_args = conn.fetch.call_args
        assert "CRITICAL" in call_args.args or "CRITICAL" in str(call_args)
    finally:
        clear_overrides()


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


@pytest.mark.asyncio
async def test_get_accuracy_pct_computed(client):
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


@pytest.mark.asyncio
async def test_get_costs_today_threshold_breached(client):
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


@pytest.mark.asyncio
async def test_resolve_queue_item_422_on_bad_cin(client):
    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "entity_resolution_queue" in query:
            return row(id=123)
        if "master_entities" in query:
            return None
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
        assert conn.execute.called
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_captcha_solve_stores_solution_and_logs(client):
    from api.routers.operator import _captcha_solutions

    conn = make_async_mock_connection()
    override_db(conn)
    _captcha_solutions.pop("nclt", None)

    payload = {"source_id": "nclt", "solution": "XY7Z2"}

    try:
        resp = await client.post("/op/captcha/solve", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["source_id"] == "nclt"
        assert _captcha_solutions.get("nclt") == "XY7Z2"
        assert conn.execute.called
        call_args_str = str(conn.execute.call_args)
        assert "captcha_log" in call_args_str
        assert "MANUAL" in call_args_str
    finally:
        _captcha_solutions.pop("nclt", None)
        clear_overrides()


@pytest.mark.asyncio
async def test_get_subscribers_returns_counts(client):
    conn = make_async_mock_connection(
        fetch_return=[
            row(
                id=1,
                name="Asha",
                email="asha@example.com",
                severity_threshold="RED",
                watchlist_count=2,
                alert_count_this_month=5,
            )
        ]
    )
    override_db(conn)
    try:
        resp = await client.get("/op/subscribers")
        assert resp.status_code == 200
        data = resp.json()
        assert data == [
            {
                "id": 1,
                "name": "Asha",
                "email": "asha@example.com",
                "severity_threshold": "RED",
                "watchlist_count": 2,
                "alert_count_this_month": 5,
            }
        ]
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_create_subscriber(client):
    conn = make_async_mock_connection(
        fetchrow_return=row(
            id=7,
            name="Kiran",
            email="kiran@example.com",
            severity_threshold="CRITICAL",
        )
    )
    override_db(conn)
    try:
        resp = await client.post(
            "/op/subscribers",
            json={
                "name": "Kiran",
                "email": "kiran@example.com",
                "severity_threshold": "CRITICAL",
            },
        )
        assert resp.status_code == 201
        assert resp.json()["id"] == 7
        assert resp.json()["severity_threshold"] == "CRITICAL"
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_delete_subscriber_returns_204(client):
    conn = make_async_mock_connection()
    override_db(conn)
    try:
        resp = await client.delete("/op/subscribers/9")
        assert resp.status_code == 204
        assert conn.execute.await_count == 3
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_get_subscriber_watchlists(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "SELECT id FROM subscribers" in query:
            return row(id=3)
        return None

    conn.fetchrow = mock_fetchrow
    conn.fetch = AsyncMock(
        return_value=[
            row(
                cin="U27100GJ2015PTC082456",
                company_name="Acme Steel Pvt Ltd",
                added_at=now,
            )
        ]
    )
    conn.execute = AsyncMock(return_value=None)
    override_db(conn)
    try:
        resp = await client.get("/op/watchlists/3")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["cin"] == "U27100GJ2015PTC082456"
        assert data[0]["company_name"] == "Acme Steel Pvt Ltd"
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_create_subscriber_watchlist(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = AsyncMock()

    async def mock_fetchrow(query, *args):
        if "SELECT id FROM subscribers" in query:
            return row(id=11)
        if "FROM master_entities" in query:
            return row(company_name="Beta Manufacturing Ltd")
        if "SELECT cin, added_at" in query:
            return None
        if "INSERT INTO watchlists" in query:
            return row(cin="U12345MH2001PTC123456", added_at=now)
        return None

    conn.fetchrow = mock_fetchrow
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)
    override_db(conn)
    try:
        resp = await client.post(
            "/op/watchlists/11",
            json={"cin": "U12345MH2001PTC123456"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["cin"] == "U12345MH2001PTC123456"
        assert data["company_name"] == "Beta Manufacturing Ltd"
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_delete_subscriber_watchlist_returns_204(client):
    conn = make_async_mock_connection()
    override_db(conn)
    try:
        resp = await client.delete("/op/watchlists/11/U12345MH2001PTC123456")
        assert resp.status_code == 204
        assert conn.execute.await_count == 1
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_get_company_events_returns_latest_100_shape(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = make_async_mock_connection(
        fetch_return=[
            row(
                event_date=now,
                event_type="NCLT_SEC7_FILED",
                severity="CRITICAL",
                source="nclt",
                notes='{"case_number":"CP/1/2026"}',
            )
        ]
    )
    override_db(conn)
    try:
        resp = await client.get("/op/company/U27100GJ2015PTC082456/events")
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["event_type"] == "NCLT_SEC7_FILED"
        assert data[0]["severity"] == "CRITICAL"
    finally:
        clear_overrides()


@pytest.mark.asyncio
async def test_get_company_health_summary(client):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = make_async_mock_connection(
        fetchrow_return=row(
            cin="U27100GJ2015PTC082456",
            health_score=31,
            band="RED",
            last_computed=now,
        )
    )
    override_db(conn)
    try:
        resp = await client.get("/op/company/U27100GJ2015PTC082456/health")
        assert resp.status_code == 200
        assert resp.json()["band"] == "RED"
        assert resp.json()["health_score"] == 31
    finally:
        clear_overrides()
