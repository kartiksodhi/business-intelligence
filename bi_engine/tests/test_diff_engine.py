"""
pytest test suite for the diff engine.

Run: pytest tests/test_diff_engine.py -v
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from detection.diff_engine import DiffEngine, FAILURE_THRESHOLD
from detection.detectors.directors import DirectorDetector
from detection.detectors.ecourts import ECourtsDetector
from detection.detectors.nclt import NCLTDetector
from detection.detectors.ogd import OGDDetector
from detection.detectors.sarfaesi import SARFAESIDetector


def _make_pool(fetchrow_return=None, fetch_return=None, execute_return=None):
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=fetchrow_return)
    pool.fetch = AsyncMock(return_value=fetch_return or [])
    pool.execute = AsyncMock(return_value=execute_return)

    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"id": 1})
    conn.fetch = AsyncMock(return_value=fetch_return or [])
    conn.execute = AsyncMock()

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool, conn


def _agm_date_overdue() -> str:
    past = datetime.now(timezone.utc) - timedelta(days=600)
    return past.strftime("%Y-%m-%d")


def _agm_date_recent() -> str:
    past = datetime.now(timezone.utc) - timedelta(days=100)
    return past.strftime("%Y-%m-%d")


@pytest.mark.asyncio
async def test_hash_unchanged_fires_nothing():
    data = [{"cin": "U12345MH2020PTC123456", "status": "Active"}]

    sorted_data = sorted(
        data,
        key=lambda record: (
            str(record.get("cin") or ""),
            str(record.get("case_number") or ""),
            json.dumps(record, sort_keys=True, default=str),
        ),
    )
    serialised = json.dumps(sorted_data, sort_keys=True, default=str)
    expected_hash = hashlib.sha256(serialised.encode()).hexdigest()

    pool, conn = _make_pool(
        fetchrow_return={"last_data_hash": expected_hash, "consecutive_failures": 0}
    )
    engine = DiffEngine(pool)

    result = await engine.process_source("mca_ogd", data)

    assert result.hash_changed is False
    assert result.events_fired == 0
    assert result.records_processed == 1


@pytest.mark.asyncio
async def test_ogd_status_change_fires_event():
    old_records = [
        {
            "cin": "U12345MH2020PTC123456",
            "status": "Active",
            "company_name": "Acme Ltd",
            "paid_up_capital": None,
            "date_of_last_agm": None,
        }
    ]
    new_records = [
        {
            "cin": "U12345MH2020PTC123456",
            "status": "Struck Off",
            "company_name": "Acme Ltd",
            "paid_up_capital": None,
            "date_of_last_agm": None,
        }
    ]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)

    status_events = [event for event in events if event.event_type == "STATUS_CHANGE"]
    assert len(status_events) == 1
    assert status_events[0].severity == "ALERT"
    assert status_events[0].data["old_status"] == "Active"
    assert status_events[0].data["new_status"] == "Struck Off"


@pytest.mark.asyncio
async def test_ogd_capital_change_above_threshold():
    old = [
        {
            "cin": "U1",
            "status": "Active",
            "company_name": "X",
            "paid_up_capital": "1000000",
            "date_of_last_agm": None,
        }
    ]
    new = [
        {
            "cin": "U1",
            "status": "Active",
            "company_name": "X",
            "paid_up_capital": "2000000",
            "date_of_last_agm": None,
        }
    ]

    detector = OGDDetector()
    events = await detector.detect_events(old, new, db=None)
    cap_events = [event for event in events if event.event_type == "CAPITAL_CHANGE"]
    assert len(cap_events) == 1


@pytest.mark.asyncio
async def test_ogd_capital_change_below_threshold_not_fired():
    old = [
        {
            "cin": "U1",
            "status": "Active",
            "company_name": "X",
            "paid_up_capital": "1000000",
            "date_of_last_agm": None,
        }
    ]
    new = [
        {
            "cin": "U1",
            "status": "Active",
            "company_name": "X",
            "paid_up_capital": "1200000",
            "date_of_last_agm": None,
        }
    ]

    detector = OGDDetector()
    events = await detector.detect_events(old, new, db=None)
    cap_events = [event for event in events if event.event_type == "CAPITAL_CHANGE"]
    assert len(cap_events) == 0


@pytest.mark.asyncio
async def test_ogd_agm_overdue_fires_once():
    recent_agm = _agm_date_recent()
    overdue_agm = _agm_date_overdue()

    old_records = [
        {
            "cin": "U2",
            "status": "Active",
            "company_name": "Y",
            "paid_up_capital": None,
            "date_of_last_agm": recent_agm,
        }
    ]
    new_records = [
        {
            "cin": "U2",
            "status": "Active",
            "company_name": "Y",
            "paid_up_capital": None,
            "date_of_last_agm": overdue_agm,
        }
    ]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)
    agm_events = [event for event in events if event.event_type == "AGM_OVERDUE"]
    assert len(agm_events) == 1

    events2 = await detector.detect_events(new_records, new_records, db=None)
    agm_events2 = [event for event in events2 if event.event_type == "AGM_OVERDUE"]
    assert len(agm_events2) == 0


@pytest.mark.asyncio
async def test_ogd_new_cin_fires_new_company():
    old_records: List[dict] = []
    new_records = [
        {
            "cin": "U99",
            "status": "Active",
            "company_name": "NewCo",
            "paid_up_capital": None,
            "date_of_last_agm": None,
        }
    ]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)
    new_events = [event for event in events if event.event_type == "NEW_COMPANY"]
    assert len(new_events) == 1
    assert new_events[0].severity == "INFO"


@pytest.mark.asyncio
async def test_nclt_dedup_same_case_number():
    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[{"cin": "U3", "case_number": "NCLT/MB/7/2024"}])

    new_records = [
        {
            "cin": "U3",
            "case_number": "NCLT/MB/7/2024",
            "filing_type": "Section 7",
            "bench": "Mumbai",
            "petitioner": "Bank A",
            "respondent": "Acme",
            "filing_date": "2024-01-15",
            "next_date": "2024-02-10",
        }
    ]

    detector = NCLTDetector()
    events = await detector.detect_events([], new_records, db)
    assert len(events) == 0


@pytest.mark.asyncio
async def test_sarfaesi_stages():
    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[])

    stages = [
        ("13(2)", "SARFAESI_DEMAND_NOTICE", "ALERT"),
        ("13(4)", "SARFAESI_POSSESSION_TAKEN", "CRITICAL"),
        ("auction scheduled", "SARFAESI_AUCTION_SCHEDULED", "CRITICAL"),
        ("auction completed", "SARFAESI_AUCTION_COMPLETED", "ALERT"),
    ]

    for stage, expected_event, expected_severity in stages:
        records = [
            {
                "cin": f"U_{stage}",
                "case_number": f"CASE_{stage}",
                "notice_stage": stage,
                "secured_creditor": "SBI",
                "property_description": "Plot 5",
                "outstanding_amount": 5000000,
                "notice_date": "2024-03-01",
                "auction_date": None,
                "reserve_price": None,
            }
        ]
        detector = SARFAESIDetector()
        events = await detector.detect_events([], records, db)
        assert len(events) == 1
        assert events[0].event_type == expected_event
        assert events[0].severity == expected_severity


@pytest.mark.asyncio
async def test_ecourts_sec138_escalation():
    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[])
    db.fetchrow = AsyncMock(return_value={"cnt": 2})

    new_records = [
        {
            "cin": "U5",
            "case_number": "CC/138/2024/03",
            "case_type": "Section 138 NI Act",
            "disposal_status": "",
            "court": "MM Court Mumbai",
            "complainant": "Vendor X",
            "filing_date": "2024-03-01",
            "claim_amount": None,
        }
    ]

    detector = ECourtsDetector()
    events = await detector.detect_events([], new_records, db)

    assert len(events) == 1
    assert events[0].event_type == "SEC138_MULTIPLE"
    assert events[0].severity == "CRITICAL"
    assert events[0].data["sec138_count"] == 3


@pytest.mark.asyncio
async def test_source_unreachable_after_threshold():
    pool, conn = _make_pool()
    pool.fetchrow = AsyncMock(
        return_value={"consecutive_failures": FAILURE_THRESHOLD, "status": "UNREACHABLE"}
    )

    engine = DiffEngine(pool)
    pool.fetch = AsyncMock(return_value=[])

    with patch.object(engine, "_load_last_state", return_value={"last_data_hash": "old_hash"}):
        with patch.object(engine, "_compute_hash", return_value="new_hash"):
            with patch.object(engine, "_load_old_records", return_value=[]):
                result = await engine.process_source("unknown_source", [{"cin": "X"}])

    assert len(result.errors) > 0
    pool.execute.assert_called()
    calls = [str(call) for call in pool.execute.call_args_list]
    assert any("SOURCE_UNREACHABLE" in call for call in calls)


@pytest.mark.asyncio
async def test_director_resigned_fires_event():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 2})

    old_records = [
        {
            "din": "00123456",
            "cin": "U6",
            "director_name": "John Doe",
            "designation": "Director",
            "appointment_date": "2015-01-01",
            "cessation_date": None,
        }
    ]
    new_records = [
        {
            "din": "00123456",
            "cin": "U6",
            "director_name": "John Doe",
            "designation": "Director",
            "appointment_date": "2015-01-01",
            "cessation_date": "2024-03-15",
        }
    ]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    resigned = [event for event in events if event.event_type == "DIRECTOR_RESIGNED"]
    assert len(resigned) == 1
    assert resigned[0].severity == "WATCH"
    assert resigned[0].data["din"] == "00123456"


@pytest.mark.asyncio
async def test_cfo_resignation_escalates_to_alert():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 1})

    old_records = [
        {
            "din": "00999999",
            "cin": "U7",
            "director_name": "Jane Smith",
            "designation": "Chief Financial Officer",
            "appointment_date": "2016-06-01",
            "cessation_date": None,
        }
    ]
    new_records = [
        {
            "din": "00999999",
            "cin": "U7",
            "director_name": "Jane Smith",
            "designation": "Chief Financial Officer",
            "appointment_date": "2016-06-01",
            "cessation_date": "2024-03-15",
        }
    ]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    resigned = [event for event in events if event.event_type == "DIRECTOR_RESIGNED"]
    assert len(resigned) == 1
    assert resigned[0].severity == "ALERT"


@pytest.mark.asyncio
async def test_director_overload_fires():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 10})

    old_records: List[dict] = []
    new_records = [
        {
            "din": "00777777",
            "cin": "U8",
            "director_name": "Busy Person",
            "designation": "Director",
            "appointment_date": "2020-01-01",
            "cessation_date": None,
        }
    ]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    overload = [event for event in events if event.event_type == "DIRECTOR_OVERLOADED"]
    assert len(overload) == 1
    assert overload[0].severity == "WATCH"
    assert overload[0].data["board_count"] == 10
