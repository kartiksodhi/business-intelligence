from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from detection.signal_combiner import check_combinations


def _make_conn(events=None, master_status="Active", insert_id=999):
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=events or [])
    conn.fetchrow = AsyncMock(
        side_effect=[
            None,
            {"status": master_status},
            {"id": insert_id},
        ]
    )
    return conn


@pytest.mark.asyncio
async def test_signal_combiner_returns_empty_when_no_match():
    conn = MagicMock()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "id": 1,
                "source": "ecourts",
                "event_type": "ECOURTS_SEC138",
                "severity": "ALERT",
                "detected_at": datetime.now(timezone.utc),
                "data_json": {},
            }
        ]
    )
    conn.fetchrow = AsyncMock(return_value=None)

    result = await check_combinations("CIN123", "ECOURTS_SEC138", conn)

    assert result == []


@pytest.mark.asyncio
async def test_signal_combiner_fires_zombie_company():
    conn = _make_conn(
        events=[
            {
                "id": 11,
                "source": "gst",
                "event_type": "GST_CANCELLED",
                "severity": "CRITICAL",
                "detected_at": datetime.now(timezone.utc),
                "data_json": {},
            }
        ]
    )

    result = await check_combinations("CIN123", "GST_CANCELLED", conn)

    assert len(result) == 1
    assert result[0]["event_type"] == "ZOMBIE_COMPANY"
