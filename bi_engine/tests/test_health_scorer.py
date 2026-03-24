"""
Pytest suite for health scoring engine.
All DB interactions are mocked. No real DB required.

Run: pytest tests/test_health_scorer.py -v
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

from detection.health_scorer import ContagionPropagator, HealthScorer
from detection.sector_cluster import SectorClusterDetector
from detection.shell_detector import ShellDetector


def make_pool(
    fetch_map: Dict[str, Any] = None,
    fetchrow_map: Dict = None,
    fetchval_map: Dict = None,
) -> MagicMock:
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetchval = AsyncMock(return_value=None)
    pool.execute = AsyncMock(return_value=None)

    conn = MagicMock()
    conn.fetchrow = pool.fetchrow
    conn.fetch = pool.fetch
    conn.fetchval = pool.fetchval
    conn.execute = pool.execute
    conn.transaction = MagicMock(return_value=_AsyncNullContextManager())

    acquire_cm = _AsyncNullContextManager(conn)
    pool.acquire = MagicMock(return_value=acquire_cm)
    return pool


class _AsyncNullContextManager:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        pass


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_filing_freshness_overdue_18_months():
    pool = make_pool()
    agm_date = date.today() - timedelta(days=20 * 30)
    pool.fetchrow = AsyncMock(return_value={"date_of_last_agm": agm_date, "status": "Active"})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_filing_freshness("TEST001"))

    assert score <= 40.0
    assert score == 40.0


def test_director_stability_three_changes():
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value={"change_count": 3})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_director_stability("TEST001"))

    assert score == 20.0


def test_legal_risk_nclt_filing():
    pool = make_pool()
    pool.fetch = AsyncMock(return_value=[{"case_type": "NCLT_7", "cnt": 1}])

    scorer = HealthScorer(pool)
    score = run(scorer._compute_legal_risk("TEST001"))

    assert score == 5.0


def test_legal_risk_sarfaesi_possession():
    pool = make_pool()
    pool.fetch = AsyncMock(return_value=[{"case_type": "SARFAESI_13_4", "cnt": 1}])

    scorer = HealthScorer(pool)
    score = run(scorer._compute_legal_risk("TEST001"))

    assert score == 5.0


def test_financial_health_de_ratio_3():
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value={"debt_to_equity": 3.0})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_financial_health("TEST001"))

    assert score == 40.0


def test_financial_health_no_data():
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value=None)

    scorer = HealthScorer(pool)
    score = run(scorer._compute_financial_health("TEST001"))

    assert score == 50.0


def test_capital_trajectory_increased_10_pct():
    pool = make_pool()
    pool.fetchrow = AsyncMock(
        side_effect=[
            {"paid_up_capital": 1_100_000},
            {"prev_capital": "1000000"},
        ]
    )

    scorer = HealthScorer(pool)
    score = run(scorer._compute_capital_trajectory("TEST001"))

    assert score == 100.0


def test_contagion_two_red_directors():
    pool = make_pool()
    pool.fetch = AsyncMock(
        side_effect=[
            [{"din": "DIN001"}, {"din": "DIN002"}],
            [
                {"cin": "PEER001", "health_band": "RED"},
                {"cin": "PEER002", "health_band": "RED"},
            ],
        ]
    )

    scorer = HealthScorer(pool)
    final_score, penalty, sources = run(scorer._apply_contagion_penalty("TEST001", 70.0))

    assert penalty == 30.0
    assert final_score == 40.0
    assert set(sources) == {"PEER001", "PEER002"}


def test_contagion_floor_at_zero():
    pool = make_pool()
    pool.fetch = AsyncMock(
        side_effect=[
            [{"din": "DIN001"}],
            [{"cin": f"PEER{i:03d}", "health_band": "RED"} for i in range(10)],
        ]
    )

    scorer = HealthScorer(pool)
    final_score, penalty, _ = run(scorer._apply_contagion_penalty("TEST001", 10.0))

    assert final_score == 0.0
    assert penalty == 150.0


def test_band_change_triggers_contagion(monkeypatch):
    pool = make_pool()

    scorer = HealthScorer(pool)
    scorer._persist_score = AsyncMock(return_value=None)
    scorer._compute_filing_freshness = AsyncMock(return_value=0.0)
    scorer._compute_director_stability = AsyncMock(return_value=20.0)
    scorer._compute_legal_risk = AsyncMock(return_value=5.0)
    scorer._compute_financial_health = AsyncMock(return_value=50.0)
    scorer._compute_capital_trajectory = AsyncMock(return_value=20.0)

    pool.fetchrow = AsyncMock(return_value={"health_score": 55, "health_band": "AMBER"})
    pool.fetch = AsyncMock(return_value=[])

    propagator = ContagionPropagator(pool, scorer)

    async def mock_propagate(cin, new_band, depth=0, _visited=None):
        return []

    propagator.propagate = mock_propagate

    result = run(scorer.recompute("TEST001", triggering_event_id=99))

    assert result.band == "RED"
    assert result.previous_band == "AMBER"
    assert result.band != result.previous_band


def test_contagion_depth_limit():
    pool = make_pool()
    scorer = HealthScorer(pool)
    propagator = ContagionPropagator(pool, scorer)

    result = run(propagator.propagate("TEST001", "RED", depth=2))

    assert result == []
    pool.fetch.assert_not_called()


def test_shell_detection_all_conditions_met():
    pool = make_pool()
    pool.fetch = AsyncMock(
        side_effect=[
            [
                {"column_name": "authorized_capital"},
                {"column_name": "date_of_last_agm"},
                {"column_name": "epfo_id"},
                {"column_name": "gstin"},
            ],
            [
                {
                    "cin": "TEST001",
                    "date_of_incorporation": date.today() - timedelta(days=365),
                    "authorized_capital": 500_000,
                    "date_of_last_agm": None,
                    "director_din": "DIN001",
                    "other_board_count": 7,
                }
            ],
        ]
    )

    detector = ShellDetector(pool)
    fired = run(detector.check("TEST001"))

    assert fired is True
    pool.execute.assert_called_once()
    call_args = pool.execute.call_args
    assert "SHELL_RISK" in call_args[0][0]


def test_shell_detection_five_of_six_conditions():
    pool = make_pool()
    pool.fetch = AsyncMock(
        side_effect=[
            [
                {"column_name": "authorized_capital"},
                {"column_name": "date_of_last_agm"},
                {"column_name": "epfo_id"},
                {"column_name": "gstin"},
            ],
            [],
        ]
    )

    detector = ShellDetector(pool)
    fired = run(detector.check("TEST001"))

    assert fired is False
    pool.execute.assert_not_called()


def test_sector_cluster_fires_when_threshold_met():
    pool = make_pool()
    cluster_rows = [
        {
            "registered_state": "GJ",
            "industrial_class": "2410",
            "stressed_count": 5,
            "affected_cins": ["C1", "C2", "C3", "C4", "C5"],
        }
    ]
    pool.fetch = AsyncMock(return_value=cluster_rows)
    pool.fetchval = AsyncMock(return_value=None)

    detector = SectorClusterDetector(pool)
    count = run(detector.run())

    assert count == 1
    pool.execute.assert_called_once()
    insert_call = pool.execute.call_args[0][0]
    assert "SECTOR_CLUSTER_ALERT" in insert_call


def test_sector_cluster_deduplication():
    pool = make_pool()
    cluster_rows = [
        {
            "registered_state": "GJ",
            "industrial_class": "2410",
            "stressed_count": 6,
            "affected_cins": ["C1", "C2", "C3", "C4", "C5", "C6"],
        }
    ]
    pool.fetch = AsyncMock(return_value=cluster_rows)
    pool.fetchval = AsyncMock(return_value=42)

    detector = SectorClusterDetector(pool)
    count = run(detector.run())

    assert count == 0
    pool.execute.assert_not_called()
