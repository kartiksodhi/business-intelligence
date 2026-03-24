"""
Tests for entity resolution pipeline.

Run with: pytest tests/test_entity_resolver.py -v
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bi_engine.ingestion import entity_resolver as resolver_module
from bi_engine.ingestion.entity_resolver import (
    EntityResolver,
    ResolutionResult,
    normalize_company_name,
)


def make_row(
    cin: str = "U27100GJ2015PTC082456",
    company_name: str = "Gujarat Positra Steel Private Limited",
    registered_state: str = "GJ",
    industrial_class: str = "2710",
    sim: float = 0.95,
) -> dict:
    return {
        "cin": cin,
        "company_name": company_name,
        "registered_state": registered_state,
        "industrial_class": industrial_class,
        "sim": sim,
        "normalized_name": normalize_company_name(company_name),
    }


class FakePool:
    """
    Minimal asyncpg pool fake.

    Pre-load responses via stage_response(query_substring, row_or_rows).
    Unmatched queries return None / [].
    """

    def __init__(self):
        self._responses: list[tuple[str, Any]] = []
        self._executed: list[str] = []

    def stage_response(self, query_fragment: str, response: Any) -> None:
        self._responses.append((query_fragment, response))

    def acquire(self):
        return _FakeConnection(self)

    async def execute(self, query: str, *args) -> None:
        self._executed.append(query)


class _FakeConnection:
    def __init__(self, pool: FakePool):
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def fetchrow(self, query: str, *args) -> Optional[dict]:
        for fragment, response in self._pool._responses:
            if fragment.lower() in query.lower():
                if isinstance(response, list):
                    return response[0] if response else None
                return response
        return None

    async def fetch(self, query: str, *args) -> list[dict]:
        for fragment, response in self._pool._responses:
            if fragment.lower() in query.lower():
                if isinstance(response, list):
                    return response
                return [response] if response else []
        return []

    async def execute(self, query: str, *args) -> None:
        self._pool._executed.append(query)


@pytest.fixture(autouse=True)
def clear_candidate_cache():
    resolver_module._CANDIDATE_CACHE.clear()


@pytest.mark.asyncio
async def test_exact_cin_match():
    pool = FakePool()
    row = make_row()
    pool.stage_response("where cin =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="GUJARAT POSITRA STEEL PVT LTD",
        cin="U27100GJ2015PTC082456",
    )

    assert result.resolved is True
    assert result.cin == "U27100GJ2015PTC082456"
    assert result.confidence == 100.0
    assert result.method == "exact_cin"
    assert result.queued is False


@pytest.mark.asyncio
async def test_invalid_cin_skips_stage_and_continues():
    pool = FakePool()
    row = make_row()
    pool.stage_response("normalized_name =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="GUJARAT POSITRA STEEL PVT LTD",
        cin="INVALID",
    )

    assert result.method == "normalized_exact"
    assert result.confidence == 95.0


@pytest.mark.asyncio
async def test_normalized_pvt_vs_private():
    pool = FakePool()
    row = make_row()
    pool.stage_response("normalized_name =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve("GUJARAT POSITRA STEEL PRIVATE LIMITED")

    assert result.resolved is True
    assert result.confidence == 95.0
    assert result.method == "normalized_exact"


@pytest.mark.asyncio
async def test_normalized_llp_stripped():
    pool = FakePool()
    row = make_row(company_name="Gujarat Positra Steel LLP")
    pool.stage_response("normalized_name =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve("Gujarat Positra Steel LLP")

    assert result.resolved is True
    assert result.method == "normalized_exact"


@pytest.mark.asyncio
async def test_normalized_opc_stripped():
    pool = FakePool()
    row = make_row(company_name="Positra One Person Company")
    pool.stage_response("normalized_name =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve("POSITRA ONE PERSON COMPANY")

    assert result.resolved is True
    assert result.method == "normalized_exact"


@pytest.mark.asyncio
async def test_trigram_state_typo():
    pool = FakePool()
    row = make_row(sim=0.88)
    pool.stage_response("similarity(normalized_name", [row])

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="Gujarat Positara Steel Pvt Ltd",
        state="GJ",
    )

    assert result.resolved is True
    assert result.confidence == 90.0
    assert result.method == "trigram_state_08"


@pytest.mark.asyncio
async def test_state_filter_narrows():
    pool = FakePool()
    gj_row = make_row(cin="U27100GJ2015PTC082456", registered_state="GJ", sim=0.85)
    mh_row = make_row(cin="U27100MH2010PTC111111", registered_state="MH", sim=0.82)

    pool.stage_response("registered_state = $2", [gj_row])

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="Gujarat Positra Steel Pvt Ltd",
        state="GJ",
    )

    assert result.cin == "U27100GJ2015PTC082456"
    assert result.resolved is True


@pytest.mark.asyncio
async def test_below_threshold_queued():
    pool = FakePool()

    resolver = EntityResolver(pool)
    result = await resolver.resolve(raw_name="Some Completely Unknown Company XYZ")

    assert result.resolved is False
    assert result.queued is True
    assert result.cin is None
    assert any("entity_resolution_queue" in q for q in pool._executed)


@pytest.mark.asyncio
async def test_llm_triggered_in_ambiguous_range():
    pool = FakePool()

    candidate_a = make_row(
        cin="U27100GJ2015PTC082456",
        company_name="Gujarat Positra Steel Pvt Ltd",
        sim=0.62,
    )
    candidate_b = make_row(
        cin="U27100GJ2016PTC099999",
        company_name="Gujarat Positra Steels Pvt Ltd",
        sim=0.60,
    )

    pool.stage_response("similarity(normalized_name, $1) > $2", [candidate_a, candidate_b])

    mock_llm = AsyncMock()
    mock_response = MagicMock()
    mock_response.content = [
        MagicMock(
            text='{"matched_cin":"U27100GJ2015PTC082456","confidence_score":82,"reasoning_flag":"best_match"}'
        )
    ]
    mock_llm.messages.create = AsyncMock(return_value=mock_response)
    pool.stage_response("coalesce(sum(units)", {"total": 0})

    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_called_once()
    assert result.resolved is True
    assert result.method == "llm_fallback"
    assert result.cin == "U27100GJ2015PTC082456"


@pytest.mark.asyncio
async def test_llm_not_triggered_above_70_percent():
    pool = FakePool()
    row = make_row(sim=0.72)
    pool.stage_response("similarity(normalized_name, $1) > $2", [row])

    mock_llm = AsyncMock()
    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_not_called()
    assert result.method == "trigram_any_06"
    assert result.confidence == 60.0


@pytest.mark.asyncio
async def test_llm_monthly_cap_respected():
    pool = FakePool()

    candidate_a = make_row(sim=0.62)
    candidate_b = make_row(cin="U27100GJ2016PTC099999", sim=0.60)
    pool.stage_response("similarity(normalized_name, $1) > $2", [candidate_a, candidate_b])
    pool.stage_response("coalesce(sum(units)", {"total": 500})

    mock_llm = AsyncMock()
    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_not_called()
    assert result.resolved is False
    assert result.queued is True


def test_normalize_strips_pvt():
    assert normalize_company_name("GUJARAT STEEL PVT LTD") == "gujarat steel"


def test_normalize_strips_private_limited():
    assert normalize_company_name("Gujarat Steel Private Limited") == "gujarat steel"


def test_normalize_strips_llp():
    assert normalize_company_name("Acme Solutions LLP") == "acme"


def test_normalize_strips_opc():
    assert normalize_company_name("Ravi One Person Company") == "ravi"


def test_normalize_strips_technologies():
    assert normalize_company_name("Infosys Technologies Ltd") == "infosys"


def test_normalize_strips_enterprises():
    assert normalize_company_name("Tata Enterprises Pvt Ltd") == "tata"


def test_normalize_strips_corporation():
    assert normalize_company_name("National Corporation Ltd") == "national"


def test_normalize_removes_punctuation():
    assert normalize_company_name("A.B.C. Steel (India) Pvt. Ltd.") == "abc steel"


def test_normalize_collapses_spaces():
    assert normalize_company_name("  Reliance   Industries   Ltd  ") == "reliance"


def test_normalize_idempotent():
    name = "Gujarat Positra Steel Pvt Ltd"
    once = normalize_company_name(name)
    twice = normalize_company_name(once)
    assert once == twice
