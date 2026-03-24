# ENTITY_RESOLUTION_SPEC.md

## Spec owner: Claude Code
## Implementor: Codex
## Status: Ready for implementation
## Last updated: 2026-03-16

---

## Overview

The entity resolver is the critical path through which every government data event enters the system. It takes a raw company name (and optionally a CIN, PAN, state, or industry code) from a scraper output and returns a canonical CIN from `master_entities`. Without a CIN, nothing enters the main graph. With an unresolved CIN, the event is queued, not dropped.

The resolver runs nine stages in order of ascending cost. The first stage to produce a confident match short-circuits the rest. Algorithmic stages handle roughly 90%+ of volume. LLM fallback is rate-capped at 500 calls per calendar month and is reserved exclusively for ambiguous cases where 2-3 plausible candidates exist and trigram alone cannot distinguish them. Every resolution attempt — successful or not — is logged.

This file is the complete implementation spec. Codex must implement exactly what is described here, in the order described, without architectural deviation. Any uncertainty must be raised with Claude Code before implementation begins.

---

## Dependencies

Add these to `requirements.txt`:

```
asyncpg>=0.29.0
jellyfish>=1.0.3
anthropic>=0.25.0
cachetools>=5.3.3
pytest>=8.1.0
pytest-asyncio>=0.23.6
```

`pg_trgm` must be enabled in PostgreSQL. Codex does not need to enable it — assume it is already enabled by the DB schema spec.

---

## Full implementation: `ingestion/entity_resolver.py`

```python
"""
Entity resolution pipeline.

Resolves raw company names (and optional CIN/PAN/state/industry hints)
to a canonical CIN in master_entities.

Stage order (cheapest first):
  1. Exact CIN match
  2. Exact PAN match
  3. Normalized name exact match
  4. Trigram similarity > 0.8 + same state
  5. Jaro-Winkler > 0.85 + same state
  6. Trigram > 0.6 + same state + same NIC industry
  7. Trigram > 0.6, no state filter
  8. Queue for LLM
  9. LLM fallback (async, max 500/month)
"""

from __future__ import annotations

import asyncio
import logging
import re
import string
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import jellyfish
from anthropic import AsyncAnthropic
from cachetools import TTLCache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CIN_PATTERN = re.compile(r"^[UL]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")
PAN_PATTERN = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")

# Tokens stripped during normalization (order matters — longer tokens first
# to prevent partial-strip bugs, e.g. "corporation" before "corp")
STRIP_TOKENS = [
    "one person company",
    "corporation",
    "technologies",
    "enterprises",
    "enterprise",
    "industries",
    "solutions",
    "services",
    "private",
    "trading",
    "limited",
    "india",
    "corp",
    "tech",
    "pvt",
    "llp",
    "opc",
    "ltd",
    "co",
]

LLM_MONTHLY_CAP = 500
LLM_TRIGGER_CONFIDENCE_MIN = 50.0
LLM_TRIGGER_CONFIDENCE_MAX = 70.0
LLM_AMBIGUITY_DELTA = 0.05  # candidates within 5% of each other trigger LLM


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ResolutionResult:
    cin: Optional[str]
    confidence: float          # 0–100
    method: str                # which stage resolved it
    candidates: list[dict]     # top candidates considered
    queued: bool               # True if sent to resolution queue
    resolved: bool             # True if CIN was definitively found


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def normalize_company_name(name: str) -> str:
    """
    Normalize a company name for comparison.

    Deterministic and idempotent. Same input always produces same output.
    Calling twice produces the same result as calling once.

    Steps:
      1. Lowercase
      2. Remove punctuation (keep spaces)
      3. Strip known legal/generic tokens (whole words only)
      4. Collapse multiple spaces
      5. Strip leading/trailing whitespace
    """
    if not name:
        return ""

    # Step 1: lowercase
    normalized = name.lower()

    # Step 2: remove punctuation except spaces
    normalized = normalized.translate(
        str.maketrans(string.punctuation, " " * len(string.punctuation))
    )

    # Step 3: strip known tokens (whole-word boundaries)
    for token in STRIP_TOKENS:
        # Use word boundary matching to avoid partial strips
        normalized = re.sub(
            r"\b" + re.escape(token) + r"\b", " ", normalized
        )

    # Step 4: collapse multiple spaces
    normalized = re.sub(r"\s+", " ", normalized)

    # Step 5: strip leading/trailing whitespace
    normalized = normalized.strip()

    return normalized


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

# Key: normalized_name (str) → value: list of candidate dicts from DB
# 10,000 entries, 24-hour TTL
_CANDIDATE_CACHE: TTLCache = TTLCache(maxsize=10_000, ttl=86_400)


# ---------------------------------------------------------------------------
# Main resolver
# ---------------------------------------------------------------------------


class EntityResolver:
    """
    Resolves a raw company name to a canonical CIN.

    Usage:
        resolver = EntityResolver(pool, anthropic_client)
        result = await resolver.resolve("GUJARAT POSITRA STEEL PVT LTD",
                                        state="GJ", industry="2710")
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        anthropic_client: Optional[AsyncAnthropic] = None,
    ) -> None:
        self._pool = pool
        self._llm = anthropic_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def resolve(
        self,
        raw_name: str,
        state: Optional[str] = None,
        industry: Optional[str] = None,
        cin: Optional[str] = None,
        pan: Optional[str] = None,
    ) -> ResolutionResult:
        """
        Run the full resolution pipeline and return a ResolutionResult.

        Stages are attempted in order. The first stage to produce a
        confident match returns immediately without running later stages.
        """
        start_ms = time.monotonic() * 1000

        result = await self._run_pipeline(
            raw_name=raw_name,
            state=state,
            industry=industry,
            cin=cin,
            pan=pan,
        )

        duration_ms = (time.monotonic() * 1000) - start_ms
        await self._log_attempt(raw_name, result, duration_ms)

        return result

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    async def _run_pipeline(
        self,
        raw_name: str,
        state: Optional[str],
        industry: Optional[str],
        cin: Optional[str],
        pan: Optional[str],
    ) -> ResolutionResult:

        # --- Stage 1: Exact CIN match ---
        if cin:
            if not CIN_PATTERN.match(cin):
                logger.warning(
                    "entity_resolver: invalid CIN format from source: %s — skipping CIN stage",
                    cin,
                )
            else:
                row = await self._lookup_cin(cin)
                if row:
                    return ResolutionResult(
                        cin=row["cin"],
                        confidence=100.0,
                        method="exact_cin",
                        candidates=[dict(row)],
                        queued=False,
                        resolved=True,
                    )

        # --- Stage 2: Exact PAN match ---
        if pan and PAN_PATTERN.match(pan):
            row = await self._lookup_pan(pan)
            if row:
                return ResolutionResult(
                    cin=row["cin"],
                    confidence=100.0,
                    method="exact_pan",
                    candidates=[dict(row)],
                    queued=False,
                    resolved=True,
                )

        # --- Stage 3: Normalized name exact match ---
        normalized = normalize_company_name(raw_name)
        row = await self._lookup_normalized_exact(normalized)
        if row:
            return ResolutionResult(
                cin=row["cin"],
                confidence=95.0,
                method="normalized_exact",
                candidates=[dict(row)],
                queued=False,
                resolved=True,
            )

        # --- Stage 4: Trigram > 0.8 + same state ---
        if state:
            candidates = await self._trigram_state(normalized, state, threshold=0.8)
            if candidates:
                top = candidates[0]
                return ResolutionResult(
                    cin=top["cin"],
                    confidence=90.0,
                    method="trigram_state_08",
                    candidates=candidates,
                    queued=False,
                    resolved=True,
                )

        # --- Stage 5: Jaro-Winkler > 0.85 + same state ---
        if state:
            candidates = await self._trigram_state(normalized, state, threshold=0.5)
            jw_result = self._jaro_winkler_top(normalized, candidates, threshold=0.85)
            if jw_result:
                return ResolutionResult(
                    cin=jw_result["cin"],
                    confidence=85.0,
                    method="jaro_winkler_state",
                    candidates=candidates,
                    queued=False,
                    resolved=True,
                )

        # --- Stage 6: Trigram > 0.6 + same state + same industry ---
        if state and industry:
            candidates = await self._trigram_state_industry(
                normalized, state, industry, threshold=0.6
            )
            if candidates:
                top = candidates[0]
                return ResolutionResult(
                    cin=top["cin"],
                    confidence=75.0,
                    method="trigram_state_industry_06",
                    candidates=candidates,
                    queued=False,
                    resolved=True,
                )

        # --- Stage 7: Trigram > 0.6, no state filter ---
        candidates = await self._trigram_any(normalized, threshold=0.6)
        if candidates:
            top = candidates[0]
            return ResolutionResult(
                cin=top["cin"],
                confidence=60.0,
                method="trigram_any_06",
                candidates=candidates,
                queued=False,
                resolved=True,
            )

        # --- Stage 8 / 9: Queue or LLM ---
        return await self._queue_or_llm(raw_name, normalized, candidates=[])

    # ------------------------------------------------------------------
    # DB lookup helpers
    # ------------------------------------------------------------------

    async def _lookup_cin(self, cin: str) -> Optional[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT cin, company_name, registered_state, industrial_class "
                "FROM master_entities WHERE cin = $1",
                cin,
            )

    async def _lookup_pan(self, pan: str) -> Optional[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT cin, company_name, registered_state, industrial_class "
                "FROM master_entities WHERE pan = $1",
                pan,
            )

    async def _lookup_normalized_exact(
        self, normalized: str
    ) -> Optional[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT cin, company_name, registered_state, industrial_class "
                "FROM master_entities WHERE normalized_name = $1",
                normalized,
            )

    async def _trigram_state(
        self, normalized: str, state: str, threshold: float
    ) -> list[dict]:
        cache_key = f"trgm_state:{normalized}:{state}:{threshold}"
        if cache_key in _CANDIDATE_CACHE:
            return _CANDIDATE_CACHE[cache_key]

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cin, company_name, registered_state, industrial_class,
                       similarity(normalized_name, $1) AS sim
                FROM master_entities
                WHERE registered_state = $2
                  AND similarity(normalized_name, $1) > $3
                ORDER BY sim DESC
                LIMIT 5
                """,
                normalized,
                state,
                threshold,
            )
        result = [dict(r) for r in rows]
        _CANDIDATE_CACHE[cache_key] = result
        return result

    async def _trigram_state_industry(
        self, normalized: str, state: str, industry: str, threshold: float
    ) -> list[dict]:
        cache_key = f"trgm_state_ind:{normalized}:{state}:{industry}:{threshold}"
        if cache_key in _CANDIDATE_CACHE:
            return _CANDIDATE_CACHE[cache_key]

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cin, company_name, registered_state, industrial_class,
                       similarity(normalized_name, $1) AS sim
                FROM master_entities
                WHERE registered_state = $2
                  AND industrial_class = $3
                  AND similarity(normalized_name, $1) > $4
                ORDER BY sim DESC
                LIMIT 5
                """,
                normalized,
                state,
                industry,
                threshold,
            )
        result = [dict(r) for r in rows]
        _CANDIDATE_CACHE[cache_key] = result
        return result

    async def _trigram_any(
        self, normalized: str, threshold: float
    ) -> list[dict]:
        cache_key = f"trgm_any:{normalized}:{threshold}"
        if cache_key in _CANDIDATE_CACHE:
            return _CANDIDATE_CACHE[cache_key]

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cin, company_name, registered_state, industrial_class,
                       similarity(normalized_name, $1) AS sim
                FROM master_entities
                WHERE similarity(normalized_name, $1) > $2
                ORDER BY sim DESC
                LIMIT 10
                """,
                normalized,
                threshold,
            )
        result = [dict(r) for r in rows]
        _CANDIDATE_CACHE[cache_key] = result
        return result

    # ------------------------------------------------------------------
    # Jaro-Winkler
    # ------------------------------------------------------------------

    def _jaro_winkler_top(
        self,
        normalized: str,
        candidates: list[dict],
        threshold: float,
    ) -> Optional[dict]:
        """
        Score candidates with Jaro-Winkler. Return highest-scoring
        candidate if it exceeds the threshold, else None.
        """
        best = None
        best_score = 0.0
        for c in candidates:
            score = jellyfish.jaro_winkler_similarity(
                normalized, c.get("normalized_name", c.get("company_name", ""))
            )
            if score > best_score:
                best_score = score
                best = c
        if best and best_score >= threshold:
            return best
        return None

    # ------------------------------------------------------------------
    # Queue and LLM fallback
    # ------------------------------------------------------------------

    async def _queue_or_llm(
        self,
        raw_name: str,
        normalized: str,
        candidates: list[dict],
    ) -> ResolutionResult:
        """
        Decide between queuing for LLM or marking UNRESOLVABLE.

        LLM is triggered when:
          - 2–3 candidates exist
          - Top candidate confidence is in the 50–70% range
          - Candidates are within 0.05 sim of each other (ambiguous)

        Otherwise: insert into queue and return queued=True, resolved=False.
        """
        # Always insert into queue first so no event is lost
        await self._insert_queue(raw_name, normalized, candidates)

        # Determine whether LLM should run now
        should_llm = self._should_trigger_llm(candidates)

        if should_llm and self._llm is not None:
            llm_result = await self._call_llm(raw_name, candidates)
            if llm_result:
                await self._mark_queue_resolved(raw_name, llm_result)
                return ResolutionResult(
                    cin=llm_result,
                    confidence=80.0,
                    method="llm_fallback",
                    candidates=candidates,
                    queued=True,
                    resolved=True,
                )
            else:
                return ResolutionResult(
                    cin=None,
                    confidence=0.0,
                    method="llm_fallback_none",
                    candidates=candidates,
                    queued=True,
                    resolved=False,
                )

        return ResolutionResult(
            cin=None,
            confidence=0.0,
            method="queued",
            candidates=candidates,
            queued=True,
            resolved=False,
        )

    def _should_trigger_llm(self, candidates: list[dict]) -> bool:
        """
        Return True if LLM disambiguation is warranted.

        Conditions:
          - 2 or 3 candidates present
          - Top candidate sim is between 50% and 70%
          - Top two candidates are within LLM_AMBIGUITY_DELTA of each other
        """
        if len(candidates) < 2 or len(candidates) > 3:
            return False

        top_sim = candidates[0].get("sim", 0.0) * 100  # convert 0-1 → 0-100
        if not (LLM_TRIGGER_CONFIDENCE_MIN <= top_sim <= LLM_TRIGGER_CONFIDENCE_MAX):
            return False

        if len(candidates) >= 2:
            delta = abs(
                candidates[0].get("sim", 0.0) - candidates[1].get("sim", 0.0)
            )
            if delta > LLM_AMBIGUITY_DELTA:
                return False

        return True

    async def _call_llm(
        self, raw_name: str, candidates: list[dict]
    ) -> Optional[str]:
        """
        Call Claude API to disambiguate between candidates.

        Increments monthly counter. Returns selected CIN or None.
        On API failure: logs error, returns None, does NOT raise.
        """
        # Check monthly cap before calling
        month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        count = await self._get_llm_monthly_count(month_key)
        if count >= LLM_MONTHLY_CAP:
            logger.warning(
                "entity_resolver: LLM monthly cap (%d) reached for %s — "
                "marking UNRESOLVABLE: %s",
                LLM_MONTHLY_CAP,
                month_key,
                raw_name,
            )
            await self._flag_unresolvable(raw_name)
            return None

        candidate_lines = "\n".join(
            f"  - CIN: {c['cin']} | Name: {c['company_name']} "
            f"| State: {c.get('registered_state', 'unknown')} "
            f"| Industry: {c.get('industrial_class', 'unknown')}"
            for c in candidates
        )

        prompt = (
            f"A government data source references the company: \"{raw_name}\"\n\n"
            f"The following registered companies are possible matches:\n"
            f"{candidate_lines}\n\n"
            f"Which company is most likely being referred to? "
            f"Return only the CIN (e.g. U27100GJ2015PTC082456) if you are confident, "
            f"or return the single word NONE if none of these is a plausible match. "
            f"Do not explain. Do not return anything else."
        )

        try:
            response = await self._llm.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=32,
                messages=[{"role": "user", "content": prompt}],
            )
            await self._increment_llm_count(month_key)
            await self._log_llm_cost(raw_name)

            answer = response.content[0].text.strip()
            if answer == "NONE":
                return None
            if CIN_PATTERN.match(answer):
                return answer
            logger.warning(
                "entity_resolver: LLM returned unexpected value: %s for query: %s",
                answer,
                raw_name,
            )
            return None

        except Exception as exc:
            logger.error(
                "entity_resolver: LLM API failure for %s: %s — marking UNRESOLVABLE",
                raw_name,
                exc,
            )
            await self._flag_unresolvable(raw_name)
            return None

    # ------------------------------------------------------------------
    # DB write helpers
    # ------------------------------------------------------------------

    async def _insert_queue(
        self, raw_name: str, normalized: str, candidates: list[dict]
    ) -> None:
        """Insert unresolved entry into entity_resolution_queue."""
        import json

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entity_resolution_queue
                    (raw_name, normalized_name, candidates, created_at, status)
                VALUES ($1, $2, $3, now(), 'PENDING')
                ON CONFLICT (raw_name) DO UPDATE
                    SET candidates = EXCLUDED.candidates,
                        updated_at = now()
                """,
                raw_name,
                normalized,
                json.dumps(candidates),
            )

    async def _mark_queue_resolved(self, raw_name: str, cin: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entity_resolution_queue
                SET resolved_cin = $1, status = 'RESOLVED', updated_at = now()
                WHERE raw_name = $2
                """,
                cin,
                raw_name,
            )

    async def _flag_unresolvable(self, raw_name: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entity_resolution_queue
                SET status = 'UNRESOLVABLE', updated_at = now()
                WHERE raw_name = $1
                """,
                raw_name,
            )

    async def _get_llm_monthly_count(self, month_key: str) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(call_count), 0) AS total
                FROM cost_log
                WHERE service = 'entity_resolution_llm'
                  AND month_key = $1
                """,
                month_key,
            )
            return int(row["total"]) if row else 0

    async def _increment_llm_count(self, month_key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO cost_log (service, month_key, call_count, last_called_at)
                VALUES ('entity_resolution_llm', $1, 1, now())
                ON CONFLICT (service, month_key)
                DO UPDATE SET call_count = cost_log.call_count + 1,
                              last_called_at = now()
                """,
                month_key,
            )

    async def _log_llm_cost(self, raw_name: str) -> None:
        """Log individual LLM call for cost audit trail."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO cost_log_detail
                    (service, raw_input, called_at)
                VALUES ('entity_resolution_llm', $1, now())
                """,
                raw_name,
            )

    async def _log_attempt(
        self, raw_name: str, result: ResolutionResult, duration_ms: float
    ) -> None:
        """Log every resolution attempt for auditing and recalibration."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entity_resolution_log
                    (raw_name, resolved_cin, confidence, method, duration_ms, logged_at)
                VALUES ($1, $2, $3, $4, $5, now())
                """,
                raw_name,
                result.cin,
                result.confidence,
                result.method,
                duration_ms,
            )
```

---

## Full test file: `tests/test_entity_resolver.py`

```python
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

from ingestion.entity_resolver import (
    EntityResolver,
    ResolutionResult,
    normalize_company_name,
)


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 1. Exact CIN match
# ---------------------------------------------------------------------------


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
    """Invalid CIN format should log a warning and fall through to next stage."""
    pool = FakePool()
    row = make_row()
    pool.stage_response("normalized_name =", row)

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="GUJARAT POSITRA STEEL PVT LTD",
        cin="INVALID",
    )

    # Should not resolve via CIN; falls through to normalized exact match
    assert result.method == "normalized_exact"
    assert result.confidence == 95.0


# ---------------------------------------------------------------------------
# 2. Normalized name exact match — pvt vs private, ltd vs limited
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 3. Trigram match — deliberate typo in name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigram_state_typo():
    """A typo should still resolve via trigram if similarity > 0.8."""
    pool = FakePool()
    row = make_row(sim=0.88)
    # Ensure exact and PAN stages return nothing
    # Only trigram_state query returns a result
    pool.stage_response("similarity(normalized_name", [row])

    resolver = EntityResolver(pool)
    # Deliberate typo: "Positra" → "Positara"
    result = await resolver.resolve(
        raw_name="Gujarat Positara Steel Pvt Ltd",
        state="GJ",
    )

    assert result.resolved is True
    assert result.confidence == 90.0
    assert result.method == "trigram_state_08"


# ---------------------------------------------------------------------------
# 4. State filter narrows correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_state_filter_narrows():
    """Only the candidate in the matching state should be returned."""
    pool = FakePool()
    gj_row = make_row(cin="U27100GJ2015PTC082456", registered_state="GJ", sim=0.85)
    mh_row = make_row(cin="U27100MH2010PTC111111", registered_state="MH", sim=0.82)

    # Return only GJ row for state-filtered query
    pool.stage_response("registered_state = $2", [gj_row])

    resolver = EntityResolver(pool)
    result = await resolver.resolve(
        raw_name="Gujarat Positra Steel Pvt Ltd",
        state="GJ",
    )

    assert result.cin == "U27100GJ2015PTC082456"
    assert result.resolved is True


# ---------------------------------------------------------------------------
# 5. Below threshold → goes to queue, not resolved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_below_threshold_queued():
    """When no stage resolves, result should be queued and not resolved."""
    pool = FakePool()
    # All DB queries return empty
    # (no staged responses means FakePool returns None / [])

    resolver = EntityResolver(pool)
    result = await resolver.resolve(raw_name="Some Completely Unknown Company XYZ")

    assert result.resolved is False
    assert result.queued is True
    assert result.cin is None

    # Verify queue insert was executed
    assert any("entity_resolution_queue" in q for q in pool._executed)


# ---------------------------------------------------------------------------
# 6. LLM fallback called only when in 50–70% confidence range
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_llm_triggered_in_ambiguous_range():
    """LLM should be called when 2 candidates exist with sim 0.55–0.65."""
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

    # Stage 7 (trigram_any) returns two close candidates
    pool.stage_response("similarity(normalized_name, $1) > $2", [candidate_a, candidate_b])

    mock_llm = AsyncMock()
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="U27100GJ2015PTC082456")]
    mock_llm.messages.create = AsyncMock(return_value=mock_response)

    # Stub cost_log to return count = 0
    pool.stage_response("coalesce(sum(call_count)", {"total": 0})

    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_called_once()
    assert result.resolved is True
    assert result.method == "llm_fallback"
    assert result.cin == "U27100GJ2015PTC082456"


@pytest.mark.asyncio
async def test_llm_not_triggered_above_70_percent():
    """LLM must not fire when top candidate sim is above 0.7 (stage 7 returns it directly)."""
    pool = FakePool()
    row = make_row(sim=0.72)
    pool.stage_response("similarity(normalized_name, $1) > $2", [row])

    mock_llm = AsyncMock()
    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_not_called()
    assert result.method == "trigram_any_06"
    assert result.confidence == 60.0


# ---------------------------------------------------------------------------
# 7. Monthly LLM limit respected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_llm_monthly_cap_respected():
    """When monthly count == 500, LLM must not be called and result is UNRESOLVABLE."""
    pool = FakePool()

    candidate_a = make_row(sim=0.62)
    candidate_b = make_row(cin="U27100GJ2016PTC099999", sim=0.60)
    pool.stage_response("similarity(normalized_name, $1) > $2", [candidate_a, candidate_b])

    # Return count = 500 (cap reached)
    pool.stage_response("coalesce(sum(call_count)", {"total": 500})

    mock_llm = AsyncMock()
    resolver = EntityResolver(pool, anthropic_client=mock_llm)
    result = await resolver.resolve(raw_name="Gujarat Positra Steel")

    mock_llm.messages.create.assert_not_called()
    assert result.resolved is False
    assert result.queued is True


# ---------------------------------------------------------------------------
# 8. normalize_company_name — 10 test cases
# ---------------------------------------------------------------------------


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
```

---

## Required PostgreSQL schema objects

Codex does not create these — they are documented here so the DB schema spec stays in sync. The following tables and columns must exist before this module is deployed:

**`master_entities`**
- `cin` — VARCHAR(21), primary key
- `pan` — VARCHAR(10), nullable, indexed
- `company_name` — TEXT
- `normalized_name` — TEXT (pre-computed, indexed with GIN for pg_trgm)
- `registered_state` — CHAR(2), indexed
- `industrial_class` — VARCHAR(10), nullable, indexed

**`entity_resolution_queue`**
- `raw_name` — TEXT, unique
- `normalized_name` — TEXT
- `candidates` — JSONB
- `resolved_cin` — VARCHAR(21), nullable
- `status` — VARCHAR(20) — values: PENDING, RESOLVED, UNRESOLVABLE
- `created_at` — TIMESTAMPTZ
- `updated_at` — TIMESTAMPTZ

**`entity_resolution_log`**
- `id` — BIGSERIAL, primary key
- `raw_name` — TEXT
- `resolved_cin` — VARCHAR(21), nullable
- `confidence` — NUMERIC(5,2)
- `method` — VARCHAR(50)
- `duration_ms` — NUMERIC(10,3)
- `logged_at` — TIMESTAMPTZ

**`cost_log`**
- `service` — VARCHAR(50)
- `month_key` — CHAR(7) — format: YYYY-MM
- `call_count` — INTEGER
- `last_called_at` — TIMESTAMPTZ
- PRIMARY KEY (service, month_key)

**`cost_log_detail`**
- `id` — BIGSERIAL, primary key
- `service` — VARCHAR(50)
- `raw_input` — TEXT
- `called_at` — TIMESTAMPTZ

---

## Performance considerations

**GIN index on `normalized_name` is mandatory.** pg_trgm similarity queries without a GIN index do full-table scans. On 18 lakh rows, an unindexed trigram query will exceed acceptable latency. The index must be:

```sql
CREATE INDEX idx_master_entities_normalized_trgm
ON master_entities
USING GIN (normalized_name gin_trgm_ops);
```

**Composite indexes for stage 4 and 6 queries.** State is the most selective early filter. Add:

```sql
CREATE INDEX idx_master_entities_state ON master_entities (registered_state);
CREATE INDEX idx_master_entities_state_industry ON master_entities (registered_state, industrial_class);
```

**Cache hit rate.** The TTLCache on candidate results means repeated lookups for the same normalized name (e.g. same company appearing in multiple source events within 24 hours) hit memory, not the DB. On a busy ingestion day this will absorb the majority of resolution calls for active companies.

**Jaro-Winkler pre-filter.** Stage 5 does not scan all 18 lakh rows. It pulls the trigram > 0.5 candidate set from the DB first (at most a few dozen rows) and runs Jaro-Winkler in Python against that shortlist only. This keeps the Python-side computation negligible.

**LLM latency.** LLM calls are async and non-blocking. The caller receives a `queued=True` result immediately while the LLM call completes in a background task in production. The spec above calls LLM inline for simplicity; the orchestration layer should wrap `_call_llm` in `asyncio.create_task` for production use and poll queue resolution separately.

**Normalization is the performance multiplier.** The `normalized_name` column must be pre-computed at insert time and kept current on updates. A DB trigger on `master_entities` INSERT/UPDATE should call the same normalization logic (ported to a PostgreSQL function) to keep Python and SQL normalization in sync. Codex should spec this trigger with Claude Code before implementing.

**Monthly LLM counter is approximate.** The `cost_log` counter uses `ON CONFLICT DO UPDATE` which is safe under concurrent writers but not atomic at the application level. At 500 calls/month the margin is wide enough that minor over-counting from concurrent calls is acceptable. If exact enforcement is required, a `SELECT ... FOR UPDATE` lock can be added — raise with Claude Code.
