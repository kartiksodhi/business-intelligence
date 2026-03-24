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

import json
import logging
import re
import string
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
from pydantic import BaseModel, ValidationError

try:
    import jellyfish
except ImportError:  # pragma: no cover
    class _JellyfishFallback:
        @staticmethod
        def jaro_winkler_similarity(left: str, right: str) -> float:
            return _jaro_winkler_similarity(left, right)

    jellyfish = _JellyfishFallback()

try:
    from anthropic import AsyncAnthropic
except ImportError:  # pragma: no cover
    AsyncAnthropic = Any  # type: ignore[assignment]

try:
    from cachetools import TTLCache
except ImportError:  # pragma: no cover
    class TTLCache(dict):
        def __init__(self, maxsize: int, ttl: int) -> None:
            super().__init__()
            self.maxsize = maxsize
            self.ttl = ttl
            self._expires: dict[Any, float] = {}

        def __contains__(self, key: object) -> bool:
            if key not in self._expires:
                return False
            if self._expires[key] <= time.time():
                super().pop(key, None)
                self._expires.pop(key, None)
                return False
            return dict.__contains__(self, key)

        def __getitem__(self, key: Any) -> Any:
            if key not in self:
                raise KeyError(key)
            return dict.__getitem__(self, key)

        def __setitem__(self, key: Any, value: Any) -> None:
            if key not in self and len(self) >= self.maxsize:
                oldest_key = next(iter(self))
                dict.__delitem__(self, oldest_key)
                self._expires.pop(oldest_key, None)
            dict.__setitem__(self, key, value)
            self._expires[key] = time.time() + self.ttl


logger = logging.getLogger(__name__)


CIN_PATTERN = re.compile(r"^[UL]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")
PAN_PATTERN = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")
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
LLM_AMBIGUITY_DELTA = 0.05


@dataclass
class ResolutionResult:
    cin: Optional[str]
    confidence: float
    method: str
    candidates: list[dict]
    queued: bool
    resolved: bool


class EntityResolutionResponse(BaseModel):
    matched_cin: Optional[str]
    confidence_score: int
    reasoning_flag: str


def normalize_company_name(name: str) -> str:
    if not name:
        return ""

    normalized = name.lower()
    normalized = normalized.translate(str.maketrans("", "", string.punctuation))

    for token in STRIP_TOKENS:
        normalized = re.sub(r"\b" + re.escape(token) + r"\b", " ", normalized)

    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _jaro_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0

    max_dist = max(len(left), len(right)) // 2 - 1
    left_matches = [False] * len(left)
    right_matches = [False] * len(right)

    matches = 0
    for i, left_char in enumerate(left):
        start = max(0, i - max_dist)
        end = min(i + max_dist + 1, len(right))
        for j in range(start, end):
            if right_matches[j] or left_char != right[j]:
                continue
            left_matches[i] = True
            right_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    transpositions = 0
    right_index = 0
    for i, matched in enumerate(left_matches):
        if not matched:
            continue
        while not right_matches[right_index]:
            right_index += 1
        if left[i] != right[right_index]:
            transpositions += 1
        right_index += 1

    transpositions /= 2
    return (
        (matches / len(left))
        + (matches / len(right))
        + ((matches - transpositions) / matches)
    ) / 3.0


def _common_prefix_length(left: str, right: str, limit: int = 4) -> int:
    prefix = 0
    for left_char, right_char in zip(left[:limit], right[:limit]):
        if left_char != right_char:
            break
        prefix += 1
    return prefix


def _jaro_winkler_similarity(left: str, right: str) -> float:
    jaro = _jaro_similarity(left, right)
    prefix = _common_prefix_length(left, right)
    return jaro + (prefix * 0.1 * (1 - jaro))


_CANDIDATE_CACHE: TTLCache = TTLCache(maxsize=10_000, ttl=86_400)


class EntityResolver:
    def __init__(
        self,
        pool: asyncpg.Pool,
        anthropic_client: Optional[AsyncAnthropic] = None,
    ) -> None:
        self._pool = pool
        self._llm = anthropic_client

    async def resolve(
        self,
        raw_name: str,
        state: Optional[str] = None,
        industry: Optional[str] = None,
        cin: Optional[str] = None,
        pan: Optional[str] = None,
    ) -> ResolutionResult:
        return await self._run_pipeline(
            raw_name=raw_name,
            state=state,
            industry=industry,
            cin=cin,
            pan=pan,
        )

    async def _run_pipeline(
        self,
        raw_name: str,
        state: Optional[str],
        industry: Optional[str],
        cin: Optional[str],
        pan: Optional[str],
    ) -> ResolutionResult:
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

        candidates = await self._trigram_any(normalized, threshold=0.6)
        if candidates and not self._should_trigger_llm(candidates):
            top = candidates[0]
            return ResolutionResult(
                cin=top["cin"],
                confidence=60.0,
                method="trigram_any_06",
                candidates=candidates,
                queued=False,
                resolved=True,
            )

        return await self._queue_or_llm(raw_name, normalized, candidates=candidates)

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

    async def _lookup_normalized_exact(self, normalized: str) -> Optional[asyncpg.Record]:
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
        result = [dict(row) for row in rows]
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
        result = [dict(row) for row in rows]
        _CANDIDATE_CACHE[cache_key] = result
        return result

    async def _trigram_any(self, normalized: str, threshold: float) -> list[dict]:
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
        result = [dict(row) for row in rows]
        _CANDIDATE_CACHE[cache_key] = result
        return result

    def _jaro_winkler_top(
        self,
        normalized: str,
        candidates: list[dict],
        threshold: float,
    ) -> Optional[dict]:
        best = None
        best_score = 0.0
        for candidate in candidates:
            score = jellyfish.jaro_winkler_similarity(
                normalized,
                candidate.get("normalized_name", candidate.get("company_name", "")),
            )
            if score > best_score:
                best_score = score
                best = candidate
        if best and best_score >= threshold:
            return best
        return None

    async def _queue_or_llm(
        self,
        raw_name: str,
        normalized: str,
        candidates: list[dict],
    ) -> ResolutionResult:
        await self._insert_queue(raw_name, normalized, candidates)
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
        if len(candidates) < 2 or len(candidates) > 3:
            return False

        top_sim = candidates[0].get("sim", 0.0) * 100
        if not (LLM_TRIGGER_CONFIDENCE_MIN <= top_sim <= LLM_TRIGGER_CONFIDENCE_MAX):
            return False

        delta = abs(candidates[0].get("sim", 0.0) - candidates[1].get("sim", 0.0))
        return delta <= LLM_AMBIGUITY_DELTA

    async def _call_llm(self, raw_name: str, candidates: list[dict]) -> Optional[str]:
        month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        count = await self._get_llm_monthly_count(month_key)
        if count >= LLM_MONTHLY_CAP:
            logger.warning(
                "entity_resolver: LLM monthly cap (%d) reached for %s — marking UNRESOLVABLE: %s",
                LLM_MONTHLY_CAP,
                month_key,
                raw_name,
            )
            await self._flag_unresolvable(raw_name)
            return None

        candidate_lines = "\n".join(
            f"  - CIN: {candidate['cin']} | Name: {candidate['company_name']} "
            f"| State: {candidate.get('registered_state', 'unknown')} "
            f"| Industry: {candidate.get('industrial_class', 'unknown')}"
            for candidate in candidates
        )
        SYSTEM_PROMPT = (
            "You are a deterministic database routing function. You do not converse. "
            "Compare the provided unstructured 'Target Name' against the 'Candidate Array' "
            "of legal CINs and Names. "
            'Output your response strictly as a JSON object matching this exact schema: '
            '{"matched_cin": "<CIN_STRING_OR_NULL>", "confidence_score": <INTEGER_1_100>, '
            '"reasoning_flag": "<SHORT_STRING>"}. '
            "If no logical match exists, return null for matched_cin. "
            "Do not output markdown or backticks."
        )
        prompt = (
            f'A government data source references the company: "{raw_name}"\n\n'
            f"The following registered companies are possible matches:\n"
            f"{candidate_lines}"
        )

        try:
            response = await self._llm.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=128,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_response = response.content[0].text.strip()
            try:
                result = EntityResolutionResponse.model_validate_json(raw_response)
            except ValidationError:
                response = await self._llm.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=128,
                    temperature=0,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_response = response.content[0].text.strip()
                try:
                    result = EntityResolutionResponse.model_validate_json(raw_response)
                except ValidationError:
                    logger.error(
                        "entity_resolver: malformed LLM response for %s: %s",
                        raw_name,
                        raw_response,
                    )
                    await self._flag_unresolvable(raw_name)
                    return None

            await self._increment_llm_count(month_key)
            return result.matched_cin
        except Exception as exc:  # pragma: no cover
            logger.error(
                "entity_resolver: LLM API failure for %s: %s — marking UNRESOLVABLE",
                raw_name,
                exc,
            )
            await self._flag_unresolvable(raw_name)
            return None

    async def _insert_queue(
        self, raw_name: str, normalized: str, candidates: list[dict]
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entity_resolution_queue
                    (source, raw_name, candidates, best_confidence, resolution_method, created_at)
                VALUES ('resolver', $1, $2, $3, 'queued', now())
                """,
                raw_name,
                json.dumps(candidates),
                float(candidates[0].get("sim", 0.0) * 100) if candidates else 0.0,
            )

    async def _mark_queue_resolved(self, raw_name: str, cin: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entity_resolution_queue
                SET resolved_cin = $1, resolved = TRUE, llm_used = TRUE, resolved_at = now()
                WHERE raw_name = $2
                """,
                cin,
                raw_name,
            )

    async def _flag_unresolvable(self, raw_name: str) -> None:
        # UNRESOLVABLE items stay resolved=FALSE with no resolved_cin.
        # Operator sees them via /op/resolve.
        # No DB update needed — they stay in queue.
        return None

    async def _get_llm_monthly_count(self, month_key: str) -> int:
        first_of_month = month_key + "-01"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(units), 0) AS total
                FROM cost_log
                WHERE service = 'claude_api'
                  AND operation = 'entity_resolution'
                  AND log_date >= $1::date
                  AND log_date < ($1::date + INTERVAL '1 month')
                """,
                first_of_month,
            )
            return int(row["total"]) if row else 0

    async def _increment_llm_count(self, month_key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO cost_log (log_date, service, operation, units, cost_inr)
                VALUES (CURRENT_DATE, 'claude_api', 'entity_resolution', 1, 0.03)
                """,
            )
