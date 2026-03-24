from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

try:
    from bi_engine.ingestion.entity_resolver import normalize_company_name
except ImportError:  # pragma: no cover
    def normalize_company_name(name: str) -> str:
        return " ".join((name or "").lower().split())


logger = logging.getLogger(__name__)

# Tokens that indicate a name is a registered company, not a natural person
_COMPANY_TOKENS = re.compile(
    r"\b(pvt|private|limited|ltd|llp|opc|corp|corporation|industries|enterprise|"
    r"enterprises|exports|imports|trading|solutions|services|technologies|"
    r"infrastructure|holdings|group|associates|consultancy|agency|m/s)\b",
    re.IGNORECASE,
)

# Strip M/S. prefix before normalizing
_MS_PREFIX = re.compile(r"^m/s\.?\s*", re.IGNORECASE)


def _is_likely_person(name: str) -> bool:
    """Return True if name looks like a natural person rather than a company."""
    if _COMPANY_TOKENS.search(name):
        return False
    # Persons: 2-4 all-alpha words, no digits, no punctuation beyond spaces
    clean = name.strip()
    if re.search(r"\d", clean):
        return False
    words = clean.split()
    if not (1 < len(words) <= 5):
        return False
    if all(re.match(r"^[A-Za-z]+$", w) for w in words):
        return True
    return False


@dataclass
class ResolutionResult:
    cin: Optional[str]
    confidence: float
    method: str
    candidates: list[dict] = field(default_factory=list)
    queued: bool = False
    resolved: bool = False


class EntityResolver:
    """Synchronous adapter for scraper ingestion paths."""

    def __init__(self, db_conn):
        self.db = db_conn

    def resolve(self, raw_name: str) -> ResolutionResult:
        raw_name = (raw_name or "").strip()

        # Skip natural persons — they have no CIN
        if _is_likely_person(raw_name):
            return ResolutionResult(cin=None, confidence=0.0, method="person_skip")

        # Strip M/S. prefix that SARFAESI adds
        cleaned = _MS_PREFIX.sub("", raw_name).strip()
        normalized = normalize_company_name(cleaned)
        if not normalized:
            return ResolutionResult(cin=None, confidence=0.0, method="empty")

        try:
            # Stage 1: exact normalized match
            row = self.db.execute(
                """
                SELECT cin, company_name
                FROM master_entities
                WHERE normalized_name = %s
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if row:
                return ResolutionResult(
                    cin=row[0],
                    confidence=0.95,
                    method="normalized_exact",
                    candidates=[{"cin": row[0], "company_name": row[1]}],
                    queued=False,
                    resolved=True,
                )

            # Stage 2: case-insensitive exact match on raw name
            row = self.db.execute(
                """
                SELECT cin, company_name
                FROM master_entities
                WHERE company_name ILIKE %s
                LIMIT 1
                """,
                (cleaned,),
            ).fetchone()
            if row:
                return ResolutionResult(
                    cin=row[0],
                    confidence=0.80,
                    method="name_exact",
                    candidates=[{"cin": row[0], "company_name": row[1]}],
                    queued=False,
                    resolved=True,
                )

            # Stage 3: trigram similarity >= 0.6 (requires pg_trgm)
            rows = self.db.execute(
                """
                SELECT cin, company_name,
                       similarity(normalized_name, %s) AS sim
                FROM master_entities
                WHERE similarity(normalized_name, %s) > 0.6
                ORDER BY sim DESC
                LIMIT 5
                """,
                (normalized, normalized),
            ).fetchall()
            if rows:
                top = rows[0]
                sim = float(top[2])
                # High confidence (>=0.75) → direct upsert; lower → queue
                conf = 0.80 if sim >= 0.75 else 0.60
                return ResolutionResult(
                    cin=top[0],
                    confidence=conf,
                    method=f"trigram_{sim:.2f}",
                    candidates=[{"cin": r[0], "company_name": r[1], "sim": float(r[2])} for r in rows],
                    queued=False,
                    resolved=True,
                )

        except Exception as exc:  # pragma: no cover
            logger.warning("entity resolver lookup failed for %r: %s", raw_name, exc)

        return ResolutionResult(cin=None, confidence=0.0, method="none")

