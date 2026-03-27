from __future__ import annotations

import logging
import re
import string
from dataclasses import dataclass, field
from typing import Optional


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Normalization — TWO levels
# ---------------------------------------------------------------------------
# "Light" matches what load_ogd.py stored in normalized_name column.
# "Aggressive" strips additional business-type words for fuzzy comparison.
# ---------------------------------------------------------------------------

_PUNCT_TABLE = str.maketrans("", "", string.punctuation)

# Must match load_ogd.py exactly — these 6 tokens only
_LIGHT_STRIP = re.compile(r"\b(?:pvt|ltd|private|limited|india|llp)\b")

# Additional tokens stripped for aggressive / core-name matching
_AGGRESSIVE_STRIP = re.compile(
    r"\b(?:pvt|ltd|private|limited|india|llp|opc|corp|corporation|"
    r"technologies|technology|tech|enterprises|enterprise|industries|industry|"
    r"solutions|services|trading|exports|imports|infrastructure|holdings|"
    r"group|associates|consultancy|agency|company|co|one person company)\b"
)


def normalize_light(name: str) -> str:
    """Normalize to match what load_ogd.py stored in DB."""
    if not name:
        return ""
    lowered = name.lower()
    stripped = _LIGHT_STRIP.sub(" ", lowered)
    no_punct = stripped.translate(_PUNCT_TABLE)
    return re.sub(r"\s+", " ", no_punct).strip()


def normalize_aggressive(name: str) -> str:
    """Aggressive normalization — strips all business-type words."""
    if not name:
        return ""
    lowered = name.lower()
    stripped = _AGGRESSIVE_STRIP.sub(" ", lowered)
    no_punct = stripped.translate(_PUNCT_TABLE)
    return re.sub(r"\s+", " ", no_punct).strip()


# ---------------------------------------------------------------------------
# Person detection
# ---------------------------------------------------------------------------
_COMPANY_TOKENS = re.compile(
    r"\b(pvt|private|limited|ltd|llp|opc|corp|corporation|industries|enterprise|"
    r"enterprises|exports|imports|trading|solutions|services|technologies|"
    r"infrastructure|holdings|group|associates|consultancy|agency|m/s|"
    r"bank|finance|financial|insurance|pharma|pharmaceutical|cement|"
    r"steel|power|energy|telecom|motors|automotive|chemicals|textiles|"
    r"realty|builders|construction|developers|logistics|shipping)\b",
    re.IGNORECASE,
)

_MS_PREFIX = re.compile(r"^m/s\.?\s*", re.IGNORECASE)


def _is_likely_person(name: str) -> bool:
    """Return True if name looks like a natural person rather than a company."""
    if _COMPANY_TOKENS.search(name):
        return False
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

        # Strip M/S. prefix that SARFAESI / court records add
        cleaned = _MS_PREFIX.sub("", raw_name).strip()
        normalized = normalize_light(cleaned)
        if not normalized:
            return ResolutionResult(cin=None, confidence=0.0, method="empty")

        core = normalize_aggressive(cleaned)

        try:
            # Stage 0: alias table — trade names / brand names mapped to CIN
            row = self.db.execute(
                """
                SELECT a.cin, m.company_name
                FROM entity_aliases a
                JOIN master_entities m ON m.cin = a.cin
                WHERE a.normalized_alias = %s
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if row:
                return ResolutionResult(
                    cin=row[0],
                    confidence=0.99,
                    method="alias_exact",
                    candidates=[{"cin": row[0], "company_name": row[1]}],
                    resolved=True,
                )

            # Stage 1: exact match on light-normalized name (matches DB column)
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
                    resolved=True,
                )

            # Stage 2: case-insensitive exact match on raw company_name
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
                    confidence=0.85,
                    method="name_ilike",
                    candidates=[{"cin": row[0], "company_name": row[1]}],
                    resolved=True,
                )

            # Stage 3: prefix match — scraped name is often a truncated version
            # e.g. "tata steel" should match "tata steel mining"
            if len(normalized) >= 5:
                row = self.db.execute(
                    """
                    SELECT cin, company_name, normalized_name
                    FROM master_entities
                    WHERE normalized_name LIKE %s || '%%'
                    ORDER BY length(normalized_name) ASC
                    LIMIT 1
                    """,
                    (normalized,),
                ).fetchone()
                if row:
                    return ResolutionResult(
                        cin=row[0],
                        confidence=0.82,
                        method="prefix_match",
                        candidates=[{"cin": row[0], "company_name": row[1]}],
                        resolved=True,
                    )

            # Stage 4: aggressive-normalized exact match via SQL
            # Strips "industries", "technologies", etc. on BOTH sides
            if core and core != normalized:
                rows = self.db.execute(
                    """
                    SELECT cin, company_name, normalized_name
                    FROM master_entities
                    WHERE regexp_replace(
                        normalized_name,
                        '\\m(opc|corp|corporation|technologies|technology|tech|enterprises|enterprise|industries|industry|solutions|services|trading|exports|imports|infrastructure|holdings|group|associates|consultancy|agency|company|co|one person company)\\M',
                        ' ', 'gi'
                    ) ~* ('^\\s*' || regexp_replace(%s, '([\\[\\](){}.*+?^$|\\\\])', '\\\\\\1', 'g') || '\\s*$')
                    LIMIT 3
                    """,
                    (core,),
                ).fetchall()
                if rows:
                    top = rows[0]
                    return ResolutionResult(
                        cin=top[0],
                        confidence=0.78,
                        method="core_name_match",
                        candidates=[{"cin": r[0], "company_name": r[1]} for r in rows],
                        resolved=True,
                    )

            # Stage 5: trigram similarity on light-normalized name (>= 0.45)
            rows = self.db.execute(
                """
                SELECT cin, company_name,
                       similarity(normalized_name, %s) AS sim
                FROM master_entities
                WHERE similarity(normalized_name, %s) > 0.45
                ORDER BY sim DESC
                LIMIT 5
                """,
                (normalized, normalized),
            ).fetchall()
            if rows:
                top = rows[0]
                sim = float(top[2])
                if sim >= 0.75:
                    conf = 0.85
                elif sim >= 0.6:
                    conf = 0.72
                else:
                    conf = 0.55
                return ResolutionResult(
                    cin=top[0],
                    confidence=conf,
                    method=f"trigram_{sim:.2f}",
                    candidates=[
                        {"cin": r[0], "company_name": r[1], "sim": float(r[2])}
                        for r in rows
                    ],
                    resolved=True,
                )

            # Stage 6: trigram on aggressive-normalized name (catches suffix mismatches)
            if core and core != normalized:
                rows = self.db.execute(
                    """
                    SELECT cin, company_name,
                           similarity(normalized_name, %s) AS sim
                    FROM master_entities
                    WHERE similarity(normalized_name, %s) > 0.4
                    ORDER BY sim DESC
                    LIMIT 5
                    """,
                    (core, core),
                ).fetchall()
                if rows:
                    top = rows[0]
                    sim = float(top[2])
                    conf = 0.65 if sim >= 0.6 else 0.45
                    return ResolutionResult(
                        cin=top[0],
                        confidence=conf,
                        method=f"trigram_core_{sim:.2f}",
                        candidates=[
                            {"cin": r[0], "company_name": r[1], "sim": float(r[2])}
                            for r in rows
                        ],
                        resolved=True,
                    )

        except Exception as exc:  # pragma: no cover
            logger.warning("entity resolver lookup failed for %r: %s", raw_name, exc)

        return ResolutionResult(cin=None, confidence=0.0, method="none")
