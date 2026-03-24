# DIFF_ENGINE_SPEC.md

## What this is

The detection layer of the signal intelligence system. Every source lands new data into the pipeline. This engine compares that data against the last known state stored in `source_state`. If nothing changed, zero cost, zero events. If something changed, it extracts deltas, resolves CINs, and fires structured events into the `events` table.

Core principle: **diff not reprocess**. Hash unchanged = nothing fires.

---

## Tables involved (already exist — do NOT recreate schema)

- `source_state` — tracks `last_pull_at`, `last_data_hash`, `status`, `consecutive_failures`, `record_count` per `source_id`
- `events` — detected changes land here: `cin`, `source`, `event_type`, `severity`, `detected_at`, `data_json`
- `master_entities` — CIN reference table
- `legal_events` — denormalized legal case tracking (used for dedup in NCLT, DRT, SARFAESI, e-Courts)
- `governance_graph` — director-company edges (used in director detector)

---

## File layout

```
detection/
    diff_engine.py
    detectors/
        __init__.py
        base.py
        ogd.py
        nclt.py
        drt.py
        sarfaesi.py
        ecourts.py
        directors.py
tests/
    test_diff_engine.py
```

---

## File: `detection/detectors/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EventSpec:
    """
    Internal representation of a detected event, before DB insert.
    cin is None only for operator-level alerts (e.g. SOURCE_UNREACHABLE).
    health_score_before is populated by the health scorer after insert — not here.
    """
    cin: Optional[str]
    event_type: str
    severity: str          # INFO | WATCH | ALERT | CRITICAL
    data: dict
    source: str = ""       # set by DiffEngine before insert


class BaseDetector(ABC):
    """
    All source detectors inherit from this.
    detect_events receives the old and new record lists for the source.
    For append-only sources (NCLT, DRT, SARFAESI, e-Courts), old_records is the
    set of case identifiers already stored in legal_events — the detector treats
    any record not in that set as new.
    """

    @abstractmethod
    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,                    # asyncpg connection or pool passed through
    ) -> List[EventSpec]:
        """
        Compare old vs new records, return list of EventSpec to fire.
        Must be idempotent: running twice on the same input must not produce
        duplicate EventSpec entries.
        """
        ...
```

---

## File: `detection/detectors/ogd.py`

```python
"""
OGD detector — source_id: 'mca_ogd'

Compares old vs new master entity snapshots.
Records are keyed by CIN. Uses O(1) dict lookup.

Events fired:
  STATUS_CHANGE       ALERT      — status field changed
  CAPITAL_CHANGE      WATCH      — paid_up_capital moved > 50%
  AGM_OVERDUE         WATCH      — Active company, AGM > 18 months ago, not previously flagged
  NEW_COMPANY         INFO       — CIN not in old set
  COMPANY_REMOVED     ALERT      — CIN was Active, now absent from new set
"""

from datetime import datetime, timezone
from typing import List

from .base import BaseDetector, EventSpec


class OGDDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        old_by_cin: dict = {r["cin"]: r for r in old_records if r.get("cin")}
        new_by_cin: dict = {r["cin"]: r for r in new_records if r.get("cin")}

        # Detect changes and new entries
        for cin, new_rec in new_by_cin.items():
            old_rec = old_by_cin.get(cin)

            if old_rec is None:
                # New CIN appeared
                events.append(EventSpec(
                    cin=cin,
                    event_type="NEW_COMPANY",
                    severity="INFO",
                    data={
                        "company_name": new_rec.get("company_name"),
                        "status": new_rec.get("status"),
                        "registration_date": new_rec.get("date_of_registration"),
                    },
                ))
                continue

            # Status changed
            old_status = (old_rec.get("status") or "").strip()
            new_status = (new_rec.get("status") or "").strip()
            if old_status and new_status and old_status != new_status:
                events.append(EventSpec(
                    cin=cin,
                    event_type="STATUS_CHANGE",
                    severity="ALERT",
                    data={
                        "company_name": new_rec.get("company_name"),
                        "old_status": old_status,
                        "new_status": new_status,
                    },
                ))

            # Paid-up capital moved > 50%
            old_cap = _parse_capital(old_rec.get("paid_up_capital"))
            new_cap = _parse_capital(new_rec.get("paid_up_capital"))
            if old_cap is not None and new_cap is not None and old_cap > 0:
                ratio = abs(new_cap - old_cap) / old_cap
                if ratio > 0.50:
                    events.append(EventSpec(
                        cin=cin,
                        event_type="CAPITAL_CHANGE",
                        severity="WATCH",
                        data={
                            "company_name": new_rec.get("company_name"),
                            "old_capital": old_cap,
                            "new_capital": new_cap,
                            "change_pct": round(ratio * 100, 2),
                        },
                    ))

            # AGM overdue — Active company, AGM > 18 months ago
            # Only fire if not already flagged in previous state
            if new_status == "Active":
                agm_overdue = _is_agm_overdue(new_rec.get("date_of_last_agm"))
                old_agm_overdue = _is_agm_overdue(old_rec.get("date_of_last_agm"))
                # Fire only on transition: was not overdue before, is overdue now
                # OR was overdue but date_of_last_agm is unchanged (already flagged — skip)
                if agm_overdue and not old_agm_overdue:
                    events.append(EventSpec(
                        cin=cin,
                        event_type="AGM_OVERDUE",
                        severity="WATCH",
                        data={
                            "company_name": new_rec.get("company_name"),
                            "date_of_last_agm": new_rec.get("date_of_last_agm"),
                        },
                    ))

        # Detect removed Active companies
        for cin, old_rec in old_by_cin.items():
            if cin not in new_by_cin:
                old_status = (old_rec.get("status") or "").strip()
                if old_status == "Active":
                    events.append(EventSpec(
                        cin=cin,
                        event_type="COMPANY_REMOVED",
                        severity="ALERT",
                        data={
                            "company_name": old_rec.get("company_name"),
                            "last_known_status": old_status,
                        },
                    ))

        return events


def _parse_capital(value) -> float | None:
    """Parse paid_up_capital to float. Returns None if unparseable."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _is_agm_overdue(date_str) -> bool:
    """Return True if date is more than 18 months before today."""
    if not date_str:
        return False
    try:
        agm_date = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        delta_days = (now - agm_date).days
        return delta_days > 548  # 18 months * 30.4 days
    except (ValueError, TypeError):
        return False
```

---

## File: `detection/detectors/nclt.py`

```python
"""
NCLT detector — source_id: 'nclt'

Append-only source. Each scraped record is a new filing.
Dedup against legal_events using (cin, case_number).

Events fired:
  NCLT_SEC7_FILED       CRITICAL
  NCLT_SEC9_FILED       CRITICAL
  NCLT_SEC10_FILED      ALERT
  CIRP_ADMITTED         CRITICAL
  LIQUIDATION_ORDERED   CRITICAL
  RESOLUTION_APPROVED   WATCH
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


# Maps filing_type string (normalised to lowercase) → (event_type, severity)
FILING_TYPE_MAP = {
    "section 7":             ("NCLT_SEC7_FILED",     "CRITICAL"),
    "sec 7":                 ("NCLT_SEC7_FILED",     "CRITICAL"),
    "s7":                    ("NCLT_SEC7_FILED",     "CRITICAL"),
    "section 9":             ("NCLT_SEC9_FILED",     "CRITICAL"),
    "sec 9":                 ("NCLT_SEC9_FILED",     "CRITICAL"),
    "s9":                    ("NCLT_SEC9_FILED",     "CRITICAL"),
    "section 10":            ("NCLT_SEC10_FILED",    "ALERT"),
    "sec 10":                ("NCLT_SEC10_FILED",    "ALERT"),
    "s10":                   ("NCLT_SEC10_FILED",    "ALERT"),
    "cirp admitted":         ("CIRP_ADMITTED",       "CRITICAL"),
    "admitted":              ("CIRP_ADMITTED",       "CRITICAL"),
    "liquidation ordered":   ("LIQUIDATION_ORDERED", "CRITICAL"),
    "liquidation":           ("LIQUIDATION_ORDERED", "CRITICAL"),
    "resolution approved":   ("RESOLUTION_APPROVED", "WATCH"),
    "resolution plan":       ("RESOLUTION_APPROVED", "WATCH"),
}


class NCLTDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],   # unused — dedup done against DB directly
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        # Fetch existing (cin, case_number) pairs from legal_events to dedup
        existing: Set[Tuple[str, str]] = await _fetch_existing_nclt(db)

        for record in new_records:
            cin = record.get("cin")
            case_number = str(record.get("case_number") or "").strip()

            if not cin or not case_number:
                continue

            if (cin, case_number) in existing:
                continue  # already ingested

            filing_type = str(record.get("filing_type") or "").lower().strip()
            event_type, severity = _map_filing_type(filing_type)

            events.append(EventSpec(
                cin=cin,
                event_type=event_type,
                severity=severity,
                data={
                    "case_number": case_number,
                    "filing_type": record.get("filing_type"),
                    "bench": record.get("bench"),
                    "petitioner": record.get("petitioner"),
                    "respondent": record.get("respondent"),
                    "filing_date": record.get("filing_date"),
                    "next_date": record.get("next_date"),
                },
            ))
            existing.add((cin, case_number))  # prevent within-batch duplicates

        return events


async def _fetch_existing_nclt(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch(
        "SELECT cin, case_number FROM legal_events WHERE source = 'nclt'"
    )
    return {(r["cin"], r["case_number"]) for r in rows}


def _map_filing_type(filing_type: str) -> Tuple[str, str]:
    for key, val in FILING_TYPE_MAP.items():
        if key in filing_type:
            return val
    # Default: treat as a generic NCLT filing
    return ("NCLT_SEC7_FILED", "CRITICAL")
```

---

## File: `detection/detectors/drt.py`

```python
"""
DRT detector — source_id: 'drt'

Append-only source. New records = new recovery applications or orders.
Dedup against legal_events using (cin, case_number).

Events fired:
  DRT_APPLICATION_FILED   ALERT
  DRT_ORDER_PASSED        ALERT
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


class DRTDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        existing: Set[Tuple[str, str]] = await _fetch_existing_drt(db)

        for record in new_records:
            cin = record.get("cin")
            case_number = str(record.get("case_number") or "").strip()

            if not cin or not case_number:
                continue

            if (cin, case_number) in existing:
                continue

            record_type = str(record.get("record_type") or "").lower().strip()

            if "order" in record_type:
                event_type = "DRT_ORDER_PASSED"
            else:
                event_type = "DRT_APPLICATION_FILED"

            events.append(EventSpec(
                cin=cin,
                event_type=event_type,
                severity="ALERT",
                data={
                    "case_number": case_number,
                    "record_type": record.get("record_type"),
                    "drt_bench": record.get("drt_bench"),
                    "applicant_bank": record.get("applicant_bank"),
                    "amount_claimed": record.get("amount_claimed"),
                    "filing_date": record.get("filing_date"),
                    "order_date": record.get("order_date"),
                },
            ))
            existing.add((cin, case_number))

        return events


async def _fetch_existing_drt(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch(
        "SELECT cin, case_number FROM legal_events WHERE source = 'drt'"
    )
    return {(r["cin"], r["case_number"]) for r in rows}
```

---

## File: `detection/detectors/sarfaesi.py`

```python
"""
SARFAESI detector — source_id: 'sarfaesi'

Append-only. Critical to distinguish notice stages.
Dedup against legal_events using (cin, case_number, notice_stage).

Events fired:
  SARFAESI_DEMAND_NOTICE      ALERT      — Section 13(2)
  SARFAESI_POSSESSION_TAKEN   CRITICAL   — Section 13(4)
  SARFAESI_AUCTION_SCHEDULED  CRITICAL   — Auction date set
  SARFAESI_AUCTION_COMPLETED  ALERT      — Auction result recorded
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


# Maps notice_stage (normalised) → (event_type, severity)
STAGE_MAP = {
    "13(2)":           ("SARFAESI_DEMAND_NOTICE",     "ALERT"),
    "section 13(2)":   ("SARFAESI_DEMAND_NOTICE",     "ALERT"),
    "demand notice":   ("SARFAESI_DEMAND_NOTICE",     "ALERT"),
    "13(4)":           ("SARFAESI_POSSESSION_TAKEN",  "CRITICAL"),
    "section 13(4)":   ("SARFAESI_POSSESSION_TAKEN",  "CRITICAL"),
    "possession":      ("SARFAESI_POSSESSION_TAKEN",  "CRITICAL"),
    "auction scheduled": ("SARFAESI_AUCTION_SCHEDULED", "CRITICAL"),
    "auction notice":  ("SARFAESI_AUCTION_SCHEDULED", "CRITICAL"),
    "auction completed": ("SARFAESI_AUCTION_COMPLETED", "ALERT"),
    "auction sold":    ("SARFAESI_AUCTION_COMPLETED", "ALERT"),
}


class SARFAESIDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        existing: Set[Tuple[str, str, str]] = await _fetch_existing_sarfaesi(db)

        for record in new_records:
            cin = record.get("cin")
            case_number = str(record.get("case_number") or "").strip()
            notice_stage = str(record.get("notice_stage") or "").strip()

            if not cin or not case_number:
                continue

            dedup_key = (cin, case_number, notice_stage.lower())
            if dedup_key in existing:
                continue

            event_type, severity = _map_stage(notice_stage)

            events.append(EventSpec(
                cin=cin,
                event_type=event_type,
                severity=severity,
                data={
                    "case_number": case_number,
                    "notice_stage": notice_stage,
                    "secured_creditor": record.get("secured_creditor"),
                    "property_description": record.get("property_description"),
                    "outstanding_amount": record.get("outstanding_amount"),
                    "notice_date": record.get("notice_date"),
                    "auction_date": record.get("auction_date"),
                    "reserve_price": record.get("reserve_price"),
                },
            ))
            existing.add(dedup_key)

        return events


async def _fetch_existing_sarfaesi(db) -> Set[Tuple[str, str, str]]:
    rows = await db.fetch(
        """
        SELECT cin, case_number, data_json->>'notice_stage' AS notice_stage
        FROM legal_events
        WHERE source = 'sarfaesi'
        """
    )
    return {(r["cin"], r["case_number"], (r["notice_stage"] or "").lower()) for r in rows}


def _map_stage(notice_stage: str) -> Tuple[str, str]:
    normalised = notice_stage.lower().strip()
    for key, val in STAGE_MAP.items():
        if key in normalised:
            return val
    return ("SARFAESI_DEMAND_NOTICE", "ALERT")
```

---

## File: `detection/detectors/ecourts.py`

```python
"""
e-Courts detector — source_id: 'ecourts'

Append-only. New scraped cases are compared against legal_events.
Dedup on (cin, case_number).

Events fired:
  SEC138_FILED      ALERT      — New Section 138 cheque bounce case
  SEC138_MULTIPLE   CRITICAL   — 3rd or more Section 138 case on the same CIN
  CIVIL_SUIT_FILED  WATCH      — Civil suit with claim > ₹1 Cr
  CASE_DISPOSED     INFO       — Case disposal recorded
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec

SEC138_THRESHOLD = 3          # escalate to CRITICAL at this count
CIVIL_SUIT_AMOUNT_THRESHOLD = 10_000_000  # ₹1 Cr in rupees


class ECourtsDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        existing: Set[Tuple[str, str]] = await _fetch_existing_ecourts(db)

        for record in new_records:
            cin = record.get("cin")
            case_number = str(record.get("case_number") or "").strip()

            if not cin or not case_number:
                continue

            if (cin, case_number) in existing:
                continue

            case_type = str(record.get("case_type") or "").lower().strip()
            disposal_status = str(record.get("disposal_status") or "").lower().strip()

            # Case disposed
            if disposal_status in ("disposed", "decided", "closed"):
                events.append(EventSpec(
                    cin=cin,
                    event_type="CASE_DISPOSED",
                    severity="INFO",
                    data={
                        "case_number": case_number,
                        "case_type": record.get("case_type"),
                        "court": record.get("court"),
                        "disposal_date": record.get("disposal_date"),
                        "disposal_status": record.get("disposal_status"),
                    },
                ))
                existing.add((cin, case_number))
                continue

            # Section 138 — cheque bounce
            if _is_sec138(case_type):
                # Count existing Sec 138 cases for this CIN
                count = await _count_sec138(db, cin)
                count += 1  # include this new one

                severity = "CRITICAL" if count >= SEC138_THRESHOLD else "ALERT"
                event_type = "SEC138_MULTIPLE" if count >= SEC138_THRESHOLD else "SEC138_FILED"

                events.append(EventSpec(
                    cin=cin,
                    event_type=event_type,
                    severity=severity,
                    data={
                        "case_number": case_number,
                        "case_type": record.get("case_type"),
                        "court": record.get("court"),
                        "complainant": record.get("complainant"),
                        "filing_date": record.get("filing_date"),
                        "sec138_count": count,
                    },
                ))
                existing.add((cin, case_number))
                continue

            # Civil suit above ₹1 Cr
            claim_amount = _parse_amount(record.get("claim_amount"))
            if claim_amount and claim_amount > CIVIL_SUIT_AMOUNT_THRESHOLD:
                events.append(EventSpec(
                    cin=cin,
                    event_type="CIVIL_SUIT_FILED",
                    severity="WATCH",
                    data={
                        "case_number": case_number,
                        "case_type": record.get("case_type"),
                        "court": record.get("court"),
                        "claim_amount": claim_amount,
                        "plaintiff": record.get("plaintiff"),
                        "filing_date": record.get("filing_date"),
                    },
                ))
                existing.add((cin, case_number))

        return events


async def _fetch_existing_ecourts(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch(
        "SELECT cin, case_number FROM legal_events WHERE source = 'ecourts'"
    )
    return {(r["cin"], r["case_number"]) for r in rows}


async def _count_sec138(db, cin: str) -> int:
    row = await db.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM legal_events
        WHERE source = 'ecourts'
          AND cin = $1
          AND event_type IN ('SEC138_FILED', 'SEC138_MULTIPLE')
        """,
        cin,
    )
    return int(row["cnt"]) if row else 0


def _is_sec138(case_type: str) -> bool:
    return "138" in case_type or "cheque" in case_type or "ni act" in case_type


def _parse_amount(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
```

---

## File: `detection/detectors/directors.py`

```python
"""
Director detector — source_id: 'mca_directors'

Compares old vs new DIN-CIN mappings from governance_graph.
Records keyed by (din, cin).

Events fired:
  DIRECTOR_RESIGNED     WATCH / ALERT   — ALERT if role is CFO or Auditor
  DIRECTOR_APPOINTED    INFO
  DIRECTOR_OVERLOADED   WATCH           — director now on 10+ boards
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec

OVERLOAD_THRESHOLD = 10
HIGH_RISK_ROLES = {"cfo", "chief financial officer", "auditor", "statutory auditor"}


class DirectorDetector(BaseDetector):

    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []

        # Key: (din, cin)
        old_by_key: dict = {_edge_key(r): r for r in old_records if r.get("din") and r.get("cin")}
        new_by_key: dict = {_edge_key(r): r for r in new_records if r.get("din") and r.get("cin")}

        # Overload check — track DINs already emitted this run to avoid duplicates
        overload_emitted: Set[str] = set()

        for key, new_rec in new_by_key.items():
            old_rec = old_by_key.get(key)
            din = new_rec["din"]
            cin = new_rec["cin"]

            if old_rec is None:
                # New appointment
                events.append(EventSpec(
                    cin=cin,
                    event_type="DIRECTOR_APPOINTED",
                    severity="INFO",
                    data={
                        "din": din,
                        "name": new_rec.get("director_name"),
                        "designation": new_rec.get("designation"),
                        "appointment_date": new_rec.get("appointment_date"),
                    },
                ))
            else:
                # Check for cessation (resignation/removal)
                old_cessation = old_rec.get("cessation_date")
                new_cessation = new_rec.get("cessation_date")

                if not old_cessation and new_cessation:
                    role = str(new_rec.get("designation") or "").lower()
                    severity = "ALERT" if any(r in role for r in HIGH_RISK_ROLES) else "WATCH"
                    events.append(EventSpec(
                        cin=cin,
                        event_type="DIRECTOR_RESIGNED",
                        severity=severity,
                        data={
                            "din": din,
                            "name": new_rec.get("director_name"),
                            "designation": new_rec.get("designation"),
                            "cessation_date": new_cessation,
                        },
                    ))

            # Overload check — director now on 10+ boards
            if din not in overload_emitted:
                board_count = await _count_boards(db, din)
                if board_count >= OVERLOAD_THRESHOLD:
                    overload_emitted.add(din)
                    events.append(EventSpec(
                        cin=cin,   # anchor to this company (arbitrary but required)
                        event_type="DIRECTOR_OVERLOADED",
                        severity="WATCH",
                        data={
                            "din": din,
                            "name": new_rec.get("director_name"),
                            "board_count": board_count,
                        },
                    ))

        return events


async def _count_boards(db, din: str) -> int:
    row = await db.fetchrow(
        """
        SELECT COUNT(DISTINCT cin) AS cnt
        FROM governance_graph
        WHERE din = $1
          AND (cessation_date IS NULL OR cessation_date > NOW())
        """,
        din,
    )
    return int(row["cnt"]) if row else 0


def _edge_key(record: dict) -> Tuple[str, str]:
    return (str(record["din"]), str(record["cin"]))
```

---

## File: `detection/diff_engine.py`

```python
"""
DiffEngine — detection layer entry point.

For each source, on each pull:
  1. Compute hash of new data.
  2. Load last known hash from source_state.
  3. If hash unchanged: update pull timestamp, return immediately — zero events.
  4. If hash changed: run source-specific detector, fire events, update state.
  5. On exception: increment consecutive_failures; if >= 4, fire SOURCE_UNREACHABLE.

Never reprocesses unchanged data. Never fires duplicate events.
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Type

import asyncpg

from .detectors.base import BaseDetector, EventSpec
from .detectors.directors import DirectorDetector
from .detectors.drt import DRTDetector
from .detectors.ecourts import ECourtsDetector
from .detectors.nclt import NCLTDetector
from .detectors.ogd import OGDDetector
from .detectors.sarfaesi import SARFAESIDetector

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = 4

# Registry: source_id → detector class
DETECTOR_REGISTRY: Dict[str, Type[BaseDetector]] = {
    "mca_ogd":       OGDDetector,
    "nclt":          NCLTDetector,
    "drt":           DRTDetector,
    "sarfaesi":      SARFAESIDetector,
    "ecourts":       ECourtsDetector,
    "mca_directors": DirectorDetector,
}


@dataclass
class DiffResult:
    source_id: str
    records_processed: int
    events_fired: int
    hash_changed: bool
    duration_ms: int
    errors: List[str] = field(default_factory=list)


class DiffEngine:
    def __init__(self, db: asyncpg.Pool):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def process_source(
        self, source_id: str, new_data: List[dict]
    ) -> DiffResult:
        """
        Main entry point called by each scraper after pulling new data.
        new_data is the full current snapshot (or list of new records) from the source.
        """
        start = time.monotonic()
        errors: List[str] = []

        try:
            new_hash = await self._compute_hash(new_data)
            last_state = await self._load_last_state(source_id)
            last_hash = (last_state or {}).get("last_data_hash")

            if last_hash == new_hash:
                # Nothing changed
                await self._update_source_state(
                    source_id, new_hash, len(new_data), "OK"
                )
                return DiffResult(
                    source_id=source_id,
                    records_processed=len(new_data),
                    events_fired=0,
                    hash_changed=False,
                    duration_ms=_elapsed_ms(start),
                )

            # Hash changed — run detector
            detector_cls = DETECTOR_REGISTRY.get(source_id)
            if detector_cls is None:
                raise ValueError(f"No detector registered for source_id='{source_id}'")

            detector = detector_cls()

            # For diff-based sources (OGD, directors), we pass old records from DB.
            # For append-only sources (NCLT, DRT, SARFAESI, e-Courts), old_records is
            # empty — the detector queries legal_events directly for dedup.
            old_records = await self._load_old_records(source_id, last_state)

            async with self.db.acquire() as conn:
                event_specs: List[EventSpec] = await detector.detect_events(
                    old_records, new_data, conn
                )

                events_fired = 0
                for spec in event_specs:
                    spec.source = source_id
                    try:
                        await self._fire_event(
                            spec.cin, source_id, spec.event_type,
                            spec.severity, spec.data, conn
                        )
                        events_fired += 1
                    except Exception as e:
                        err = f"Failed to fire {spec.event_type} for cin={spec.cin}: {e}"
                        logger.error(err)
                        errors.append(err)

            await self._update_source_state(
                source_id, new_hash, len(new_data), "OK"
            )

            return DiffResult(
                source_id=source_id,
                records_processed=len(new_data),
                events_fired=events_fired,
                hash_changed=True,
                duration_ms=_elapsed_ms(start),
                errors=errors,
            )

        except Exception as e:
            logger.exception(f"DiffEngine error for source_id='{source_id}': {e}")
            errors.append(str(e))
            await self._handle_source_failure(source_id)
            return DiffResult(
                source_id=source_id,
                records_processed=0,
                events_fired=0,
                hash_changed=False,
                duration_ms=_elapsed_ms(start),
                errors=errors,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _compute_hash(self, data: List[dict]) -> str:
        """
        Stable SHA-256 hash of the dataset.
        Records are sorted by CIN where present, then by case_number, then
        full JSON as fallback, to guarantee deterministic ordering.
        """
        sorted_data = sorted(
            data,
            key=lambda r: (
                str(r.get("cin") or ""),
                str(r.get("case_number") or ""),
                json.dumps(r, sort_keys=True, default=str),
            ),
        )
        serialised = json.dumps(sorted_data, sort_keys=True, default=str)
        return hashlib.sha256(serialised.encode("utf-8")).hexdigest()

    async def _load_last_state(self, source_id: str) -> Optional[dict]:
        row = await self.db.fetchrow(
            "SELECT * FROM source_state WHERE source_id = $1",
            source_id,
        )
        return dict(row) if row else None

    async def _load_old_records(
        self, source_id: str, last_state: Optional[dict]
    ) -> List[dict]:
        """
        For diff-based sources, retrieve the last snapshot from the DB.
        Append-only sources return empty list — they dedup via legal_events.
        """
        append_only = {"nclt", "drt", "sarfaesi", "ecourts"}
        if source_id in append_only:
            return []

        if source_id == "mca_ogd":
            rows = await self.db.fetch(
                """
                SELECT cin, company_name, status, paid_up_capital,
                       date_of_last_agm, date_of_registration
                FROM master_entities
                """
            )
            return [dict(r) for r in rows]

        if source_id == "mca_directors":
            rows = await self.db.fetch(
                """
                SELECT din, cin, director_name, designation,
                       appointment_date, cessation_date
                FROM governance_graph
                """
            )
            return [dict(r) for r in rows]

        return []

    async def _fire_event(
        self,
        cin: Optional[str],
        source: str,
        event_type: str,
        severity: str,
        data: dict,
        conn,  # asyncpg connection
    ) -> int:
        """Insert into events table. Returns the new event id."""
        row = await conn.fetchrow(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES ($1, $2, $3, $4, NOW(), $5)
            RETURNING id
            """,
            cin,
            source,
            event_type,
            severity,
            json.dumps(data, default=str),
        )
        return row["id"]

    async def _update_source_state(
        self,
        source_id: str,
        new_hash: str,
        record_count: int,
        status: str,
    ):
        await self.db.execute(
            """
            INSERT INTO source_state (source_id, last_data_hash, record_count,
                                      status, last_pull_at, consecutive_failures)
            VALUES ($1, $2, $3, $4, NOW(), 0)
            ON CONFLICT (source_id) DO UPDATE
              SET last_data_hash       = EXCLUDED.last_data_hash,
                  record_count         = EXCLUDED.record_count,
                  status               = EXCLUDED.status,
                  last_pull_at         = EXCLUDED.last_pull_at,
                  consecutive_failures = 0
            """,
            source_id,
            new_hash,
            record_count,
            status,
        )

    async def _handle_source_failure(self, source_id: str):
        """
        Increment consecutive_failures. If >= FAILURE_THRESHOLD, mark UNREACHABLE
        and fire a SOURCE_UNREACHABLE operator alert.
        """
        row = await self.db.fetchrow(
            """
            UPDATE source_state
            SET consecutive_failures = consecutive_failures + 1,
                status = CASE
                    WHEN consecutive_failures + 1 >= $2 THEN 'UNREACHABLE'
                    ELSE 'DEGRADED'
                END,
                last_pull_at = NOW()
            WHERE source_id = $1
            RETURNING consecutive_failures, status
            """,
            source_id,
            FAILURE_THRESHOLD,
        )

        if row is None:
            # source_state row did not exist yet — insert it
            await self.db.execute(
                """
                INSERT INTO source_state (source_id, consecutive_failures, status, last_pull_at)
                VALUES ($1, 1, 'DEGRADED', NOW())
                ON CONFLICT (source_id) DO NOTHING
                """,
                source_id,
            )
            return

        if row["status"] == "UNREACHABLE":
            logger.critical(
                f"Source '{source_id}' has reached {row['consecutive_failures']} "
                f"consecutive failures — firing SOURCE_UNREACHABLE"
            )
            try:
                await self.db.execute(
                    """
                    INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
                    VALUES (NULL, $1, 'SOURCE_UNREACHABLE', 'CRITICAL', NOW(), $2)
                    """,
                    source_id,
                    json.dumps({"consecutive_failures": row["consecutive_failures"]}),
                )
            except Exception as e:
                logger.error(f"Could not fire SOURCE_UNREACHABLE alert: {e}")


def _elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
```

---

## File: `tests/test_diff_engine.py`

```python
"""
pytest test suite for the diff engine.

Uses pytest-asyncio and unittest.mock. No live DB required — all DB calls
are mocked via AsyncMock or a fake asyncpg pool fixture.

Run: pytest tests/test_diff_engine.py -v
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from detection.diff_engine import DiffEngine, FAILURE_THRESHOLD
from detection.detectors.base import EventSpec
from detection.detectors.directors import DirectorDetector
from detection.detectors.drt import DRTDetector
from detection.detectors.ecourts import ECourtsDetector
from detection.detectors.nclt import NCLTDetector
from detection.detectors.ogd import OGDDetector, _is_agm_overdue
from detection.detectors.sarfaesi import SARFAESIDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool(fetchrow_return=None, fetch_return=None, execute_return=None):
    """Build a minimal asyncpg pool mock."""
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=fetchrow_return)
    pool.fetch = AsyncMock(return_value=fetch_return or [])
    pool.execute = AsyncMock(return_value=execute_return)

    # .acquire() returns an async context manager that yields a connection mock
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
    """Return an AGM date > 18 months ago."""
    past = datetime.now(timezone.utc) - timedelta(days=600)
    return past.strftime("%Y-%m-%d")


def _agm_date_recent() -> str:
    """Return an AGM date < 18 months ago."""
    past = datetime.now(timezone.utc) - timedelta(days=100)
    return past.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# 1. Hash unchanged → DiffResult.hash_changed=False, events_fired=0
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hash_unchanged_fires_nothing():
    data = [{"cin": "U12345MH2020PTC123456", "status": "Active"}]

    # Compute what the hash will be
    sorted_data = sorted(
        data,
        key=lambda r: (str(r.get("cin") or ""), str(r.get("case_number") or ""),
                       json.dumps(r, sort_keys=True, default=str)),
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


# ---------------------------------------------------------------------------
# 2. Status change in OGD → STATUS_CHANGE event fired
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ogd_status_change_fires_event():
    old_records = [{"cin": "U12345MH2020PTC123456", "status": "Active",
                    "company_name": "Acme Ltd", "paid_up_capital": None,
                    "date_of_last_agm": None}]
    new_records = [{"cin": "U12345MH2020PTC123456", "status": "Struck Off",
                    "company_name": "Acme Ltd", "paid_up_capital": None,
                    "date_of_last_agm": None}]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)

    status_events = [e for e in events if e.event_type == "STATUS_CHANGE"]
    assert len(status_events) == 1
    ev = status_events[0]
    assert ev.severity == "ALERT"
    assert ev.data["old_status"] == "Active"
    assert ev.data["new_status"] == "Struck Off"


# ---------------------------------------------------------------------------
# 3. Capital increase >50% → CAPITAL_CHANGE fired; <50% → not fired
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ogd_capital_change_above_threshold():
    old = [{"cin": "U1", "status": "Active", "company_name": "X",
            "paid_up_capital": "1000000", "date_of_last_agm": None}]
    new = [{"cin": "U1", "status": "Active", "company_name": "X",
            "paid_up_capital": "2000000", "date_of_last_agm": None}]

    detector = OGDDetector()
    events = await detector.detect_events(old, new, db=None)
    cap_events = [e for e in events if e.event_type == "CAPITAL_CHANGE"]
    assert len(cap_events) == 1


@pytest.mark.asyncio
async def test_ogd_capital_change_below_threshold_not_fired():
    old = [{"cin": "U1", "status": "Active", "company_name": "X",
            "paid_up_capital": "1000000", "date_of_last_agm": None}]
    new = [{"cin": "U1", "status": "Active", "company_name": "X",
            "paid_up_capital": "1200000", "date_of_last_agm": None}]  # +20%

    detector = OGDDetector()
    events = await detector.detect_events(old, new, db=None)
    cap_events = [e for e in events if e.event_type == "CAPITAL_CHANGE"]
    assert len(cap_events) == 0


# ---------------------------------------------------------------------------
# 4. AGM overdue → fires only once (idempotent on re-run)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ogd_agm_overdue_fires_once():
    recent_agm = _agm_date_recent()
    overdue_agm = _agm_date_overdue()

    old_records = [{"cin": "U2", "status": "Active", "company_name": "Y",
                    "paid_up_capital": None, "date_of_last_agm": recent_agm}]
    new_records = [{"cin": "U2", "status": "Active", "company_name": "Y",
                    "paid_up_capital": None, "date_of_last_agm": overdue_agm}]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)
    agm_events = [e for e in events if e.event_type == "AGM_OVERDUE"]
    assert len(agm_events) == 1

    # Second run: old and new both have the same overdue AGM — must not re-fire
    events2 = await detector.detect_events(new_records, new_records, db=None)
    agm_events2 = [e for e in events2 if e.event_type == "AGM_OVERDUE"]
    assert len(agm_events2) == 0


# ---------------------------------------------------------------------------
# 5. New CIN → NEW_COMPANY event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ogd_new_cin_fires_new_company():
    old_records: List[dict] = []
    new_records = [{"cin": "U99", "status": "Active", "company_name": "NewCo",
                    "paid_up_capital": None, "date_of_last_agm": None}]

    detector = OGDDetector()
    events = await detector.detect_events(old_records, new_records, db=None)
    new_events = [e for e in events if e.event_type == "NEW_COMPANY"]
    assert len(new_events) == 1
    assert new_events[0].severity == "INFO"


# ---------------------------------------------------------------------------
# 6. NCLT dedup → same case_number not double-inserted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_nclt_dedup_same_case_number():
    existing_row = MagicMock()
    existing_row.__getitem__ = lambda self, k: "NCLT/MB/7/2024" if k == "case_number" else "U3"
    existing_row.__iter__ = lambda self: iter(["cin", "case_number"])

    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[
        {"cin": "U3", "case_number": "NCLT/MB/7/2024"}
    ])

    new_records = [
        {"cin": "U3", "case_number": "NCLT/MB/7/2024", "filing_type": "Section 7",
         "bench": "Mumbai", "petitioner": "Bank A", "respondent": "Acme",
         "filing_date": "2024-01-15", "next_date": "2024-02-10"},
    ]

    detector = NCLTDetector()
    events = await detector.detect_events([], new_records, db)
    assert len(events) == 0


# ---------------------------------------------------------------------------
# 7. SARFAESI stages fire correct event_types
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sarfaesi_stages():
    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[])

    stages = [
        ("13(2)",            "SARFAESI_DEMAND_NOTICE",    "ALERT"),
        ("13(4)",            "SARFAESI_POSSESSION_TAKEN", "CRITICAL"),
        ("auction scheduled","SARFAESI_AUCTION_SCHEDULED","CRITICAL"),
        ("auction completed","SARFAESI_AUCTION_COMPLETED","ALERT"),
    ]

    for stage, expected_event, expected_severity in stages:
        records = [{
            "cin": f"U_{stage}",
            "case_number": f"CASE_{stage}",
            "notice_stage": stage,
            "secured_creditor": "SBI",
            "property_description": "Plot 5",
            "outstanding_amount": 5000000,
            "notice_date": "2024-03-01",
            "auction_date": None,
            "reserve_price": None,
        }]
        detector = SARFAESIDetector()
        events = await detector.detect_events([], records, db)
        assert len(events) == 1, f"Expected 1 event for stage '{stage}', got {len(events)}"
        assert events[0].event_type == expected_event, \
            f"Stage '{stage}': expected {expected_event}, got {events[0].event_type}"
        assert events[0].severity == expected_severity


# ---------------------------------------------------------------------------
# 8. SEC138 3rd case → severity escalates to CRITICAL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ecourts_sec138_escalation():
    db = AsyncMock()
    db.fetch = AsyncMock(return_value=[])  # no existing cases in legal_events

    # Simulate 2 existing Sec 138 cases already in events table
    db.fetchrow = AsyncMock(return_value={"cnt": 2})

    new_records = [{
        "cin": "U5",
        "case_number": "CC/138/2024/03",
        "case_type": "Section 138 NI Act",
        "disposal_status": "",
        "court": "MM Court Mumbai",
        "complainant": "Vendor X",
        "filing_date": "2024-03-01",
        "claim_amount": None,
    }]

    detector = ECourtsDetector()
    events = await detector.detect_events([], new_records, db)

    assert len(events) == 1
    assert events[0].event_type == "SEC138_MULTIPLE"
    assert events[0].severity == "CRITICAL"
    assert events[0].data["sec138_count"] == 3


# ---------------------------------------------------------------------------
# 9. Consecutive failures → SOURCE_UNREACHABLE after 4th failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_source_unreachable_after_threshold():
    pool, conn = _make_pool()

    # Simulate consecutive_failures reaching threshold on update
    pool.fetchrow = AsyncMock(return_value={
        "consecutive_failures": FAILURE_THRESHOLD,
        "status": "UNREACHABLE",
    })

    engine = DiffEngine(pool)

    # Force an exception inside process_source by giving it an unregistered source_id
    # but with a hash mismatch (so it tries to look up the detector and fails)
    pool.fetch = AsyncMock(return_value=[])  # _load_last_state returns None via fetchrow=None

    with patch.object(engine, "_load_last_state", return_value={"last_data_hash": "old_hash"}):
        with patch.object(engine, "_compute_hash", return_value="new_hash"):
            with patch.object(engine, "_load_old_records", return_value=[]):
                # No detector registered for 'unknown_source' → raises ValueError
                result = await engine.process_source("unknown_source", [{"cin": "X"}])

    assert len(result.errors) > 0
    # SOURCE_UNREACHABLE event should have been inserted
    pool.execute.assert_called()
    calls = [str(c) for c in pool.execute.call_args_list]
    # At least one call should reference SOURCE_UNREACHABLE
    assert any("SOURCE_UNREACHABLE" in c for c in calls)


# ---------------------------------------------------------------------------
# 10. Director resignation fires DIRECTOR_RESIGNED
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_director_resigned_fires_event():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 2})  # only 2 boards, no overload

    old_records = [{
        "din": "00123456",
        "cin": "U6",
        "director_name": "John Doe",
        "designation": "Director",
        "appointment_date": "2015-01-01",
        "cessation_date": None,   # no cessation before
    }]
    new_records = [{
        "din": "00123456",
        "cin": "U6",
        "director_name": "John Doe",
        "designation": "Director",
        "appointment_date": "2015-01-01",
        "cessation_date": "2024-03-15",  # resigned
    }]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    resigned = [e for e in events if e.event_type == "DIRECTOR_RESIGNED"]
    assert len(resigned) == 1
    assert resigned[0].severity == "WATCH"
    assert resigned[0].data["din"] == "00123456"


@pytest.mark.asyncio
async def test_cfo_resignation_escalates_to_alert():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 1})

    old_records = [{
        "din": "00999999",
        "cin": "U7",
        "director_name": "Jane Smith",
        "designation": "Chief Financial Officer",
        "appointment_date": "2016-06-01",
        "cessation_date": None,
    }]
    new_records = [{
        "din": "00999999",
        "cin": "U7",
        "director_name": "Jane Smith",
        "designation": "Chief Financial Officer",
        "appointment_date": "2016-06-01",
        "cessation_date": "2024-03-15",
    }]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    resigned = [e for e in events if e.event_type == "DIRECTOR_RESIGNED"]
    assert len(resigned) == 1
    assert resigned[0].severity == "ALERT"


# ---------------------------------------------------------------------------
# Bonus: Director overload fires at threshold
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_director_overload_fires():
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"cnt": 10})  # exactly at threshold

    old_records: List[dict] = []
    new_records = [{
        "din": "00777777",
        "cin": "U8",
        "director_name": "Busy Person",
        "designation": "Director",
        "appointment_date": "2020-01-01",
        "cessation_date": None,
    }]

    detector = DirectorDetector()
    events = await detector.detect_events(old_records, new_records, db)

    overload = [e for e in events if e.event_type == "DIRECTOR_OVERLOADED"]
    assert len(overload) == 1
    assert overload[0].severity == "WATCH"
    assert overload[0].data["board_count"] == 10
```

---

## Behaviour contracts Codex must preserve

### Hash stability
- Sort order: primary on `cin`, secondary on `case_number`, tertiary on full JSON dump.
- `json.dumps(..., sort_keys=True, default=str)` — always `sort_keys=True`. Dict insertion order must not affect the hash.

### Idempotency
- Running `process_source` twice with identical `new_data` must produce zero events on the second run. The hash check guarantees this at the source level.
- Within a single run, detectors must not emit duplicate `EventSpec` entries for the same record. Use the `existing` set (updated in-loop) to prevent within-batch duplicates.

### CIN resolution
- Every `EventSpec` must carry a resolved CIN or `None` (operator alerts only).
- If a scraper provides a company name but not a CIN, resolution must happen before calling `detect_events`. Resolution is out of scope for this spec — the entity resolver populates `cin` before the data reaches the diff engine.

### Transaction boundaries
- Each `_fire_event` call uses the same `conn` acquired once per `process_source` call.
- `_update_source_state` runs after all events are fired. If event insertion fails partially, the source state still updates (events are best-effort; blocking the state update would cause reprocessing on the next pull, which is worse than a missed event).

### No pre-generated AI summaries
- `data_json` stored in `events` is raw structured data only. AI summary is generated at alert delivery time, not here.

### Health score
- The diff engine does not compute health scores. After `_fire_event` returns, the health scorer (separate module) subscribes to new event inserts via a Postgres LISTEN/NOTIFY channel and recomputes on event only.

---

## Dependency install

```
pip install asyncpg pytest pytest-asyncio
```

No additional dependencies. The diff engine uses only the standard library (`hashlib`, `json`, `time`, `logging`) plus `asyncpg` for DB access.

---

## What Codex must NOT do

- Do not recreate any DB tables. Schema already exists.
- Do not add a scheduler or cron logic inside this module. Scheduling lives in the ingestion layer.
- Do not call the Claude API or any external API from within the diff engine.
- Do not write to any table other than `events` and `source_state`.
- Do not pre-generate alert summaries or trigger Telegram sends from here. Routing happens downstream.
