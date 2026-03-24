from __future__ import annotations

"""
NCLT detector — source_id: 'nclt'
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


FILING_TYPE_MAP = {
    "section 7": ("NCLT_SEC7_FILED", "CRITICAL"),
    "sec 7": ("NCLT_SEC7_FILED", "CRITICAL"),
    "s7": ("NCLT_SEC7_FILED", "CRITICAL"),
    "section 9": ("NCLT_SEC9_FILED", "CRITICAL"),
    "sec 9": ("NCLT_SEC9_FILED", "CRITICAL"),
    "s9": ("NCLT_SEC9_FILED", "CRITICAL"),
    "section 10": ("NCLT_SEC10_FILED", "ALERT"),
    "sec 10": ("NCLT_SEC10_FILED", "ALERT"),
    "s10": ("NCLT_SEC10_FILED", "ALERT"),
    "cirp admitted": ("CIRP_ADMITTED", "CRITICAL"),
    "admitted": ("CIRP_ADMITTED", "CRITICAL"),
    "liquidation ordered": ("LIQUIDATION_ORDERED", "CRITICAL"),
    "liquidation": ("LIQUIDATION_ORDERED", "CRITICAL"),
    "resolution approved": ("RESOLUTION_APPROVED", "WATCH"),
    "resolution plan": ("RESOLUTION_APPROVED", "WATCH"),
}


class NCLTDetector(BaseDetector):
    async def detect_events(
        self,
        old_records: List[dict],
        new_records: List[dict],
        db,
    ) -> List[EventSpec]:
        events: List[EventSpec] = []
        existing: Set[Tuple[str, str]] = await _fetch_existing_nclt(db)

        for record in new_records:
            cin = record.get("cin")
            case_number = str(record.get("case_number") or "").strip()

            if not cin or not case_number:
                continue
            if (cin, case_number) in existing:
                continue

            filing_type = str(record.get("filing_type") or "").lower().strip()
            event_type, severity = _map_filing_type(filing_type)

            events.append(
                EventSpec(
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
                )
            )
            existing.add((cin, case_number))

        return events


async def _fetch_existing_nclt(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch("SELECT cin, case_number FROM legal_events WHERE source = 'nclt'")
    return {(row["cin"], row["case_number"]) for row in rows}


def _map_filing_type(filing_type: str) -> Tuple[str, str]:
    for key, value in FILING_TYPE_MAP.items():
        if key in filing_type:
            return value
    return ("NCLT_SEC7_FILED", "CRITICAL")
