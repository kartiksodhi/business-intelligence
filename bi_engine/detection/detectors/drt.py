from __future__ import annotations

"""
DRT detector — source_id: 'drt'
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
            event_type = "DRT_ORDER_PASSED" if "order" in record_type else "DRT_APPLICATION_FILED"

            events.append(
                EventSpec(
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
                )
            )
            existing.add((cin, case_number))

        return events


async def _fetch_existing_drt(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch("SELECT cin, case_number FROM legal_events WHERE source = 'drt'")
    return {(row["cin"], row["case_number"]) for row in rows}
