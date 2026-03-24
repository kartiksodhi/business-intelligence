from __future__ import annotations

"""
SARFAESI detector — source_id: 'sarfaesi'
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


STAGE_MAP = {
    "13(2)": ("SARFAESI_DEMAND_NOTICE", "ALERT"),
    "section 13(2)": ("SARFAESI_DEMAND_NOTICE", "ALERT"),
    "demand notice": ("SARFAESI_DEMAND_NOTICE", "ALERT"),
    "13(4)": ("SARFAESI_POSSESSION_TAKEN", "CRITICAL"),
    "section 13(4)": ("SARFAESI_POSSESSION_TAKEN", "CRITICAL"),
    "possession": ("SARFAESI_POSSESSION_TAKEN", "CRITICAL"),
    "auction scheduled": ("SARFAESI_AUCTION_SCHEDULED", "CRITICAL"),
    "auction notice": ("SARFAESI_AUCTION_SCHEDULED", "CRITICAL"),
    "auction completed": ("SARFAESI_AUCTION_COMPLETED", "ALERT"),
    "auction sold": ("SARFAESI_AUCTION_COMPLETED", "ALERT"),
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
            events.append(
                EventSpec(
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
                )
            )
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
    return {(row["cin"], row["case_number"], (row["notice_stage"] or "").lower()) for row in rows}


def _map_stage(notice_stage: str) -> Tuple[str, str]:
    normalised = notice_stage.lower().strip()
    for key, value in STAGE_MAP.items():
        if key in normalised:
            return value
    return ("SARFAESI_DEMAND_NOTICE", "ALERT")
