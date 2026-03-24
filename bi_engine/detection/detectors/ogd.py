from __future__ import annotations

"""
OGD detector — source_id: 'mca_ogd'

Compares old vs new master entity snapshots.
Records are keyed by CIN. Uses O(1) dict lookup.
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

        old_by_cin: dict = {record["cin"]: record for record in old_records if record.get("cin")}
        new_by_cin: dict = {record["cin"]: record for record in new_records if record.get("cin")}

        for cin, new_rec in new_by_cin.items():
            old_rec = old_by_cin.get(cin)

            if old_rec is None:
                events.append(
                    EventSpec(
                        cin=cin,
                        event_type="NEW_COMPANY",
                        severity="INFO",
                        data={
                            "company_name": new_rec.get("company_name"),
                            "status": new_rec.get("status"),
                            "registration_date": new_rec.get("date_of_registration"),
                        },
                    )
                )
                continue

            old_status = (old_rec.get("status") or "").strip()
            new_status = (new_rec.get("status") or "").strip()
            if old_status and new_status and old_status != new_status:
                events.append(
                    EventSpec(
                        cin=cin,
                        event_type="STATUS_CHANGE",
                        severity="ALERT",
                        data={
                            "company_name": new_rec.get("company_name"),
                            "old_status": old_status,
                            "new_status": new_status,
                        },
                    )
                )

            old_cap = _parse_capital(old_rec.get("paid_up_capital"))
            new_cap = _parse_capital(new_rec.get("paid_up_capital"))
            if old_cap is not None and new_cap is not None and old_cap > 0:
                ratio = abs(new_cap - old_cap) / old_cap
                if ratio > 0.50:
                    events.append(
                        EventSpec(
                            cin=cin,
                            event_type="CAPITAL_CHANGE",
                            severity="WATCH",
                            data={
                                "company_name": new_rec.get("company_name"),
                                "previous_paid_up_capital": old_cap,
                                "new_capital": new_cap,
                                "change_pct": round(ratio * 100, 2),
                            },
                        )
                    )

            if new_status == "Active":
                agm_overdue = _is_agm_overdue(new_rec.get("date_of_last_agm"))
                old_agm_overdue = _is_agm_overdue(old_rec.get("date_of_last_agm"))
                if agm_overdue and not old_agm_overdue:
                    events.append(
                        EventSpec(
                            cin=cin,
                            event_type="AGM_OVERDUE",
                            severity="WATCH",
                            data={
                                "company_name": new_rec.get("company_name"),
                                "date_of_last_agm": new_rec.get("date_of_last_agm"),
                            },
                        )
                    )

        for cin, old_rec in old_by_cin.items():
            if cin not in new_by_cin:
                old_status = (old_rec.get("status") or "").strip()
                if old_status == "Active":
                    events.append(
                        EventSpec(
                            cin=cin,
                            event_type="COMPANY_REMOVED",
                            severity="ALERT",
                            data={
                                "company_name": old_rec.get("company_name"),
                                "last_known_status": old_status,
                            },
                        )
                    )

        return events


def _parse_capital(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _is_agm_overdue(date_str) -> bool:
    if not date_str:
        return False
    try:
        agm_date = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        delta_days = (now - agm_date).days
        return delta_days > 548
    except (ValueError, TypeError):
        return False
