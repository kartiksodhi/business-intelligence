from __future__ import annotations

"""
e-Courts detector — source_id: 'ecourts'
"""

from typing import List, Set, Tuple

from .base import BaseDetector, EventSpec


SEC138_THRESHOLD = 3
CIVIL_SUIT_AMOUNT_THRESHOLD = 10_000_000


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

            if disposal_status in ("disposed", "decided", "closed"):
                events.append(
                    EventSpec(
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
                    )
                )
                existing.add((cin, case_number))
                continue

            if _is_sec138(case_type):
                count = await _count_sec138(db, cin)
                count += 1

                severity = "CRITICAL" if count >= SEC138_THRESHOLD else "ALERT"
                event_type = "SEC138_MULTIPLE" if count >= SEC138_THRESHOLD else "SEC138_FILED"

                events.append(
                    EventSpec(
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
                    )
                )
                existing.add((cin, case_number))
                continue

            claim_amount = _parse_amount(record.get("claim_amount"))
            if claim_amount and claim_amount > CIVIL_SUIT_AMOUNT_THRESHOLD:
                events.append(
                    EventSpec(
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
                    )
                )
                existing.add((cin, case_number))

        return events


async def _fetch_existing_ecourts(db) -> Set[Tuple[str, str]]:
    rows = await db.fetch("SELECT cin, case_number FROM legal_events WHERE source = 'ecourts'")
    return {(row["cin"], row["case_number"]) for row in rows}


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
