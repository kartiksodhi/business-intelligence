from __future__ import annotations

"""
Director detector — source_id: 'mca_directors'
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

        old_by_key: dict = {
            _edge_key(record): record
            for record in old_records
            if record.get("din") and record.get("cin")
        }
        new_by_key: dict = {
            _edge_key(record): record
            for record in new_records
            if record.get("din") and record.get("cin")
        }
        overload_emitted: Set[str] = set()

        for key, new_rec in new_by_key.items():
            old_rec = old_by_key.get(key)
            din = new_rec["din"]
            cin = new_rec["cin"]

            if old_rec is None:
                events.append(
                    EventSpec(
                        cin=cin,
                        event_type="DIRECTOR_APPOINTED",
                        severity="INFO",
                        data={
                            "din": din,
                            "name": new_rec.get("director_name"),
                            "designation": new_rec.get("designation"),
                            "appointment_date": new_rec.get("date_of_appointment"),
                        },
                    )
                )
            else:
                old_cessation = old_rec.get("cessation_date")
                new_cessation = new_rec.get("cessation_date")

                if not old_cessation and new_cessation:
                    role = str(new_rec.get("designation") or "").lower()
                    severity = "ALERT" if any(risk_role in role for risk_role in HIGH_RISK_ROLES) else "WATCH"
                    events.append(
                        EventSpec(
                            cin=cin,
                            event_type="DIRECTOR_RESIGNED",
                            severity=severity,
                            data={
                                "din": din,
                                "name": new_rec.get("director_name"),
                                "designation": new_rec.get("designation"),
                                "cessation_date": new_cessation,
                            },
                        )
                    )

            if din not in overload_emitted:
                board_count = await _count_boards(db, din)
                if board_count >= OVERLOAD_THRESHOLD:
                    overload_emitted.add(din)
                    events.append(
                        EventSpec(
                            cin=cin,
                            event_type="DIRECTOR_OVERLOADED",
                            severity="WATCH",
                            data={
                                "din": din,
                                "name": new_rec.get("director_name"),
                                "board_count": board_count,
                            },
                        )
                    )

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
