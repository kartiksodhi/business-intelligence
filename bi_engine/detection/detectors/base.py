from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
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
    severity: str
    data: dict
    source: str = ""


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
        db,
    ) -> List[EventSpec]:
        """
        Compare old vs new records, return list of EventSpec to fire.
        Must be idempotent: running twice on the same input must not produce
        duplicate EventSpec entries.
        """
        ...
