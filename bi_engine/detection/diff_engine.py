from __future__ import annotations

"""
DiffEngine — detection layer entry point.
"""

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
from .detectors.phase3 import (
    IBBIDetector,
    SEBIEnforcementDetector,
    SEBIBulkDealsDetector,
    RBIWilfulDefaulterDetector,
    RBINBFCDetector,
    GSTDetector,
    EPFODetector,
    MCAChargeDetector,
    ROCFilingsDetector,
    HighCourtDetector,
    SupremeCourtDetector,
    LabourCourtDetector,
    CCIDetector,
    DGFTDetector,
    RERADetector,
    MoEFDetector,
    PollutionControlDetector,
    CERSAIDetector,
    StateVATDetector,
    GenericHiringDetector,
    UdyamDetector,
    ESICDetector,
    GeMDetector,
    CPPPDetector,
)


logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = 4

DETECTOR_REGISTRY: Dict[str, Type[BaseDetector]] = {
    # Phase 2 — existing
    "mca_ogd": OGDDetector,
    "nclt": NCLTDetector,
    "drt": DRTDetector,
    "sarfaesi": SARFAESIDetector,
    "ecourts": ECourtsDetector,
    "mca_directors": DirectorDetector,
    # Phase 3 — legal & financial
    "ibbi": IBBIDetector,
    "sebi_enforcement_orders": SEBIEnforcementDetector,
    "sebi_bulk_block_deals": SEBIBulkDealsDetector,
    "rbi_wilful_defaulter": RBIWilfulDefaulterDetector,
    "rbi_nbfc_bank_notifications": RBINBFCDetector,
    "gst_portal": GSTDetector,
    "epfo": EPFODetector,
    "mca_charge_register": MCAChargeDetector,
    "roc_filings": ROCFilingsDetector,
    # Phase 3 — courts
    "high_court_commercial_division": HighCourtDetector,
    "supreme_court_cause_lists": SupremeCourtDetector,
    "labour_court_orders": LabourCourtDetector,
    # Phase 3 — regulatory
    "cci_filings": CCIDetector,
    "dgft": DGFTDetector,
    "rera": RERADetector,
    "moef_environment_clearance_portal": MoEFDetector,
    "pollution_control_boards": PollutionControlDetector,
    "cersai": CERSAIDetector,
    "state_vat_commercial_tax_portals": StateVATDetector,
    # Phase 3 — employment signals
    "udyam_registration_portal": UdyamDetector,
    "esic": ESICDetector,
    "gem": GeMDetector,
    "cppp": CPPPDetector,
}

# Hiring detectors are instantiated with source_id (not default-constructible)
_HIRING_SOURCES = ["naukri", "indeed_foundit", "glassdoor_india", "linkedin_indirect", "company_career_pages"]
for _src in _HIRING_SOURCES:
    DETECTOR_REGISTRY[_src] = GenericHiringDetector  # type: ignore[assignment]


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

    async def process_source(self, source_id: str, new_data: List[dict]) -> DiffResult:
        start = time.monotonic()
        errors: List[str] = []

        try:
            new_hash = await self._compute_hash(new_data)
            last_state = await self._load_last_state(source_id)
            last_hash = (last_state or {}).get("last_data_hash")

            if last_hash == new_hash:
                await self._update_source_state(source_id, new_hash, len(new_data), "OK")
                return DiffResult(
                    source_id=source_id,
                    records_processed=len(new_data),
                    events_fired=0,
                    hash_changed=False,
                    duration_ms=_elapsed_ms(start),
                )

            detector_cls = DETECTOR_REGISTRY.get(source_id)
            if detector_cls is None:
                raise ValueError(f"No detector registered for source_id='{source_id}'")

            try:
                detector = detector_cls()
            except TypeError:
                detector = detector_cls(source_id)
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
                            spec.cin, source_id, spec.event_type, spec.severity, spec.data, conn
                        )
                        events_fired += 1
                    except Exception as exc:
                        err = f"Failed to fire {spec.event_type} for cin={spec.cin}: {exc}"
                        logger.error(err)
                        errors.append(err)

            await self._update_source_state(source_id, new_hash, len(new_data), "OK")

            return DiffResult(
                source_id=source_id,
                records_processed=len(new_data),
                events_fired=events_fired,
                hash_changed=True,
                duration_ms=_elapsed_ms(start),
                errors=errors,
            )

        except Exception as exc:
            logger.exception("DiffEngine error for source_id='%s': %s", source_id, exc)
            errors.append(str(exc))
            await self._handle_source_failure(source_id)
            return DiffResult(
                source_id=source_id,
                records_processed=0,
                events_fired=0,
                hash_changed=False,
                duration_ms=_elapsed_ms(start),
                errors=errors,
            )

    async def _compute_hash(self, data: List[dict]) -> str:
        sorted_data = sorted(
            data,
            key=lambda record: (
                str(record.get("cin") or ""),
                str(record.get("case_number") or ""),
                json.dumps(record, sort_keys=True, default=str),
            ),
        )
        serialised = json.dumps(sorted_data, sort_keys=True, default=str)
        return hashlib.sha256(serialised.encode("utf-8")).hexdigest()

    async def _load_last_state(self, source_id: str) -> Optional[dict]:
        row = await self.db.fetchrow("SELECT * FROM source_state WHERE source_id = $1", source_id)
        return dict(row) if row else None

    async def _load_old_records(
        self, source_id: str, last_state: Optional[dict]
    ) -> List[dict]:
        append_only = {
            "nclt", "drt", "sarfaesi", "ecourts",
            # Phase 3 — all append-only (detect new records only)
            "ibbi", "sebi_enforcement_orders", "sebi_bulk_block_deals",
            "rbi_wilful_defaulter", "rbi_nbfc_bank_notifications",
            "gst_portal", "epfo", "mca_charge_register", "roc_filings",
            "high_court_commercial_division", "supreme_court_cause_lists",
            "labour_court_orders", "cci_filings", "dgft", "rera",
            "moef_environment_clearance_portal", "pollution_control_boards",
            "cersai", "state_vat_commercial_tax_portals",
            "udyam_registration_portal", "esic", "gem", "cppp",
            "naukri", "indeed_foundit", "glassdoor_india",
            "linkedin_indirect", "company_career_pages",
        }
        if source_id in append_only:
            return []

        if source_id == "mca_ogd":
            rows = await self.db.fetch(
                """
                SELECT cin, company_name, status, paid_up_capital,
                       date_of_last_agm, date_of_incorporation
                FROM master_entities
                """
            )
            return [dict(row) for row in rows]

        if source_id == "mca_directors":
            rows = await self.db.fetch(
                """
                SELECT din, cin, director_name, designation,
                       date_of_appointment, cessation_date
                FROM governance_graph
                """
            )
            return [dict(row) for row in rows]

        return []

    async def _fire_event(
        self,
        cin: Optional[str],
        source: str,
        event_type: str,
        severity: str,
        data: dict,
        conn,
    ) -> int:
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
                "Source '%s' has reached %s consecutive failures — firing SOURCE_UNREACHABLE",
                source_id,
                row["consecutive_failures"],
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
            except Exception as exc:
                logger.error("Could not fire SOURCE_UNREACHABLE alert: %s", exc)


def _elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
