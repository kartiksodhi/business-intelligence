from __future__ import annotations

"""
Phase 3 detectors — generic append-only detectors for all Phase 3 sources.

Each source maps incoming records to (event_type, severity) based on source-
specific logic.  De-duplication uses (cin, case_number/record_id, source).
"""

from typing import Dict, List, Set, Tuple

from .base import BaseDetector, EventSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_existing_keys(db, source_id: str) -> Set[Tuple[str, str]]:
    """Return set of (cin, case_number) already stored for this source."""
    rows = await db.fetch(
        "SELECT cin, case_number FROM legal_events WHERE source = $1",
        source_id,
    )
    return {(row["cin"], row["case_number"]) for row in rows}


def _record_id(record: dict) -> str:
    """Best-effort unique record identifier from scraped data."""
    return str(
        record.get("case_number")
        or record.get("record_id")
        or record.get("order_number")
        or record.get("filing_number")
        or record.get("id")
        or ""
    ).strip()


# ---------------------------------------------------------------------------
# IBBI — Insolvency & Bankruptcy Board of India
# ---------------------------------------------------------------------------

IBBI_TYPE_MAP = {
    "cirp": ("IBBI_CIRP_ADMITTED", "CRITICAL"),
    "liquidation": ("IBBI_LIQUIDATION_ORDER", "CRITICAL"),
    "voluntary liquidation": ("IBBI_VOLUNTARY_LIQUIDATION", "ALERT"),
    "resolution plan approved": ("IBBI_RESOLUTION_APPROVED", "ALERT"),
    "moratorium": ("IBBI_MORATORIUM", "WATCH"),
}


class IBBIDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "ibbi")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            case_type = str(r.get("case_type") or "cirp").lower()
            event_type, severity = IBBI_TYPE_MAP.get(case_type, ("IBBI_CASE_FILED", "ALERT"))
            events.append(EventSpec(cin=cin, event_type=event_type, severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# SEBI — enforcement orders & bulk/block deals
# ---------------------------------------------------------------------------

class SEBIEnforcementDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "sebi_enforcement_orders")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="SEBI_ENFORCEMENT_ORDER", severity="CRITICAL", data=r,
            ))
            existing.add((cin, rid))
        return events


class SEBIBulkDealsDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "sebi_bulk_block_deals")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            severity = "ALERT" if float(r.get("quantity", 0) or 0) > 500000 else "WATCH"
            events.append(EventSpec(
                cin=cin, event_type="SEBI_BULK_BLOCK_DEAL", severity=severity, data=r,
            ))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# RBI — wilful defaulters & NBFC notifications
# ---------------------------------------------------------------------------

class RBIWilfulDefaulterDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "rbi_wilful_defaulter")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="RBI_WILFUL_DEFAULTER", severity="CRITICAL", data=r,
            ))
            existing.add((cin, rid))
        return events


class RBINBFCDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "rbi_nbfc_bank_notifications")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            action = str(r.get("action") or "").lower()
            if "cancel" in action or "revok" in action:
                severity = "CRITICAL"
                etype = "RBI_LICENSE_CANCELLED"
            elif "restrict" in action or "penalt" in action:
                severity = "ALERT"
                etype = "RBI_REGULATORY_ACTION"
            else:
                severity = "WATCH"
                etype = "RBI_NOTIFICATION"
            events.append(EventSpec(cin=cin, event_type=etype, severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# GST Portal
# ---------------------------------------------------------------------------

class GSTDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "gst_portal")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            status = str(r.get("gst_status") or "").lower()
            if "cancel" in status:
                severity, etype = "ALERT", "GST_CANCELLED"
            elif "suspend" in status:
                severity, etype = "ALERT", "GST_SUSPENDED"
            else:
                severity, etype = "INFO", "GST_STATUS_CHANGE"
            events.append(EventSpec(cin=cin, event_type=etype, severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# EPFO
# ---------------------------------------------------------------------------

class EPFODetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "epfo")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="EPFO_COMPLIANCE_ISSUE", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# MCA Charge Register
# ---------------------------------------------------------------------------

class MCAChargeDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "mca_charge_register")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            charge_status = str(r.get("charge_status") or "").lower()
            if "creat" in charge_status:
                etype, severity = "MCA_CHARGE_CREATED", "WATCH"
            elif "modif" in charge_status:
                etype, severity = "MCA_CHARGE_MODIFIED", "WATCH"
            elif "satisf" in charge_status:
                etype, severity = "MCA_CHARGE_SATISFIED", "INFO"
            else:
                etype, severity = "MCA_CHARGE_FILED", "WATCH"
            events.append(EventSpec(cin=cin, event_type=etype, severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# ROC Filings
# ---------------------------------------------------------------------------

class ROCFilingsDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "roc_filings")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            form = str(r.get("form_type") or "").upper()
            if form in ("MGT-7", "AOC-4"):
                severity = "INFO"
            elif form in ("STK-2", "STK-7"):
                severity = "ALERT"
            else:
                severity = "WATCH"
            events.append(EventSpec(cin=cin, event_type=f"ROC_FILING_{form or 'OTHER'}", severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# Courts — High Court, Supreme Court, Labour Court
# ---------------------------------------------------------------------------

class HighCourtDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "high_court_commercial_division")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="HIGH_COURT_CASE", severity="ALERT", data=r,
            ))
            existing.add((cin, rid))
        return events


class SupremeCourtDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "supreme_court_cause_lists")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="SUPREME_COURT_CASE", severity="CRITICAL", data=r,
            ))
            existing.add((cin, rid))
        return events


class LabourCourtDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "labour_court_orders")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="LABOUR_COURT_ORDER", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# Regulatory — CCI, DGFT, RERA, MoEF, Pollution, CERSAI, State VAT
# ---------------------------------------------------------------------------

class CCIDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "cci_filings")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="CCI_FILING", severity="ALERT", data=r,
            ))
            existing.add((cin, rid))
        return events


class DGFTDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "dgft")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="DGFT_TRADE_NOTICE", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


class RERADetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "rera")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="RERA_COMPLAINT", severity="ALERT", data=r,
            ))
            existing.add((cin, rid))
        return events


class MoEFDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "moef_environment_clearance_portal")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="MOEF_CLEARANCE", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


class PollutionControlDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "pollution_control_boards")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            action = str(r.get("action") or "").lower()
            severity = "ALERT" if "closure" in action or "revok" in action else "WATCH"
            events.append(EventSpec(
                cin=cin, event_type="POLLUTION_CONTROL_ACTION", severity=severity, data=r,
            ))
            existing.add((cin, rid))
        return events


class CERSAIDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "cersai")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="CERSAI_SECURITY_INTEREST", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


class StateVATDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "state_vat_commercial_tax_portals")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="VAT_TAX_ACTION", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# Employment signal sources — hiring/layoff detection
# ---------------------------------------------------------------------------

class GenericHiringDetector(BaseDetector):
    """Used for Naukri, Indeed, Glassdoor, LinkedIn, Career Pages."""

    def __init__(self, source_id: str):
        self._source_id = source_id

    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, self._source_id)
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            count = int(r.get("job_count") or r.get("posting_count") or 0)
            if count >= 50:
                severity, etype = "WATCH", "MASS_HIRING_SIGNAL"
            elif count == 0:
                severity, etype = "WATCH", "HIRING_FREEZE_SIGNAL"
            else:
                severity, etype = "INFO", "HIRING_ACTIVITY"
            events.append(EventSpec(cin=cin, event_type=etype, severity=severity, data=r))
            existing.add((cin, rid))
        return events


# ---------------------------------------------------------------------------
# Udyam & ESIC & GeM & CPPP
# ---------------------------------------------------------------------------

class UdyamDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "udyam_registration_portal")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="UDYAM_REGISTRATION_CHANGE", severity="INFO", data=r,
            ))
            existing.add((cin, rid))
        return events


class ESICDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "esic")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="ESIC_COMPLIANCE_ISSUE", severity="WATCH", data=r,
            ))
            existing.add((cin, rid))
        return events


class GeMDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "gem")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            action = str(r.get("action") or "").lower()
            if "debar" in action or "blacklist" in action:
                severity = "CRITICAL"
            else:
                severity = "WATCH"
            events.append(EventSpec(
                cin=cin, event_type="GEM_PROCUREMENT_EVENT", severity=severity, data=r,
            ))
            existing.add((cin, rid))
        return events


class CPPPDetector(BaseDetector):
    async def detect_events(self, old_records, new_records, db) -> List[EventSpec]:
        existing = await _fetch_existing_keys(db, "cppp")
        events: List[EventSpec] = []
        for r in new_records:
            cin = r.get("cin")
            rid = _record_id(r)
            if not cin or not rid or (cin, rid) in existing:
                continue
            events.append(EventSpec(
                cin=cin, event_type="CPPP_TENDER_EVENT", severity="INFO", data=r,
            ))
            existing.add((cin, rid))
        return events
