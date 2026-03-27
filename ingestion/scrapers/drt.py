from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import List

import requests
from playwright.async_api import async_playwright

from .base_scraper import BaseScraper, RawCase

logger = logging.getLogger(__name__)

DRT_URL = "https://drt.gov.in"
DRT_API_BASE = "https://drt.gov.in/drtapi"

DRT_BENCHES = [
    "Mumbai I",
    "Mumbai II",
    "Delhi",
    "Chennai",
    "Kolkata",
    "Ahmedabad",
    "Hyderabad",
    "Bengaluru",
    "Pune",
    "Jaipur",
    "Chandigarh",
    "Allahabad",
    "Nagpur",
    "Coimbatore",
    "Ernakulum",
    "Dehradun",
    "Patna",
    "Guwahati",
    "Vishakhapatnam",
    "Jabalpur",
    "Ranchi",
    "Siliguri",
    "Cuttack",
]

BANK_SEARCH_TERMS = [
    "STATE BANK OF INDIA",
    "PUNJAB NATIONAL BANK",
    "BANK OF BARODA",
    "CANARA BANK",
    "UNION BANK OF INDIA",
    "HDFC BANK",
    "ICICI BANK",
    "AXIS BANK",
]


class DRTScraper(BaseScraper):
    source_id = "drt"
    cadence_hours = 24

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        await self._ensure_live_route()
        scheme_rows = self._fetch_schemes()
        if not scheme_rows:
            raise RuntimeError("unable to load DRT benches from live portal")

        scheme_map = {row["schemeNameDrtId"]: row["SchemaName"] for row in scheme_rows}
        run_benches = self._benches_for_this_run()
        matching_scheme_ids = [
            scheme_id
            for scheme_id, scheme_name in scheme_map.items()
            if self._matches_bench_rotation(scheme_name, run_benches)
        ]

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"})
        cases: list[RawCase] = []
        seen_case_keys: set[str] = set()

        for scheme_id in matching_scheme_ids:
            bench_name = scheme_map[scheme_id]
            for term in BANK_SEARCH_TERMS:
                try:
                    listing = self._search_party_name(session, scheme_id, term)
                except Exception as exc:
                    logger.warning("drt: listing failed for bench=%s term=%s: %s", bench_name, term, exc)
                    continue

                for row in listing:
                    filing_date = self._parse_date(row.get("dateoffiling") or "")
                    filing_no = row.get("filingNo")
                    if not filing_date or filing_date < since or not filing_no:
                        continue
                    case_key = f"{scheme_id}:{filing_no}"
                    if case_key in seen_case_keys:
                        continue

                    detail = self._fetch_case_detail(session, scheme_id, filing_no)
                    respondent = self._pick_party_name(detail, row, "respondent")
                    applicant = self._pick_party_name(detail, row, "petitioner")
                    if not respondent or not applicant or not self._looks_like_bank(applicant):
                        continue

                    seen_case_keys.add(case_key)
                    next_hearing = self._parse_date(detail.get("nextlistingdate") or "")
                    cases.append(
                        RawCase(
                            source="drt",
                            case_number=self._format_case_number(detail or row),
                            case_type="DRT",
                            court=self._format_court_name(bench_name, detail),
                            filing_date=filing_date,
                            respondent_name=respondent,
                            petitioner_name=applicant,
                            status=detail.get("casestatus") or row.get("casestatus") or "Filed",
                            amount_involved=self._parse_amount(row.get("amount") or detail.get("amount")),
                            raw_data={
                                "scheme_id": scheme_id,
                                "bench": bench_name,
                                "case_type": detail.get("casetype") or row.get("casetype"),
                                "case_no": detail.get("caseno") or row.get("caseno"),
                                "case_year": detail.get("caseyear") or row.get("caseyear"),
                                "diary_no": detail.get("diaryno") or row.get("diaryno"),
                                "applicant": applicant,
                                "respondent": respondent,
                                "filing_date": filing_date.isoformat(),
                                "next_hearing_date": next_hearing.isoformat() if next_hearing else None,
                                "tribunal_bench": bench_name,
                                "court_no": detail.get("courtNo"),
                                "court_name": detail.get("courtName"),
                                "purpose": detail.get("nextListingPurpose"),
                                "filing_no": filing_no,
                            },
                        )
                    )

        logger.info("drt: collected %d recent cases from %d benches", len(cases), len(matching_scheme_ids))
        return cases

    async def _ensure_live_route(self) -> None:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(DRT_URL, wait_until="networkidle", timeout=30000)
            await page.goto(f"{DRT_URL}/#/casedetail", wait_until="networkidle", timeout=30000)
            if not await page.get_by_text("Case Details", exact=False).count():
                await browser.close()
                raise RuntimeError("drt SPA route did not load")
            await browser.close()

    def _fetch_schemes(self) -> list[dict]:
        response = requests.post(f"{DRT_API_BASE}/getDrtDratScheamName", timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return []
        return [
            row
            for row in payload
            if isinstance(row, dict)
            and str(row.get("schemeNameDrtId") or "").isdigit()
            and int(str(row.get("schemeNameDrtId"))) < 100
        ]

    def _matches_bench_rotation(self, scheme_name: str, run_benches: list[str]) -> bool:
        normalized = scheme_name.lower().replace(" ", "")
        aliases = {
            "mumbaii": ("mumbai(drt1)", "mumbai(drt 1)"),
            "mumbaiii": ("mumbai(drt2)", "mumbai(drt 2)"),
            "delhi": ("delhi(drt1)", "delhi(drt2)", "delhi(drt3)"),
            "bengaluru": ("bangalore(drt1)", "bangalore(drt2)", "bengaluru"),
            "ernakulum": ("ernakulam",),
            "vishakhapatnam": ("vishakhapatnam", "visakhapatnam"),
        }
        for bench in run_benches:
            bench_key = bench.lower().replace(" ", "")
            if bench_key in normalized:
                return True
            if any(alias in normalized for alias in aliases.get(bench_key, ())):
                return True
        return False

    def _search_party_name(self, session: requests.Session, scheme_id: str, party_name: str) -> list[dict]:
        response = session.post(
            f"{DRT_API_BASE}/casedetail_party_name_wise",
            data={"schemeNameDratDrtId": str(scheme_id), "partyName": party_name},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def _fetch_case_detail(self, session: requests.Session, scheme_id: str, filing_no: str) -> dict:
        response = session.post(
            f"{DRT_API_BASE}/getCaseDetailPartyWise",
            data={"filingNo": filing_no, "schemeNameDrtId": str(scheme_id)},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def _pick_party_name(self, detail: dict, listing: dict, role: str) -> str:
        if role == "petitioner":
            return (detail.get("petitionerName") or listing.get("applicant") or "").strip()
        return (detail.get("respondentName") or listing.get("respondent") or "").strip()

    def _looks_like_bank(self, applicant: str) -> bool:
        lowered = applicant.lower()
        return any(token in lowered for token in ("bank", "financial", "asset reconstruction", "nbfc", "finance"))

    def _format_case_number(self, row: dict) -> str:
        case_type = row.get("casetype") or "DRT"
        case_no = row.get("caseno") or "NA"
        case_year = row.get("caseyear") or row.get("diaryyear") or "NA"
        return f"{case_type}/{case_no}/{case_year}"

    def _format_court_name(self, bench_name: str, detail: dict) -> str:
        court_name = detail.get("courtName")
        court_no = detail.get("courtNo")
        if court_name and court_no:
            return f"{bench_name} - {court_name} {court_no}"
        return bench_name

    def _benches_for_this_run(self) -> List[str]:
        from ingestion.scrapers import _run_counter

        offset = (_run_counter.get(self.source_id, 0) * 5) % len(DRT_BENCHES)
        _run_counter[self.source_id] = _run_counter.get(self.source_id, 0) + 1
        return DRT_BENCHES[offset : offset + 5]

    def _severity_for_case_type(self, case_type: str) -> str:
        if case_type == "DRT":
            return "ALERT"
        return super()._severity_for_case_type(case_type)

    def _insert_event(self, case: RawCase, cin: str) -> int:
        row = self.db.execute(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            RETURNING id
            """,
            (cin, case.source, "DRT_CASE_FILED", "ALERT", json.dumps(case.raw_data)),
        ).fetchone()
        self.db.commit()
        return row[0]

    def _parse_date(self, raw: str):
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
            try:
                from datetime import datetime

                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_amount(self, raw: str):
        if not raw:
            return None
        raw = raw.replace(",", "").replace("₹", "").strip()
        cr = re.search(r"([\d.]+)\s*[Cc][Rr]", raw)
        if cr:
            return int(float(cr.group(1)) * 10_000_000)
        num = re.search(r"[\d.]+", raw)
        return int(float(num.group())) if num else None
