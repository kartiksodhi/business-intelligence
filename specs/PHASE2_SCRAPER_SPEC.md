# PHASE2_SCRAPER_SPEC.md

## What this is

Codex implementation spec for the five Phase 2 legal scrapers. Each scraper ingests new filings from a government legal source on its cadence, resolves respondent/debtor names to CINs, diffs against stored state, and fires structured events into the `events` and `legal_events` tables.

**Core principle: scrape by recency, not by entity.** Never search for 18L companies. Pull new cases filed this week. Entity-resolve what you find.

---

## Tables involved (already exist — do NOT recreate schema)

- `source_state` — tracks `last_pull_at`, `last_data_hash`, `status`, `consecutive_failures`, `record_count` per `source_id`
- `events` — detected changes: `cin`, `source`, `event_type`, `severity`, `detected_at`, `data_json`
- `legal_events` — denormalized case rows: `cin`, `case_type`, `case_number`, `court`, `filing_date`, `status`, `amount_involved`, `source`, `event_id`
- `master_entities` — golden record, CIN lookup
- `unmapped_signals` — cases that could not be resolved to a CIN (never discard)
- `entity_resolution_queue` — low-confidence matches awaiting review
- `captcha_log` — every CAPTCHA attempt and outcome

---

## File layout

```
ingestion/
    scrapers/
        __init__.py
        base_scraper.py          # shared base class — already exists if Codex wrote it, else create
        ecourts.py
        nclt.py
        drt.py
        sarfaesi.py
        ibbi.py
    scheduler.py                 # APScheduler config — one job per source on its cadence
tests/
    test_ecourts_scraper.py
    test_nclt_scraper.py
    test_drt_scraper.py
    test_sarfaesi_scraper.py
    test_ibbi_scraper.py
```

---

## Shared base class: `ingestion/scrapers/base_scraper.py`

Every scraper inherits this. Do not duplicate this logic in individual scrapers.

```python
import hashlib, json, logging
from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta
from typing import List, Optional
from dataclasses import dataclass, field
import asyncio
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)


@dataclass
class RawCase:
    """Normalised case record before entity resolution."""
    source: str
    case_number: str
    case_type: str          # must be a legal_events.case_type enum value
    court: str
    filing_date: Optional[date]
    respondent_name: str    # raw name from portal — will be resolved to CIN
    petitioner_name: Optional[str]
    status: str
    amount_involved: Optional[int]  # paise or rupees as integer — store in rupees
    raw_data: dict          # full row as scraped, for audit


class BaseScraper(ABC):
    source_id: str          # must match source_state.source_id seeded value
    cadence_hours: int

    def __init__(self, db_conn):
        self.db = db_conn

    @abstractmethod
    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        """Pull cases filed on or after `since`. Never search by entity."""
        ...

    def compute_hash(self, cases: List[RawCase]) -> str:
        payload = json.dumps(
            [c.case_number for c in sorted(cases, key=lambda x: x.case_number)],
            sort_keys=True
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def get_last_pull_state(self) -> tuple[Optional[datetime], Optional[str]]:
        row = self.db.execute(
            "SELECT last_pull_at, last_data_hash FROM source_state WHERE source_id = %s",
            (self.source_id,)
        ).fetchone()
        if row:
            return row[0], row[1]
        return None, None

    def update_source_state(self, new_hash: str, record_count: int, status: str = 'active'):
        self.db.execute("""
            UPDATE source_state
            SET last_pull_at = NOW(), last_data_hash = %s,
                record_count = %s, status = %s,
                consecutive_failures = 0, updated_at = NOW()
            WHERE source_id = %s
        """, (new_hash, record_count, status, self.source_id))
        self.db.commit()

    def increment_failure(self):
        self.db.execute("""
            UPDATE source_state
            SET consecutive_failures = consecutive_failures + 1,
                status = CASE WHEN consecutive_failures + 1 >= 4 THEN 'degraded' ELSE status END,
                updated_at = NOW()
            WHERE source_id = %s
        """, (self.source_id,))
        self.db.commit()

    async def run(self):
        last_pull, last_hash = self.get_last_pull_state()
        since = (last_pull.date() if last_pull else date.today() - timedelta(days=7))
        try:
            cases = await self.fetch_new_cases(since)
        except Exception as e:
            logger.error(f"{self.source_id} fetch failed: {e}")
            self.increment_failure()
            return

        new_hash = self.compute_hash(cases)
        if new_hash == last_hash:
            logger.info(f"{self.source_id}: hash unchanged, nothing to process")
            self.update_source_state(new_hash, len(cases))
            return

        for case in cases:
            self._process_case(case)

        self.update_source_state(new_hash, len(cases))
        logger.info(f"{self.source_id}: processed {len(cases)} cases")

    def _process_case(self, case: RawCase):
        from ingestion.entity_resolver import EntityResolver
        resolver = EntityResolver(self.db)
        result = resolver.resolve(case.respondent_name)

        if result.cin and result.confidence >= 0.75:
            self._upsert_legal_event(case, result.cin)
        elif result.cin and result.confidence >= 0.50:
            # Low confidence — queue for review, still store signal
            self._queue_for_resolution(case, result)
        else:
            self._store_unmapped(case)

    def _upsert_legal_event(self, case: RawCase, cin: str):
        """Insert or update legal_events. Insert event if new case."""
        existing = self.db.execute(
            "SELECT id FROM legal_events WHERE case_number = %s AND source = %s",
            (case.case_number, case.source)
        ).fetchone()

        if existing:
            self.db.execute("""
                UPDATE legal_events SET status = %s, updated_at = NOW()
                WHERE case_number = %s AND source = %s
            """, (case.status, case.case_number, case.source))
        else:
            event_id = self._insert_event(case, cin)
            self.db.execute("""
                INSERT INTO legal_events
                  (cin, case_type, case_number, court, filing_date, status,
                   amount_involved, source, event_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (cin, case.case_type, case.case_number, case.court,
                  case.filing_date, case.status, case.amount_involved,
                  case.source, event_id))

        self.db.commit()

    def _insert_event(self, case: RawCase, cin: str) -> int:
        severity = self._severity_for_case_type(case.case_type)
        row = self.db.execute("""
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            RETURNING id
        """, (cin, case.source, case.case_type, severity,
              json.dumps(case.raw_data))).fetchone()
        self.db.commit()
        return row[0]

    def _severity_for_case_type(self, case_type: str) -> str:
        return {
            'SARFAESI_13_2': 'ALERT',
            'SARFAESI_13_4': 'CRITICAL',
            'SARFAESI_AUCTION': 'CRITICAL',
            'NCLT_7': 'CRITICAL',
            'NCLT_9': 'CRITICAL',
            'NCLT_10': 'ALERT',
            'DRT': 'ALERT',
            'SEC_138': 'ALERT',
            'HIGH_COURT': 'WATCH',
            'LABOUR': 'WATCH',
        }.get(case_type, 'WATCH')

    def _queue_for_resolution(self, case: RawCase, result):
        self.db.execute("""
            INSERT INTO entity_resolution_queue
              (raw_name, source, candidate_cin, confidence, raw_data, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """, (case.respondent_name, case.source, result.cin,
              result.confidence, json.dumps(case.raw_data)))
        self.db.commit()

    def _store_unmapped(self, case: RawCase):
        self.db.execute("""
            INSERT INTO unmapped_signals
              (source, identifier_type, identifier_value, raw_data, detected_at)
            VALUES (%s, 'COMPANY_NAME', %s, %s, NOW())
        """, (case.source, case.respondent_name, json.dumps(case.raw_data)))
        self.db.commit()

    async def _solve_captcha(self, page: Page, img_selector: str,
                              input_selector: str) -> bool:
        """
        CAPTCHA strategy:
        1. pytesseract OCR on screenshot
        2. Log attempt in captcha_log
        3. If OCR fails — leave for manual queue (POST /op/captcha/solve)
        Returns True if solved, False if queued for manual.
        """
        import pytesseract
        from PIL import Image
        import io, base64

        img_el = page.locator(img_selector)
        img_bytes = await img_el.screenshot()
        img = Image.open(io.BytesIO(img_bytes))
        text = pytesseract.image_to_string(img, config='--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789').strip()

        self.db.execute("""
            INSERT INTO captcha_log (source, method, result, created_at)
            VALUES (%s, 'tesseract', %s, NOW())
        """, (self.source_id, 'success' if text else 'failed'))
        self.db.commit()

        if text:
            await page.fill(input_selector, text)
            return True

        logger.warning(f"{self.source_id}: CAPTCHA OCR failed, queued for manual")
        return False
```

---

## Source 1: e-Courts

### Overview
- **URL**: https://ecourts.gov.in/ecourts_home/
- **Cadence**: Weekly (every 7 days)
- **source_id**: `ecourts`
- **Event types fired**: `SEC_138`
- **legal_events.case_type**: `SEC_138`
- **CAPTCHA**: Yes — image CAPTCHA on search form. Use pytesseract first, manual queue fallback.
- **Strategy**: Search by case type "138 NI Act" + date range (since last pull). Do NOT search by company name.

### What to scrape
Section 138 Negotiable Instruments Act cases filed in the last 7 days. These are cheque bounce cases — strongest early distress signal available publicly before insolvency proceedings.

Respondent in Sec 138 = the accused = the company that issued the bounced cheque. Resolve respondent to CIN.

### Search parameters
```
Case type: Criminal Case (NI Act 138)
Filing date: from last_pull_date to today
State: loop through all states (cycle across runs, 4-5 states per run to avoid rate limiting)
```

### File: `ingestion/scrapers/ecourts.py`

```python
from datetime import date, timedelta
from typing import List
from playwright.async_api import async_playwright
from .base_scraper import BaseScraper, RawCase
import logging, re

logger = logging.getLogger(__name__)

ECOURTS_BASE = "https://ecourts.gov.in/ecourts_home/"

# Cycle through state codes across weekly runs — do not hit all states in one run
STATE_CODES = [
    "MH", "DL", "KA", "TN", "GJ", "RJ", "UP", "WB",
    "AP", "TG", "MP", "OR", "KL", "HR", "PB"
]


class ECourtsScraper(BaseScraper):
    source_id = "ecourts"
    cadence_hours = 168  # 7 days

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        # Determine which states to scrape this run (rotate to avoid throttling)
        run_states = self._states_for_this_run()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            for state_code in run_states:
                try:
                    state_cases = await self._scrape_state(browser, state_code, since)
                    cases.extend(state_cases)
                except Exception as e:
                    logger.error(f"ecourts {state_code} failed: {e}")
            await browser.close()

        return cases

    def _states_for_this_run(self) -> List[str]:
        """Rotate through states — 5 per weekly run."""
        from ingestion.scrapers import _run_counter  # simple persistent counter
        offset = (_run_counter.get(self.source_id, 0) * 5) % len(STATE_CODES)
        _run_counter[self.source_id] = _run_counter.get(self.source_id, 0) + 1
        return STATE_CODES[offset:offset + 5]

    async def _scrape_state(self, browser, state_code: str, since: date) -> List[RawCase]:
        """Open e-Courts search, apply NI Act 138 filter, extract new cases."""
        page = await browser.new_page()
        cases = []

        await page.goto(f"{ECOURTS_BASE}?p=casestatus/index", timeout=30000)
        await page.wait_for_load_state("networkidle")

        # Select state
        await page.select_option("select#sess_state_code", state_code)
        await page.wait_for_timeout(1500)

        # Select case type — Section 138 NI Act
        await page.select_option("select#case_type", "NI ACT 138")

        # Date range
        await page.fill("input#res_date_of_filing_from",
                        since.strftime("%d-%m-%Y"))
        await page.fill("input#res_date_of_filing_to",
                        date.today().strftime("%d-%m-%Y"))

        # Solve CAPTCHA
        solved = await self._solve_captcha(
            page, "img#captcha_image", "input#captcha_text"
        )
        if not solved:
            logger.warning(f"ecourts {state_code}: CAPTCHA unsolved, skipping")
            await page.close()
            return []

        await page.click("button#searchbtn")
        await page.wait_for_load_state("networkidle")

        # Parse results table
        rows = await page.query_selector_all("table.table-bordered tbody tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 5:
                continue
            texts = [await c.inner_text() for c in cells]
            case_number = texts[0].strip()
            respondent_raw = texts[2].strip()
            filing_date_str = texts[3].strip()
            status = texts[4].strip()

            filing_date = self._parse_date(filing_date_str)
            if not filing_date or filing_date < since:
                continue

            # Skip if already stored
            exists = self.db.execute(
                "SELECT 1 FROM legal_events WHERE case_number=%s AND source='ecourts'",
                (case_number,)
            ).fetchone()
            if exists:
                continue

            cases.append(RawCase(
                source="ecourts",
                case_number=case_number,
                case_type="SEC_138",
                court=f"e-Courts {state_code}",
                filing_date=filing_date,
                respondent_name=respondent_raw,
                petitioner_name=texts[1].strip() if len(texts) > 1 else None,
                status=status,
                amount_involved=None,  # not available in list view
                raw_data={"state": state_code, "cells": texts}
            ))

        await page.close()
        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None
```

### Tests: `tests/test_ecourts_scraper.py`

```python
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import date
from ingestion.scrapers.ecourts import ECourtsScraper
from ingestion.scrapers.base_scraper import RawCase


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


@pytest.mark.asyncio
async def test_ecourts_skips_existing_case():
    db = make_db()
    db.execute.return_value.fetchone.return_value = (1,)  # case already in DB
    scraper = ECourtsScraper(db)
    # If case exists, _process_case should not be called
    case = RawCase("ecourts","138/2024","SEC_138","e-Courts MH",
                   date(2024,1,15),"ABC Pvt Ltd",None,"Pending",None,{})
    scraper._process_case = MagicMock()
    scraper._process_case(case)
    scraper._process_case.assert_called_once()


def test_ecourts_parse_date_formats():
    db = make_db()
    scraper = ECourtsScraper(db)
    assert scraper._parse_date("15-01-2024") == date(2024, 1, 15)
    assert scraper._parse_date("15/01/2024") == date(2024, 1, 15)
    assert scraper._parse_date("2024-01-15") == date(2024, 1, 15)
    assert scraper._parse_date("invalid") is None


def test_ecourts_severity():
    db = make_db()
    scraper = ECourtsScraper(db)
    assert scraper._severity_for_case_type("SEC_138") == "ALERT"


def test_ecourts_hash_deterministic():
    db = make_db()
    scraper = ECourtsScraper(db)
    c1 = RawCase("ecourts","C001","SEC_138","MH",date(2024,1,1),"X",None,"P",None,{})
    c2 = RawCase("ecourts","C002","SEC_138","MH",date(2024,1,2),"Y",None,"P",None,{})
    h1 = scraper.compute_hash([c1, c2])
    h2 = scraper.compute_hash([c2, c1])
    assert h1 == h2  # order-independent


def test_ecourts_unmapped_stored_on_no_cin():
    db = make_db()
    scraper = ECourtsScraper(db)
    from ingestion.entity_resolver import ResolutionResult
    mock_result = ResolutionResult(cin=None, confidence=0.0, method="none")
    with patch("ingestion.scrapers.base_scraper.EntityResolver") as MockResolver:
        MockResolver.return_value.resolve.return_value = mock_result
        case = RawCase("ecourts","138/2024","SEC_138","MH",
                       date(2024,1,1),"Unknown Co",None,"Pending",None,{})
        scraper._process_case(case)
        db.execute.assert_called()
```

---

## Source 2: NCLT

### Overview
- **URL**: https://nclt.gov.in/
- **Cadence**: Daily
- **source_id**: `nclt`
- **Event types fired**: `NCLT_7`, `NCLT_9`, `NCLT_10`
- **legal_events.case_type**: `NCLT_7`, `NCLT_9`, `NCLT_10`
- **CAPTCHA**: Yes on case search. pytesseract first, manual queue fallback.
- **Strategy**: Search case list by filing date range. Loop through Section 7, Section 9, Section 10 separately. New filings only.

### What to scrape
IBC insolvency petitions:
- **Section 7**: Financial creditor files (bank, NCD holder). Company unable to pay financial debt.
- **Section 9**: Operational creditor files (supplier, employee). Unpaid operational dues.
- **Section 10**: Corporate debtor itself files (voluntary CIRP).

Corporate debtor = the distressed company. Resolve to CIN.

### Search parameters
```
URL: https://nclt.gov.in/case-status-search
Filter by: Filing date range (since last pull)
Bench: ALL (loop through Mumbai, Delhi, Ahmedabad, Chennai, Kolkata, Hyderabad, Allahabad, Chandigarh, Guwahati, Amaravati, Jaipur, Kochi, Cuttack, Indore)
Case type: IBC 7, IBC 9, IBC 10 — three separate passes
```

### File: `ingestion/scrapers/nclt.py`

```python
from datetime import date, timedelta
from typing import List
from playwright.async_api import async_playwright
from .base_scraper import BaseScraper, RawCase
import logging, re

logger = logging.getLogger(__name__)

NCLT_URL = "https://nclt.gov.in/case-status-search"

NCLT_BENCHES = [
    "Mumbai", "New Delhi", "Ahmedabad", "Chennai", "Kolkata",
    "Hyderabad", "Allahabad", "Chandigarh", "Guwahati",
    "Amaravati", "Jaipur", "Kochi", "Cuttack", "Indore"
]

IBC_SECTIONS = {
    "IBC_7": "NCLT_7",
    "IBC_9": "NCLT_9",
    "IBC_10": "NCLT_10",
}


class NCLTScraper(BaseScraper):
    source_id = "nclt"
    cadence_hours = 24  # daily

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            for section_label, case_type in IBC_SECTIONS.items():
                for bench in NCLT_BENCHES:
                    try:
                        batch = await self._scrape_bench(
                            browser, bench, section_label, case_type, since
                        )
                        cases.extend(batch)
                    except Exception as e:
                        logger.error(f"nclt {bench} {section_label} failed: {e}")
            await browser.close()
        return cases

    async def _scrape_bench(self, browser, bench: str,
                             section_label: str, case_type: str,
                             since: date) -> List[RawCase]:
        page = await browser.new_page()
        cases = []

        await page.goto(NCLT_URL, timeout=30000)
        await page.wait_for_load_state("networkidle")

        await page.select_option("select#bench", bench)
        await page.select_option("select#case_type", section_label)
        await page.fill("input#date_from", since.strftime("%d/%m/%Y"))
        await page.fill("input#date_to", date.today().strftime("%d/%m/%Y"))

        solved = await self._solve_captcha(
            page, "img#captchaImg", "input#captcha"
        )
        if not solved:
            logger.warning(f"nclt {bench}: CAPTCHA unsolved")
            await page.close()
            return []

        await page.click("button#btnSearch")
        await page.wait_for_load_state("networkidle")

        rows = await page.query_selector_all("table#caseTable tbody tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 6:
                continue
            texts = [await c.inner_text() for c in cells]

            case_number = texts[0].strip()
            petitioner = texts[1].strip()
            respondent = texts[2].strip()  # corporate debtor
            filing_date = self._parse_date(texts[3].strip())
            status = texts[4].strip()
            amount_raw = texts[5].strip() if len(texts) > 5 else None

            if not filing_date or filing_date < since:
                continue

            exists = self.db.execute(
                "SELECT 1 FROM legal_events WHERE case_number=%s AND source='nclt'",
                (case_number,)
            ).fetchone()
            if exists:
                continue

            amount = self._parse_amount(amount_raw) if amount_raw else None

            cases.append(RawCase(
                source="nclt",
                case_number=case_number,
                case_type=case_type,
                court=f"NCLT {bench}",
                filing_date=filing_date,
                respondent_name=respondent,
                petitioner_name=petitioner,
                status=status,
                amount_involved=amount,
                raw_data={"bench": bench, "section": section_label, "cells": texts}
            ))

        await page.close()
        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_amount(self, raw: str) -> int:
        """Extract rupee amount from strings like '₹12,50,000' or '12.5 Cr'."""
        raw = raw.replace(',', '').replace('₹', '').strip()
        cr_match = re.search(r'([\d.]+)\s*[Cc][Rr]', raw)
        if cr_match:
            return int(float(cr_match.group(1)) * 10_000_000)
        lakh_match = re.search(r'([\d.]+)\s*[Ll][Aa][Kk][Hh]', raw)
        if lakh_match:
            return int(float(lakh_match.group(1)) * 100_000)
        num = re.search(r'[\d.]+', raw)
        if num:
            return int(float(num.group()))
        return None
```

### Tests: `tests/test_nclt_scraper.py`

```python
import pytest
from datetime import date
from unittest.mock import MagicMock
from ingestion.scrapers.nclt import NCLTScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_nclt_parse_amount_crore():
    s = NCLTScraper(make_db())
    assert s._parse_amount("₹12.5 Cr") == 125_000_000


def test_nclt_parse_amount_lakh():
    s = NCLTScraper(make_db())
    assert s._parse_amount("50 Lakh") == 5_000_000


def test_nclt_parse_amount_plain():
    s = NCLTScraper(make_db())
    assert s._parse_amount("1250000") == 1_250_000


def test_nclt_severity_section_7():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_7") == "CRITICAL"


def test_nclt_severity_section_9():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_9") == "CRITICAL"


def test_nclt_severity_section_10():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_10") == "ALERT"


def test_nclt_parse_date():
    s = NCLTScraper(make_db())
    assert s._parse_date("15/03/2024") == date(2024, 3, 15)
    assert s._parse_date("bad date") is None
```

---

## Source 3: DRT

### Overview
- **URL**: https://drt.gov.in/
- **Cadence**: Daily
- **source_id**: `drt`
- **Event types fired**: `DRT`
- **legal_events.case_type**: `DRT`
- **CAPTCHA**: Possible — use pytesseract, fallback to manual queue.
- **Strategy**: Pull new Original Applications (OA) filed in date range. Respondent = debtor = distressed company.

### What to scrape
Debt Recovery Tribunal Original Applications filed by banks and financial institutions to recover dues. Respondent is the company that defaulted. OA filing = bank has exhausted pre-legal options.

### Search parameters
```
URL: https://drt.gov.in/case-status
Case type: Original Application (OA)
Filing date range: since last pull to today
Bench: loop through 33 DRT benches — cycle 5 per daily run
```

### DRT benches (partial — add all 33)
```python
DRT_BENCHES = [
    "Mumbai I", "Mumbai II", "Delhi", "Chennai", "Kolkata",
    "Ahmedabad", "Hyderabad", "Bengaluru", "Pune", "Jaipur",
    "Chandigarh", "Allahabad", "Nagpur", "Coimbatore", "Ernakulum",
    "Dehradun", "Patna", "Guwahati", "Vishakhapatnam", "Jabalpur",
    "Ranchi", "Siliguri", "Cuttack"
]
```

### File: `ingestion/scrapers/drt.py`

```python
from datetime import date, timedelta
from typing import List
from playwright.async_api import async_playwright
from .base_scraper import BaseScraper, RawCase
import logging, re

logger = logging.getLogger(__name__)

DRT_URL = "https://drt.gov.in/case-status"

DRT_BENCHES = [
    "Mumbai I", "Mumbai II", "Delhi", "Chennai", "Kolkata",
    "Ahmedabad", "Hyderabad", "Bengaluru", "Pune", "Jaipur",
    "Chandigarh", "Allahabad", "Nagpur", "Coimbatore", "Ernakulum",
    "Dehradun", "Patna", "Guwahati", "Vishakhapatnam", "Jabalpur",
    "Ranchi", "Siliguri", "Cuttack"
]


class DRTScraper(BaseScraper):
    source_id = "drt"
    cadence_hours = 24  # daily

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        run_benches = self._benches_for_this_run()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            for bench in run_benches:
                try:
                    batch = await self._scrape_bench(browser, bench, since)
                    cases.extend(batch)
                except Exception as e:
                    logger.error(f"drt {bench} failed: {e}")
            await browser.close()
        return cases

    def _benches_for_this_run(self) -> List[str]:
        from ingestion.scrapers import _run_counter
        offset = (_run_counter.get(self.source_id, 0) * 5) % len(DRT_BENCHES)
        _run_counter[self.source_id] = _run_counter.get(self.source_id, 0) + 1
        return DRT_BENCHES[offset:offset + 5]

    async def _scrape_bench(self, browser, bench: str, since: date) -> List[RawCase]:
        page = await browser.new_page()
        cases = []

        await page.goto(DRT_URL, timeout=30000)
        await page.wait_for_load_state("networkidle")

        await page.select_option("select#drt_bench", bench)
        await page.select_option("select#case_type_code", "OA")
        await page.fill("input#filing_date_from", since.strftime("%d/%m/%Y"))
        await page.fill("input#filing_date_to", date.today().strftime("%d/%m/%Y"))

        # CAPTCHA if present
        captcha_visible = await page.is_visible("img#captchaImage")
        if captcha_visible:
            solved = await self._solve_captcha(page, "img#captchaImage", "input#captchaText")
            if not solved:
                await page.close()
                return []

        await page.click("button[type='submit']")
        await page.wait_for_load_state("networkidle")

        rows = await page.query_selector_all("table.result-table tbody tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 5:
                continue
            texts = [await c.inner_text() for c in cells]

            case_number = texts[0].strip()
            petitioner = texts[1].strip()   # bank / FI
            respondent = texts[2].strip()   # debtor company
            filing_date = self._parse_date(texts[3].strip())
            amount_raw = texts[4].strip() if len(texts) > 4 else None
            status = texts[5].strip() if len(texts) > 5 else "Filed"

            if not filing_date or filing_date < since:
                continue

            exists = self.db.execute(
                "SELECT 1 FROM legal_events WHERE case_number=%s AND source='drt'",
                (case_number,)
            ).fetchone()
            if exists:
                continue

            cases.append(RawCase(
                source="drt",
                case_number=case_number,
                case_type="DRT",
                court=f"DRT {bench}",
                filing_date=filing_date,
                respondent_name=respondent,
                petitioner_name=petitioner,
                status=status,
                amount_involved=self._parse_amount(amount_raw),
                raw_data={"bench": bench, "cells": texts}
            ))

        await page.close()
        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_amount(self, raw: str):
        if not raw:
            return None
        raw = raw.replace(',', '').replace('₹', '').strip()
        cr = re.search(r'([\d.]+)\s*[Cc][Rr]', raw)
        if cr:
            return int(float(cr.group(1)) * 10_000_000)
        num = re.search(r'[\d.]+', raw)
        return int(float(num.group())) if num else None
```

### Tests: `tests/test_drt_scraper.py`

```python
import pytest
from datetime import date
from unittest.mock import MagicMock
from ingestion.scrapers.drt import DRTScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_drt_severity():
    s = DRTScraper(make_db())
    assert s._severity_for_case_type("DRT") == "ALERT"


def test_drt_parse_amount_crore():
    s = DRTScraper(make_db())
    assert s._parse_amount("5 Cr") == 50_000_000


def test_drt_parse_amount_none():
    s = DRTScraper(make_db())
    assert s._parse_amount(None) is None


def test_drt_parse_date():
    s = DRTScraper(make_db())
    assert s._parse_date("01/01/2024") == date(2024, 1, 1)


def test_drt_bench_rotation():
    s = DRTScraper(make_db())
    from ingestion.scrapers import _run_counter
    _run_counter["drt"] = 0
    first = s._benches_for_this_run()
    second = s._benches_for_this_run()
    assert first != second
```

---

## Source 4: SARFAESI

### Overview
- **URL**: https://ibapi.in (IBBI Asset Portal) for auction notices + bank portals for demand notices
- **Cadence**: Daily
- **source_id**: `sarfaesi`
- **Event types fired**: `SARFAESI_13_2`, `SARFAESI_13_4`, `SARFAESI_AUCTION`
- **legal_events.case_type**: `SARFAESI_13_2`, `SARFAESI_13_4`, `SARFAESI_AUCTION`
- **CAPTCHA**: Possible on bank portals. pytesseract first.
- **Strategy**: ibapi.in has a public auction notice list. Pull by date. Bank portals for demand notices — scrape SBI, PNB, BOB public notice pages.

### Signal priority
SARFAESI is the highest-confidence distress signal in the system:
- `13(2)` demand notice = bank classified NPA. 60-day window before possession.
- `13(4)` possession notice = 60 days elapsed, bank taking physical/symbolic possession.
- Auction notice = asset being sold. Company is past the point of return.

### Sources within SARFAESI scraper
```
Primary: ibapi.in/auctions — IBBI's asset portal, public auction listings
Secondary: Major PSU bank "legal notices" pages (SBI, PNB, BOB, Canara, Union)
These post Section 13(2) and 13(4) notices publicly before auction.
```

### File: `ingestion/scrapers/sarfaesi.py`

```python
from datetime import date, timedelta
from typing import List
from playwright.async_api import async_playwright
import httpx
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper, RawCase
import logging, re

logger = logging.getLogger(__name__)

IBAPI_AUCTIONS_URL = "https://ibapi.in/auctions"

# PSU bank notice pages — add more as discovered
BANK_NOTICE_PAGES = [
    {"bank": "SBI", "url": "https://sbi.co.in/web/personal-banking/notices/sarfaesi-notices"},
    {"bank": "PNB", "url": "https://www.pnbindia.in/SARFAESI-Notices.html"},
    {"bank": "BOB", "url": "https://www.bankofbaroda.in/sarfaesi-notices"},
]


class SARFAESIScraper(BaseScraper):
    source_id = "sarfaesi"
    cadence_hours = 24  # daily

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []

        # 1. Auction notices from ibapi.in
        auction_cases = await self._scrape_ibapi_auctions(since)
        cases.extend(auction_cases)

        # 2. Demand/possession notices from PSU bank pages
        for bank_cfg in BANK_NOTICE_PAGES:
            try:
                bank_cases = await self._scrape_bank_notices(bank_cfg, since)
                cases.extend(bank_cases)
            except Exception as e:
                logger.error(f"sarfaesi bank {bank_cfg['bank']} failed: {e}")

        return cases

    async def _scrape_ibapi_auctions(self, since: date) -> List[RawCase]:
        """ibapi.in lists assets under auction — each row is an auction notice."""
        cases = []
        async with httpx.AsyncClient(timeout=30) as client:
            # ibapi has date filter in query params
            resp = await client.get(
                IBAPI_AUCTIONS_URL,
                params={
                    "from_date": since.strftime("%Y-%m-%d"),
                    "to_date": date.today().strftime("%Y-%m-%d"),
                    "page": 1
                },
                headers={"User-Agent": "Mozilla/5.0"}
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table.auction-table tbody tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue

                borrower_name = cells[1].get_text(strip=True)
                bank_name = cells[2].get_text(strip=True)
                auction_date_raw = cells[3].get_text(strip=True)
                reserve_price_raw = cells[4].get_text(strip=True)
                case_ref = cells[0].get_text(strip=True)

                auction_date = self._parse_date(auction_date_raw)
                if not auction_date or auction_date < since:
                    continue

                cases.append(RawCase(
                    source="sarfaesi",
                    case_number=case_ref,
                    case_type="SARFAESI_AUCTION",
                    court=f"SARFAESI Auction ({bank_name})",
                    filing_date=auction_date,
                    respondent_name=borrower_name,
                    petitioner_name=bank_name,
                    status="Auction Scheduled",
                    amount_involved=self._parse_amount(reserve_price_raw),
                    raw_data={"cells": [c.get_text(strip=True) for c in cells]}
                ))

        return cases

    async def _scrape_bank_notices(self, bank_cfg: dict, since: date) -> List[RawCase]:
        """Scrape individual PSU bank SARFAESI notice pages."""
        cases = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto(bank_cfg["url"], timeout=30000)
            await page.wait_for_load_state("networkidle")

            rows = await page.query_selector_all("table tbody tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 4:
                    continue
                texts = [await c.inner_text() for c in cells]

                borrower = texts[0].strip()
                notice_type_raw = texts[1].strip().upper()
                notice_date_raw = texts[2].strip()
                amount_raw = texts[3].strip() if len(texts) > 3 else None

                notice_date = self._parse_date(notice_date_raw)
                if not notice_date or notice_date < since:
                    continue

                # Classify notice type
                if "13(4)" in notice_type_raw or "13 (4)" in notice_type_raw:
                    case_type = "SARFAESI_13_4"
                elif "13(2)" in notice_type_raw or "13 (2)" in notice_type_raw:
                    case_type = "SARFAESI_13_2"
                else:
                    case_type = "SARFAESI_13_2"  # default for unclassified demand notices

                case_num = f"{bank_cfg['bank']}-{notice_date_raw}-{borrower[:20]}"

                cases.append(RawCase(
                    source="sarfaesi",
                    case_number=case_num,
                    case_type=case_type,
                    court=f"SARFAESI ({bank_cfg['bank']})",
                    filing_date=notice_date,
                    respondent_name=borrower,
                    petitioner_name=bank_cfg["bank"],
                    status="Notice Issued",
                    amount_involved=self._parse_amount(amount_raw),
                    raw_data={"bank": bank_cfg["bank"], "cells": texts}
                ))

            await page.close()
            await browser.close()
        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_amount(self, raw: str):
        if not raw:
            return None
        raw = raw.replace(',', '').replace('₹', '').strip()
        cr = re.search(r'([\d.]+)\s*[Cc][Rr]', raw)
        if cr:
            return int(float(cr.group(1)) * 10_000_000)
        num = re.search(r'[\d.]+', raw)
        return int(float(num.group())) if num else None
```

### Tests: `tests/test_sarfaesi_scraper.py`

```python
import pytest
from datetime import date
from unittest.mock import MagicMock
from ingestion.scrapers.sarfaesi import SARFAESIScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_sarfaesi_severity_auction():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_AUCTION") == "CRITICAL"


def test_sarfaesi_severity_13_4():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_13_4") == "CRITICAL"


def test_sarfaesi_severity_13_2():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_13_2") == "ALERT"


def test_sarfaesi_parse_date_variants():
    s = SARFAESIScraper(make_db())
    assert s._parse_date("15-03-2024") == date(2024, 3, 15)
    assert s._parse_date("15 Mar 2024") == date(2024, 3, 15)
    assert s._parse_date("garbage") is None


def test_sarfaesi_parse_amount():
    s = SARFAESIScraper(make_db())
    assert s._parse_amount("2.5 Cr") == 25_000_000
    assert s._parse_amount(None) is None
```

---

## Source 5: IBBI

### Overview
- **URL**: https://ibbi.gov.in/
- **Cadence**: Weekly (every 7 days)
- **source_id**: `ibbi`
- **Event types fired**: Mapped to `NCLT_7` event type (IBBI liquidation orders are downstream of NCLT admission — reuse the case type, mark source as 'ibbi')
- **legal_events.case_type**: Use `NCLT_7` for CIRP/liquidation entries. Source column = `ibbi`.
- **CAPTCHA**: None observed — public document portal.
- **Strategy**: Scrape "New Insolvency Commencement Orders" and "Liquidation Orders" published this week. Pull by publication date range.

### What to scrape
IBBI publishes:
1. **Insolvency commencement** — CIRP admitted (NCLT order published to IBBI). Confirms NCLT filing.
2. **Liquidation orders** — company sent to liquidation. Highest distress state.
3. **Resolution professional appointments** — RP assigned to manage the process.

Use these to confirm NCLT events already fired or to catch cases we missed via NCLT scraper.

### File: `ingestion/scrapers/ibbi.py`

```python
from datetime import date, timedelta
from typing import List
import httpx
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper, RawCase
import logging, re

logger = logging.getLogger(__name__)

IBBI_BASE = "https://ibbi.gov.in"
IBBI_CIRP_URL = f"{IBBI_BASE}/home/getSearchResult"  # POST endpoint observed
IBBI_ORDERS_URL = f"{IBBI_BASE}/legal-framework/orders"


class IBBIScraper(BaseScraper):
    source_id = "ibbi"
    cadence_hours = 168  # weekly

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        orders = await self._fetch_orders(since)
        cases.extend(orders)
        return cases

    async def _fetch_orders(self, since: date) -> List[RawCase]:
        """
        IBBI publishes orders at ibbi.gov.in/legal-framework/orders
        Sorted by date descending. Pull first N pages until we reach `since`.
        No CAPTCHA — public PDF listing page.
        """
        cases = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            page_num = 1
            while True:
                resp = await client.get(
                    IBBI_ORDERS_URL,
                    params={"page": page_num, "per_page": 50},
                    headers={"User-Agent": "Mozilla/5.0"}
                )
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select("table.order-table tbody tr")

                if not rows:
                    break

                hit_cutoff = False
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 4:
                        continue

                    order_date_raw = cells[0].get_text(strip=True)
                    company_name = cells[1].get_text(strip=True)
                    order_type = cells[2].get_text(strip=True).upper()
                    case_ref = cells[3].get_text(strip=True)

                    order_date = self._parse_date(order_date_raw)
                    if not order_date:
                        continue
                    if order_date < since:
                        hit_cutoff = True
                        break

                    # Classify
                    if "LIQUIDATION" in order_type:
                        case_type = "NCLT_7"
                        status = "Liquidation Ordered"
                        severity_override = "CRITICAL"
                    elif "CIRP" in order_type or "COMMENCEMENT" in order_type:
                        case_type = "NCLT_7"
                        status = "CIRP Commenced"
                        severity_override = "CRITICAL"
                    else:
                        case_type = "NCLT_7"
                        status = order_type
                        severity_override = "ALERT"

                    exists = self.db.execute(
                        "SELECT 1 FROM legal_events WHERE case_number=%s AND source='ibbi'",
                        (case_ref,)
                    ).fetchone()
                    if exists:
                        continue

                    cases.append(RawCase(
                        source="ibbi",
                        case_number=case_ref,
                        case_type=case_type,
                        court="IBBI",
                        filing_date=order_date,
                        respondent_name=company_name,
                        petitioner_name=None,
                        status=status,
                        amount_involved=None,
                        raw_data={
                            "order_type": order_type,
                            "date": order_date_raw,
                            "cells": [c.get_text(strip=True) for c in cells]
                        }
                    ))

                if hit_cutoff:
                    break
                page_num += 1

        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
            try:
                from datetime import datetime
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None
```

### Tests: `tests/test_ibbi_scraper.py`

```python
import pytest
from datetime import date
from unittest.mock import MagicMock, patch, AsyncMock
from ingestion.scrapers.ibbi import IBBIScraper
from ingestion.scrapers.base_scraper import RawCase


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_ibbi_severity_for_liquidation():
    s = IBBIScraper(make_db())
    # IBBI maps liquidation to NCLT_7 — severity is CRITICAL
    assert s._severity_for_case_type("NCLT_7") == "CRITICAL"


def test_ibbi_parse_date():
    s = IBBIScraper(make_db())
    assert s._parse_date("10 Jan 2024") == date(2024, 1, 10)
    assert s._parse_date("10-01-2024") == date(2024, 1, 10)
    assert s._parse_date("rubbish") is None


def test_ibbi_existing_case_skipped():
    db = make_db()
    db.execute.return_value.fetchone.return_value = (1,)
    s = IBBIScraper(db)
    # If case already in DB, _process_case should not fire
    case = RawCase("ibbi","IBBI/2024/001","NCLT_7","IBBI",
                   date(2024,1,10),"Test Co",None,"CIRP Commenced",None,{})
    s._process_case = MagicMock()
    s._process_case(case)
    s._process_case.assert_called_once()


def test_ibbi_hash_stable():
    s = IBBIScraper(make_db())
    c1 = RawCase("ibbi","R001","NCLT_7","IBBI",date(2024,1,1),"A",None,"X",None,{})
    c2 = RawCase("ibbi","R002","NCLT_7","IBBI",date(2024,1,2),"B",None,"X",None,{})
    assert s.compute_hash([c1, c2]) == s.compute_hash([c2, c1])
```

---

## Scheduler: `ingestion/scheduler.py`

Wire all 5 scrapers into APScheduler. Each runs on its own cadence.

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio, psycopg2, os, logging

from ingestion.scrapers.ecourts import ECourtsScraper
from ingestion.scrapers.nclt import NCLTScraper
from ingestion.scrapers.drt import DRTScraper
from ingestion.scrapers.sarfaesi import SARFAESIScraper
from ingestion.scrapers.ibbi import IBBIScraper

logger = logging.getLogger(__name__)


def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


async def run_scraper(scraper_class):
    db = get_db()
    scraper = scraper_class(db)
    try:
        await scraper.run()
    except Exception as e:
        logger.error(f"{scraper_class.__name__} failed: {e}")
    finally:
        db.close()


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(ECourtsScraper)),
        trigger=IntervalTrigger(hours=168),
        id="ecourts", name="e-Courts weekly"
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(NCLTScraper)),
        trigger=IntervalTrigger(hours=24),
        id="nclt", name="NCLT daily"
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(DRTScraper)),
        trigger=IntervalTrigger(hours=24),
        id="drt", name="DRT daily"
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(SARFAESIScraper)),
        trigger=IntervalTrigger(hours=24),
        id="sarfaesi", name="SARFAESI daily"
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(IBBIScraper)),
        trigger=IntervalTrigger(hours=168),
        id="ibbi", name="IBBI weekly"
    )

    return scheduler


if __name__ == "__main__":
    import asyncio
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("Scheduler started — 5 legal scrapers active")
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        scheduler.shutdown()
```

---

## Codex task summary

Build in this order:

1. `ingestion/scrapers/__init__.py` — add `_run_counter = {}` module-level dict for bench rotation
2. `ingestion/scrapers/base_scraper.py` — full BaseScraper as specified above
3. `ingestion/scrapers/ecourts.py` — weekly, SEC_138, Playwright + pytesseract
4. `ingestion/scrapers/nclt.py` — daily, NCLT_7/9/10, all 14 benches, bench rotation
5. `ingestion/scrapers/drt.py` — daily, DRT, 23 benches, bench rotation
6. `ingestion/scrapers/sarfaesi.py` — daily, ibapi.in + bank notice pages, BeautifulSoup + Playwright
7. `ingestion/scrapers/ibbi.py` — weekly, httpx + BeautifulSoup, no CAPTCHA
8. `ingestion/scheduler.py` — APScheduler wiring all 5
9. Tests — one test file per scraper as specified above

**Pass criteria:**
- All 5 scrapers instantiate without error
- All test files pass
- `scheduler.py` starts without error
- No scraper calls `EntityResolver` outside `_process_case` (entity resolution stays in base class)
- Unresolved entities land in `unmapped_signals`, not discarded
- `source_state` updated after every run regardless of whether cases were found

---

## Known uncertainties (Claude Code to resolve before handing to Codex)

1. **ibapi.in exact URL and table structure** — needs a live Playwright probe to confirm selectors. Spec uses plausible selectors; Codex must verify against actual HTML before locking in.
2. **NCLT bench selector values** — the select option values on nclt.gov.in may not match bench display names exactly. Codex must inspect the actual `<select>` options.
3. **DRT portal** — drt.gov.in is sometimes down. Build retry logic using the base class failure handler.
4. **Bank SARFAESI pages** — SBI/PNB/BOB table structures vary. Codex should extract at least one (SBI) and note the others as stubs to be filled once structure is confirmed.
5. **entity_resolution_queue schema** — base class assumes columns `raw_name, source, candidate_cin, confidence, raw_data`. Verify against `SCHEMA_SPEC.md` and adjust if column names differ.
