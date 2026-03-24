import asyncio
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

from playwright.async_api import Page, async_playwright

from ingestion.entity_resolver import EntityResolver

logger = logging.getLogger(__name__)


@dataclass
class RawCase:
    """Normalised case record before entity resolution."""

    source: str
    case_number: str
    case_type: str
    court: str
    filing_date: Optional[date]
    respondent_name: str
    petitioner_name: Optional[str]
    status: str
    amount_involved: Optional[int]
    raw_data: dict


class BaseScraper(ABC):
    source_id: str
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
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def get_last_pull_state(self) -> tuple[Optional[datetime], Optional[str]]:
        row = self.db.execute(
            "SELECT last_pull_at, last_data_hash FROM source_state WHERE source_id = %s",
            (self.source_id,),
        ).fetchone()
        if row:
            return row[0], row[1]
        return None, None

    def update_source_state(self, new_hash: str, record_count: int, status: str = "OK"):
        self.db.execute(
            """
            UPDATE source_state
            SET last_pull_at = NOW(), last_data_hash = %s,
                record_count = %s, status = %s,
                consecutive_failures = 0, updated_at = NOW()
            WHERE source_id = %s
            """,
            (new_hash, record_count, status, self.source_id),
        )
        self.db.commit()

    def increment_failure(self):
        self.db.execute(
            """
            UPDATE source_state
            SET consecutive_failures = consecutive_failures + 1,
                status = CASE WHEN consecutive_failures + 1 >= 4 THEN 'DEGRADED' ELSE status END,
                updated_at = NOW()
            WHERE source_id = %s
            """,
            (self.source_id,),
        )
        self.db.commit()

    async def run(self):
        last_pull, last_hash = self.get_last_pull_state()
        since = last_pull.date() if last_pull else date.today() - timedelta(days=7)
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
        if not case.respondent_name or not case.respondent_name.strip():
            return
        resolver = EntityResolver(self.db)
        result = resolver.resolve(case.respondent_name)

        if result.cin and result.confidence >= 0.75:
            self._upsert_legal_event(case, result.cin)
        elif result.cin and result.confidence >= 0.50:
            self._queue_for_resolution(case, result)
        else:
            self._store_unmapped(case)

    def _upsert_legal_event(self, case: RawCase, cin: str):
        existing = self.db.execute(
            "SELECT id FROM legal_events WHERE case_number = %s AND source = %s",
            (case.case_number, case.source),
        ).fetchone()

        if existing:
            self.db.execute(
                """
                UPDATE legal_events SET status = %s, updated_at = NOW()
                WHERE case_number = %s AND source = %s
                """,
                (case.status, case.case_number, case.source),
            )
        else:
            event_id = self._insert_event(case, cin)
            self.db.execute(
                """
                INSERT INTO legal_events
                  (cin, case_type, case_number, court, filing_date, status,
                   amount_involved, source, event_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    cin,
                    case.case_type,
                    case.case_number,
                    case.court,
                    case.filing_date,
                    case.status,
                    case.amount_involved,
                    case.source,
                    event_id,
                ),
            )

        self.db.commit()

    def _insert_event(self, case: RawCase, cin: str) -> int:
        severity = self._severity_for_case_type(case.case_type)
        row = self.db.execute(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            RETURNING id
            """,
            (cin, case.source, case.case_type, severity, json.dumps(case.raw_data)),
        ).fetchone()
        self.db.commit()
        return row[0]

    def _severity_for_case_type(self, case_type: str) -> str:
        return {
            "SARFAESI_13_2": "ALERT",
            "SARFAESI_13_4": "CRITICAL",
            "SARFAESI_AUCTION": "CRITICAL",
            "NCLT_7": "CRITICAL",
            "NCLT_9": "CRITICAL",
            "NCLT_10": "ALERT",
            "DRT": "ALERT",
            "SEC_138": "ALERT",
            "HIGH_COURT": "WATCH",
            "LABOUR": "WATCH",
        }.get(case_type, "WATCH")

    def _queue_for_resolution(self, case: RawCase, result):
        self.db.execute(
            """
            INSERT INTO entity_resolution_queue
              (source, raw_name, candidates, best_confidence, resolution_method, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                case.source,
                case.respondent_name,
                json.dumps(result.candidates),
                result.confidence * 100 if result.confidence <= 1 else result.confidence,
                result.method,
            ),
        )
        self.db.commit()

    def _store_unmapped(self, case: RawCase):
        self.db.execute(
            """
            INSERT INTO unmapped_signals
              (source, identifier_type, identifier_value, raw_data, detected_at)
            VALUES (%s, 'COMPANY_NAME', %s, %s, NOW())
            """,
            (case.source, case.respondent_name, json.dumps(case.raw_data)),
        )
        self.db.commit()

    async def _solve_captcha(self, page: Page, img_selector: str, input_selector: str) -> bool:
        """
        CAPTCHA strategy:
        1. OpenCV pipeline OCR on screenshot
        2. Log attempt in captcha_log
        3. If OCR fails — leave for manual queue (POST /op/captcha/solve)
        Returns True if solved, False if queued for manual.
        """
        from .captcha_solver import solve as solve_captcha, WHITELIST_UPPER

        img_el = page.locator(img_selector)
        img_bytes = await img_el.screenshot()

        text = solve_captcha(img_bytes, whitelist=WHITELIST_UPPER, psm=7)

        self.db.execute(
            """
            INSERT INTO captcha_log (source, method, success, cost_inr, solved_at)
            VALUES (%s, 'OCR', %s, 0, NOW())
            """,
            (self.source_id, bool(text)),
        )
        self.db.commit()

        if text:
            await page.fill(input_selector, text)
            return True

        logger.warning(f"{self.source_id}: CAPTCHA OCR failed, queued for manual")
        return False
