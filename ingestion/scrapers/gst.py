from __future__ import annotations

import json
import logging
import time
from typing import Optional

import requests

from .captcha_solver import solve as solve_captcha, WHITELIST_LOWER
from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

GST_SEARCH_URL  = "https://services.gst.gov.in/services/searchtp"
GST_CAPTCHA_URL = "https://services.gst.gov.in/services/captcha"
GST_API_URL     = "https://services.gst.gov.in/services/api/search/taxpayerDetails"


class GSTScraper(BaseSignalScraper):
    source_id = "gst"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        monitored = self._load_monitored_gstins()
        if not monitored:
            return []

        emitted: list[dict] = []
        for cin, gstin in monitored:
            payload = self._fetch_taxpayer_payload(gstin)
            if not payload:
                continue
            details = self._normalise_taxpayer_payload(gstin, payload)
            if not details:
                continue

            state_key = f"gst:{gstin}"
            previous = self._load_state(state_key)
            previous_status = previous.get("gstin_status")
            current_status = details.get("gstin_status")
            self._store_state(state_key, details)

            event_type, severity = self._classify_transition(previous_status, current_status)
            if previous_status and event_type:
                event_payload = {
                    "cin": cin,
                    "gstin": gstin,
                    "previous_status": previous_status,
                    **details,
                }
                self._insert_event(cin, event_type, severity, event_payload)
                emitted.append(event_payload)

            time.sleep(2)  # polite delay between GSTINs

        return emitted

    def _load_monitored_gstins(self) -> list[tuple[str, str]]:
        try:
            rows = self._fetchall(
                """
                SELECT cin, gstin
                FROM master_entities
                WHERE gstin IS NOT NULL
                  AND status = 'Active'
                """,
            )
        except Exception as exc:
            logger.warning("gst: unable to load monitored GSTINs: %s", exc)
            return []

        monitored = []
        for row in rows:
            if isinstance(row, dict):
                cin, gstin = row.get("cin"), row.get("gstin")
            else:
                cin, gstin = row[0], row[1]
            if cin and gstin:
                monitored.append((cin, gstin))
        return monitored

    def _fetch_taxpayer_payload(self, gstin: str) -> Optional[dict]:
        """Session-cookie pattern (shubham-dube/GST-Verification-API):
        All three requests share one Session so the portal's CAPTCHA
        validation cookie is present when the data query fires.

        1. GET searchtp  → sets session cookie
        2. GET captcha   → same session, get CAPTCHA image
        3. POST api/search/taxpayerDetails  → same session, send solved CAPTCHA
        """
        for attempt in range(3):
            session = requests.Session()
            session.headers.update({"User-Agent": "Mozilla/5.0"})

            # 1. Init session
            try:
                session.get(GST_SEARCH_URL, timeout=20)
            except Exception as e:
                logger.warning("gst: session init failed gstin=%s: %s", gstin, e)
                return None

            # 2. Fetch CAPTCHA (same session → server ties it to our cookie)
            try:
                captcha_resp = session.get(GST_CAPTCHA_URL, timeout=15)
                captcha_resp.raise_for_status()
            except Exception as e:
                logger.warning("gst: captcha fetch failed gstin=%s: %s", gstin, e)
                return None

            captcha_text = solve_captcha(captcha_resp.content, whitelist=WHITELIST_LOWER)
            if not captcha_text:
                logger.warning("gst: captcha unsolved gstin=%s attempt %d", gstin, attempt + 1)
                time.sleep(2)
                continue

            # 3. POST with solved CAPTCHA (same session)
            try:
                resp = session.post(
                    GST_API_URL,
                    json={"gstin": gstin, "captcha": captcha_text},
                    timeout=20,
                )
            except Exception as e:
                logger.warning("gst: api POST failed gstin=%s: %s", gstin, e)
                return None

            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("gst: rate-limited gstin=%s, sleeping %ds", gstin, wait)
                time.sleep(wait)
                continue

            text = resp.text
            if resp.status_code >= 400 or "Request Rejected" in text:
                # Could be wrong CAPTCHA — retry with fresh session
                logger.warning(
                    "gst: rejected gstin=%s status=%s attempt=%d",
                    gstin, resp.status_code, attempt + 1,
                )
                time.sleep(2)
                continue

            try:
                return resp.json()
            except json.JSONDecodeError:
                logger.warning("gst: non-json response for gstin=%s", gstin)
                return None

        logger.warning("gst: all attempts failed for gstin=%s", gstin)
        return None

    def _normalise_taxpayer_payload(self, gstin: str, payload: dict) -> Optional[dict]:
        if not isinstance(payload, dict):
            return None
        trade_name = self.extract_json_value(payload, "trade")
        status = self.extract_json_value(payload, "status")
        registration_date = self.extract_json_value(payload, "registration", "date")
        cancellation_date = self.extract_json_value(payload, "cancellation", "date")
        cancellation_reason = self.extract_json_value(payload, "cancellation", "reason")
        if not status:
            status = self.extract_json_value(payload, "sts")
        if not trade_name:
            trade_name = self.extract_json_value(payload, "trad")
        return {
            "gstin": gstin,
            "trade_name": self.normalize_text(str(trade_name or "")) or None,
            "registration_date": str(self.parse_date(str(registration_date or "")) or ""),
            "gstin_status": self.normalize_text(str(status or "")) or None,
            "cancellation_date": str(self.parse_date(str(cancellation_date or "")) or ""),
            "cancellation_reason": self.normalize_text(str(cancellation_reason or "")) or None,
            "raw_payload": payload,
        }

    def _classify_transition(
        self, previous_status: Optional[str], current_status: Optional[str]
    ) -> tuple[Optional[str], Optional[str]]:
        current = (current_status or "").strip().lower()
        previous = (previous_status or "").strip().lower()
        if current == previous:
            return None, None
        if current == "cancelled":
            return "GST_CANCELLED", "CRITICAL"
        if current == "suspended":
            return "GST_SUSPENDED", "ALERT"
        if current == "active" and previous in {"cancelled", "suspended"}:
            return "GST_RESTORED", "INFO"
        return None, None
