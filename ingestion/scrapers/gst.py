from __future__ import annotations

import asyncio
import json
import logging
import re
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
            logger.info("gst: no monitored GSTINs discovered")
            self._store_state(self.source_id, {"gstins_checked": 0, "emitted_events": 0}, record_count=0)
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

            await asyncio.sleep(2)

        self._store_state(
            self.source_id,
            {"gstins_checked": len(monitored), "emitted_events": len(emitted)},
            record_count=len(monitored),
        )
        return emitted

    def _load_monitored_gstins(self) -> list[tuple[str, str]]:
        try:
            if self._table_has_column("master_entities", "gstin"):
                rows = self._fetchall(
                    """
                    SELECT cin, gstin
                    FROM master_entities
                    WHERE gstin IS NOT NULL
                      AND status = 'Active'
                    """,
                )
            else:
                rows = self._fetchall(
                    """
                    SELECT im.cin, im.identifier_value AS gstin
                    FROM identifier_map im
                    JOIN master_entities me ON me.cin = im.cin
                    WHERE im.identifier_type = 'GSTIN'
                      AND me.status = 'Active'
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
        if monitored:
            return monitored
        return self._bootstrap_gstins_from_pan()

    def _bootstrap_gstins_from_pan(self) -> list[tuple[str, str]]:
        try:
            rows = self._fetchall(
                """
                SELECT cin, pan, registered_state
                FROM master_entities
                WHERE pan IS NOT NULL
                  AND status = 'Active'
                LIMIT 100
                """
            )
        except Exception as exc:
            logger.warning("gst: unable to bootstrap GSTINs from PAN: %s", exc)
            return []

        discovered: list[tuple[str, str]] = []
        for row in rows:
            if isinstance(row, dict):
                cin = row.get("cin")
                pan = row.get("pan")
                registered_state = row.get("registered_state")
            else:
                cin, pan, registered_state = row[0], row[1], row[2]
            if not cin or not pan:
                continue

            for gstin in self._candidate_gstins(pan, registered_state):
                payload = self._fetch_taxpayer_payload(gstin)
                details = self._normalise_taxpayer_payload(gstin, payload) if payload else None
                if not details or not details.get("gstin_status") or not details.get("trade_name"):
                    continue
                discovered.append((cin, gstin))
                self._store_discovered_gstin(cin, gstin)
                logger.info("gst: bootstrapped gstin=%s for cin=%s", gstin, cin)
                break
            time.sleep(1)
        return discovered

    def _candidate_gstins(self, pan: str, registered_state: Optional[str]) -> list[str]:
        pan = (pan or "").strip().upper()
        if not re.fullmatch(r"[A-Z]{5}\d{4}[A-Z]", pan):
            return []
        state_codes = self._prioritised_state_codes(registered_state)
        gstins: list[str] = []
        for state_code in state_codes:
            prefix = f"{state_code}{pan}1Z"
            gstins.append(prefix + self._gst_checksum(prefix))
        return gstins

    def _prioritised_state_codes(self, registered_state: Optional[str]) -> list[str]:
        all_codes = [
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
            "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
            "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "97",
        ]
        state_map = {
            "andaman and nicobar islands": "35",
            "andhra pradesh": "37",
            "arunachal pradesh": "12",
            "assam": "18",
            "bihar": "10",
            "chandigarh": "04",
            "chhattisgarh": "22",
            "dadra and nagar haveli and daman and diu": "26",
            "delhi": "07",
            "goa": "30",
            "gujarat": "24",
            "haryana": "06",
            "himachal pradesh": "02",
            "jammu and kashmir": "01",
            "jharkhand": "20",
            "karnataka": "29",
            "kerala": "32",
            "ladakh": "38",
            "lakshadweep": "31",
            "madhya pradesh": "23",
            "maharashtra": "27",
            "manipur": "14",
            "meghalaya": "17",
            "mizoram": "15",
            "nagaland": "13",
            "odisha": "21",
            "orissa": "21",
            "other territory": "97",
            "puducherry": "34",
            "punjab": "03",
            "rajasthan": "08",
            "sikkim": "11",
            "tamil nadu": "33",
            "telangana": "36",
            "tripura": "16",
            "uttar pradesh": "09",
            "uttarakhand": "05",
            "west bengal": "19",
            "an": "35",
            "ap": "37",
            "ar": "12",
            "as": "18",
            "br": "10",
            "cg": "22",
            "ch": "04",
            "dd": "26",
            "dl": "07",
            "ga": "30",
            "gj": "24",
            "hr": "06",
            "hp": "02",
            "jk": "01",
            "jh": "20",
            "ka": "29",
            "kl": "32",
            "la": "38",
            "ld": "31",
            "mh": "27",
            "ml": "17",
            "mn": "14",
            "mp": "23",
            "mz": "15",
            "nl": "13",
            "od": "21",
            "or": "21",
            "pb": "03",
            "py": "34",
            "rj": "08",
            "sk": "11",
            "tn": "33",
            "tr": "16",
            "ts": "36",
            "uk": "05",
            "up": "09",
            "wb": "19",
        }
        preferred = None
        if registered_state:
            cleaned = registered_state.strip().lower()
            if cleaned.isdigit() and len(cleaned) <= 2:
                preferred = cleaned.zfill(2)
            else:
                preferred = state_map.get(cleaned)
        if preferred and preferred in all_codes:
            return [preferred] + [code for code in all_codes if code != preferred]
        return all_codes

    def _gst_checksum(self, body: str) -> str:
        charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        factor = 2
        total = 0
        for char in reversed(body):
            code_point = charset.index(char)
            digit = factor * code_point
            total += (digit // 36) + (digit % 36)
            factor = 1 if factor == 2 else 2
        remainder = total % 36
        return charset[(36 - remainder) % 36]

    def _store_discovered_gstin(self, cin: str, gstin: str) -> None:
        try:
            if self._table_has_column("master_entities", "gstin"):
                self._execute(
                    """
                    UPDATE master_entities
                    SET gstin = %s
                    WHERE cin = %s
                      AND (gstin IS NULL OR gstin = '')
                    """,
                    (gstin, cin),
                )
            else:
                self._execute(
                    """
                    INSERT INTO identifier_map (cin, identifier_type, identifier_value, source, confidence, created_at)
                    SELECT %s, 'GSTIN', %s, %s, %s, NOW()
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM identifier_map
                        WHERE cin = %s
                          AND identifier_type = 'GSTIN'
                          AND identifier_value = %s
                    )
                    """,
                    (cin, gstin, self.source_id, 0.80, cin, gstin),
                )
            self._commit()
        except Exception as exc:
            logger.warning("gst: failed to persist discovered GSTIN for cin=%s: %s", cin, exc)

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
