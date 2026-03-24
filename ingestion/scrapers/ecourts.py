"""e-Courts district court scraper — v6 portal.

Targets: https://services.ecourts.gov.in/ecourtindia_v6/
Searches NI Act Sec 138 (cheque bounce) cases by act across district courts.

Flow per state:
  1. GET ?p=casestatus/index  → extract app_token + state dropdown codes
  2. POST ?p=casestatus/fillDistrict(state_code) → district list (HTML options)
  3. POST ?p=casestatus/fillCourtComplex(state_code, dist_code) → complex list
  4. Solve CAPTCHA (securimage) → POST ?p=casestatus/submitAct
  5. Response: JSON {"act_data": "<html>..."} → parse table rows → RawCase

Verified 2026-03-22 via browser DevTools:
  NI Act code:          8
  fillDistrict params:  state_code, ajax_req=true, app_token
  submitAct params:     search_act, actcode, under_sec, case_status, act_captcha_code,
                        state_code, dist_code, court_complex_code, est_code, ajax_req, app_token
  app_token location:   <input type="hidden" id="app_token">
  CAPTCHA img src:      vendor/securimage/securimage_show.php?{hash}
"""

import logging
import time
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, RawCase
from .captcha_solver import solve as solve_captcha, WHITELIST_LOWER

logger = logging.getLogger(__name__)

ECOURTS_BASE = "https://services.ecourts.gov.in/ecourtindia_v6/"
CASE_STATUS_URL = ECOURTS_BASE + "?p=casestatus/index"
FILL_DISTRICT_URL = ECOURTS_BASE + "?p=casestatus/fillDistrict"
FILL_COMPLEX_URL = ECOURTS_BASE + "?p=casestatus/fillcomplex"
SUBMIT_ACT_URL = ECOURTS_BASE + "?p=casestatus/submitAct"

NI_ACT_CODE = "8"  # Verified 2026-03-22

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": ECOURTS_BASE,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}


class ECourtsScraper(BaseScraper):
    source_id = "ecourts"
    cadence_hours = 168  # weekly

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        try:
            session, app_token, states = self._init_session()
        except Exception as e:
            logger.error(f"ecourts session init failed: {e}")
            return []

        if not states:
            logger.error("ecourts: no states returned from portal")
            return []

        # Rotate 5 states per weekly run to spread load
        from ingestion.scrapers import _run_counter
        state_items = list(states.items())
        offset = (_run_counter.get(self.source_id, 0) * 5) % len(state_items)
        _run_counter[self.source_id] = _run_counter.get(self.source_id, 0) + 1
        run_states = state_items[offset: offset + 5]

        cases: List[RawCase] = []
        for state_code, state_name in run_states:
            # Fresh session per state — PHP sessions expire in ~10-15 min and a single
            # state run can take that long across many districts + complexes.
            try:
                session, app_token, _ = self._init_session()
            except Exception as e:
                logger.error(f"ecourts {state_name} session init failed: {e}")
                continue
            try:
                state_cases = self._scrape_state(
                    session, app_token, state_code, state_name, since
                )
                cases.extend(state_cases)
            except Exception as e:
                logger.error(f"ecourts {state_name} failed: {e}")

        return cases

    # ------------------------------------------------------------------ #
    #  Session + dropdown population                                       #
    # ------------------------------------------------------------------ #

    def _init_session(self) -> Tuple[requests.Session, str, Dict[str, str]]:
        """GET Case Status page → extract app_token and state dropdown codes."""
        session = requests.Session()
        session.headers.update(_HEADERS)

        r = session.get(CASE_STATUS_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        token_input = soup.find("input", {"id": "app_token"})
        app_token = token_input["value"] if token_input and token_input.get("value") else ""

        state_select = (
            soup.find("select", {"id": "sess_state_code"})
            or soup.find("select", {"name": "state_code"})
            or soup.find("select", {"id": "state_code"})
        )
        states: Dict[str, str] = {}
        if state_select:
            for opt in state_select.find_all("option"):
                val = opt.get("value", "").strip()
                if val and val not in ("0", ""):
                    states[val] = opt.get_text(strip=True)

        logger.info(
            f"ecourts: session ready, app_token={'set' if app_token else 'MISSING'}, "
            f"{len(states)} states"
        )
        return session, app_token, states

    def _fetch_districts(
        self, session: requests.Session, state_code: str, app_token: str
    ) -> List[Tuple[str, str]]:
        try:
            resp = session.post(
                FILL_DISTRICT_URL,
                data={"state_code": state_code, "ajax_req": "true", "app_token": app_token},
                timeout=20,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"District list failed for state {state_code}: {e}")
            return []
        # Response: {"dist_list": "<option...>"}
        html = self._unwrap_json(resp.text, "dist_list")
        return self._parse_options(html)

    def _fetch_complexes(
        self,
        session: requests.Session,
        state_code: str,
        dist_code: str,
        app_token: str,
    ) -> List[Tuple[str, str]]:
        try:
            resp = session.post(
                FILL_COMPLEX_URL,
                data={
                    "state_code": state_code,
                    "dist_code": dist_code,
                    "ajax_req": "true",
                    "app_token": app_token,
                },
                timeout=20,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Complex list failed for dist {dist_code}: {e}")
            return []
        # Try common key names for court complex response
        html = self._unwrap_json(resp.text, "complex_list") or self._unwrap_json(resp.text, "court_list") or resp.text
        return self._parse_options(html)

    def _unwrap_json(self, text: str, key: str) -> str:
        """Extract HTML string from JSON response like {"key": "<html>"}."""
        try:
            data = __import__("json").loads(text)
            return data.get(key, "")
        except Exception:
            return ""

    def _parse_options(self, html: str) -> List[Tuple[str, str]]:
        """Parse <option value="N">Name</option> from an HTML fragment."""
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for opt in soup.find_all("option"):
            val = opt.get("value", "").strip()
            if val and val not in ("0", ""):
                results.append((val, opt.get_text(strip=True)))
        return results

    # ------------------------------------------------------------------ #
    #  Scraping                                                            #
    # ------------------------------------------------------------------ #

    def _scrape_state(
        self,
        session: requests.Session,
        app_token: str,
        state_code: str,
        state_name: str,
        since: date,
    ) -> List[RawCase]:
        districts = self._fetch_districts(session, state_code, app_token)
        if not districts:
            logger.warning(f"ecourts {state_name}: no districts returned")
            return []

        cases: List[RawCase] = []
        for dist_code, dist_name in districts:
            # Fresh session per district — PHP sessions expire in ~10-15 min;
            # a single district with many complexes can exhaust that window.
            try:
                session, app_token, _ = self._init_session()
            except Exception as e:
                logger.warning(f"ecourts {state_name}/{dist_name} session reinit failed: {e}")
                continue
            try:
                complexes = self._fetch_complexes(session, state_code, dist_code, app_token)
                if not complexes:
                    logger.debug(f"ecourts {state_name}/{dist_name}: no complexes")
                    continue
                for complex_code, complex_name in complexes:
                    try:
                        c = self._scrape_complex(
                            session, app_token, state_code, state_name,
                            dist_code, dist_name, complex_code, complex_name, since,
                        )
                        cases.extend(c)
                        time.sleep(8)   # polite delay between courts
                    except Exception as e:
                        logger.warning(
                            f"ecourts {state_name}/{dist_name}/{complex_name}: {e}"
                        )
            except Exception as e:
                logger.warning(f"ecourts {state_name}/{dist_name}: {e}")
            time.sleep(15)  # longer pause between districts

        return cases

    def _scrape_complex(
        self,
        session: requests.Session,
        app_token: str,
        state_code: str,
        state_name: str,
        dist_code: str,
        dist_name: str,
        complex_code: str,
        complex_name: str,
        since: date,
    ) -> List[RawCase]:
        """Search with up to 3 CAPTCHA attempts. On failure the server returns
        a fresh CAPTCHA URL in div_captcha — use it for the next attempt."""
        import re as _re

        captcha_url: Optional[str] = None  # None = use default URL

        for attempt in range(3):
            captcha_text = self._solve_captcha_http(session, captcha_url)
            if not captcha_text:
                logger.warning(
                    f"ecourts {state_name}/{dist_name}/{complex_name}: "
                    f"CAPTCHA unsolved attempt {attempt + 1}"
                )
                continue

            try:
                resp = session.post(
                    SUBMIT_ACT_URL,
                    data={
                        "search_act": "Negotiable Instruments Act",
                        "actcode": NI_ACT_CODE,
                        "under_sec": "",
                        "case_status": "Pending",
                        "act_captcha_code": captcha_text,
                        "state_code": state_code,
                        "dist_code": dist_code,
                        "court_complex_code": complex_code,
                        "est_code": "null",
                        "ajax_req": "true",
                        "app_token": app_token,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.warning(
                    f"ecourts {state_name}/{dist_name}/{complex_name}: search POST failed: {e}"
                )
                return []

            if not resp.text.strip():
                logger.warning(
                    f"ecourts {state_name}/{dist_name}/{complex_name}: empty response (rate limited), sleeping"
                )
                time.sleep(10)
                continue

            try:
                data = resp.json()
            except Exception:
                logger.warning(
                    f"ecourts {state_name}/{dist_name}/{complex_name}: non-JSON response: {resp.text[:100]}"
                )
                return []

            # act_data present but empty + errormsg contains Captcha = wrong CAPTCHA
            if "act_data" in data and "captcha" not in data.get("errormsg", "").lower():
                return self._parse_act_html(
                    data["act_data"], state_name, dist_name, complex_name, since
                )

            # Invalid CAPTCHA — extract fresh URL from error response and retry
            if "div_captcha" in data or "captcha" in data.get("errormsg", "").lower():
                match = _re.search(
                    r'src="([^"]*securimage_show\.php[^"]*)"', data.get("div_captcha", "")
                )
                if match:
                    src = match.group(1).replace("\\/", "/")
                    captcha_url = "https://services.ecourts.gov.in" + src
                logger.debug(
                    f"ecourts {state_name}/{dist_name}/{complex_name}: "
                    f"CAPTCHA wrong attempt {attempt + 1} tried={captcha_text!r}"
                )
                time.sleep(3)
                continue

            logger.warning(
                f"ecourts {state_name}/{dist_name}/{complex_name}: unexpected response keys: {list(data.keys())}"
            )
            return []

        logger.warning(
            f"ecourts {state_name}/{dist_name}/{complex_name}: CAPTCHA failed 3 attempts"
        )
        return []

    # ------------------------------------------------------------------ #
    #  CAPTCHA                                                             #
    # ------------------------------------------------------------------ #

    def _solve_captcha_http(
        self, session: requests.Session, captcha_url: Optional[str] = None
    ) -> Optional[str]:
        """Fetch CAPTCHA image and solve with OpenCV pipeline."""
        try:
            if not captcha_url:
                captcha_url = ECOURTS_BASE + "vendor/securimage/securimage_show.php"
            resp = session.get(captcha_url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"ecourts CAPTCHA fetch failed: {e}")
            return None

        text = solve_captcha(resp.content, whitelist=WHITELIST_LOWER)

        try:
            self.db.execute(
                "INSERT INTO captcha_log (source, method, success, cost_inr, solved_at) "
                "VALUES (%s, 'OCR', %s, 0, NOW())",
                (self.source_id, bool(text)),
            )
            self.db.commit()
        except Exception:
            pass

        return text

    # ------------------------------------------------------------------ #
    #  Response parsing                                                    #
    # ------------------------------------------------------------------ #

    def _parse_act_html(
        self,
        html: str,
        state_name: str,
        dist_name: str,
        complex_name: str,
        since: date,
    ) -> List[RawCase]:
        """Parse act_data HTML (from JSON response) into RawCase objects.

        The portal returns an HTML fragment with a results table.
        Typical column order: Sr | Case No | Petitioner | Respondent | Filing Date | Status
        First run: check debug logs to verify column order matches reality.
        """
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        cases: List[RawCase] = []

        # Log first 500 chars on first encounter to verify HTML structure
        logger.debug(
            f"ecourts {state_name}/{dist_name}/{complex_name} act_html[:500]: {html[:500]}"
        )

        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue
            texts = [c.get_text(" ", strip=True) for c in cells]

            # Skip header rows
            if any(t.lower() in ("case no", "case number", "s.no", "sr", "sr.") for t in texts):
                continue

            # Typical column order — verify from debug logs on first run
            case_number = texts[1] if len(texts) > 1 else ""
            petitioner = texts[2] if len(texts) > 2 else ""
            respondent = texts[3] if len(texts) > 3 else ""
            filing_date_str = texts[4] if len(texts) > 4 else ""

            if not case_number or not respondent:
                continue

            filing_date = self._parse_date(filing_date_str)
            if not filing_date or filing_date < since:
                continue

            cases.append(
                RawCase(
                    source="ecourts",
                    case_number=case_number,
                    case_type="SEC_138",
                    court=f"{complex_name}, {dist_name}, {state_name}",
                    filing_date=filing_date,
                    respondent_name=respondent,
                    petitioner_name=petitioner or None,
                    status="Pending",
                    amount_involved=None,
                    raw_data={
                        "state": state_name,
                        "district": dist_name,
                        "complex": complex_name,
                        "raw_cells": texts,
                    },
                )
            )

        return cases

    def _parse_date(self, raw: str) -> Optional[date]:
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None
