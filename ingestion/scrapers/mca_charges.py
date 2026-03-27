from __future__ import annotations

import asyncio
import logging
from typing import Optional

import requests
from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

MCA_CHARGE_URL = "https://www.mca.gov.in/MCA21/dca"
MCA_CHARGE_API_URL = "https://efiling.mca.gov.in/efiling/ApiCall/getChargeInfoList?cinList={cin}"


class MCAChargesScraper(BaseSignalScraper):
    source_id = "mca_charges"
    cadence_hours = 24

    async def run(self) -> list[dict]:
        rows = self._load_target_companies()
        if not rows:
            rows = self._fetchall(
                """
                SELECT cin, company_name
                FROM master_entities
                WHERE health_score IS NOT NULL
                ORDER BY health_score ASC
                LIMIT 200
                """
            )

        emitted: list[dict] = []
        for row in rows:
            cin = row["cin"] if isinstance(row, dict) else row[0]
            if not cin:
                continue
            emitted.extend(await self._process_cin(cin))
            await asyncio.sleep(2)
        self._store_state(
            self.source_id,
            {"processed_companies": len(rows), "emitted_events": len(emitted)},
            record_count=len(rows),
        )
        return emitted

    def _load_target_companies(self) -> list[dict]:
        try:
            return self._fetchall(
                """
                SELECT DISTINCT me.cin, me.company_name
                FROM watchlist_entries we
                JOIN master_entities me ON me.cin = we.cin
                LIMIT 500
                """
            )
        except Exception:
            self._rollback()
            return self._load_watchlist_companies()

    async def _process_cin(self, cin: str) -> list[dict]:
        charges = await self.fetch_charges(cin)
        state_key = f"mca_charge:{cin}:last_checked"
        previous = self._load_state(state_key)
        previous_charges = previous.get("charges", [])
        previous_by_id = {row.get("charge_id"): row for row in previous_charges if row.get("charge_id")}

        if not charges:
            logger.info("mca_charges: no charge data found for cin=%s", cin)
            self._store_state(state_key, {"cin": cin, "charges": []}, record_count=0)
            return []

        emitted: list[dict] = []
        open_lenders: set[str] = set()
        previous_open_lenders = {
            row.get("lender_name")
            for row in previous_charges
            if row.get("status") == "Open" and row.get("lender_name")
        }
        multiple_lenders_emitted = False

        for charge in charges:
            charge_id = charge.get("charge_id")
            previous_charge = previous_by_id.get(charge_id)
            lender_name = charge.get("lender_name")
            if charge.get("status") == "Open" and lender_name:
                open_lenders.add(lender_name)

            if previous_charge:
                previous_amount = previous_charge.get("charge_amount_inr") or 0
                current_amount = charge.get("charge_amount_inr") or 0
                if current_amount > previous_amount:
                    payload = {"cin": cin, **charge, "previous_amount_inr": previous_amount}
                    self._insert_event(cin, "CHARGE_MODIFIED", "WATCH", payload)
                    emitted.append(payload)
                    continue

            event_type, severity = self._classify_charge(previous_charge, charge)
            if event_type:
                payload = {"cin": cin, **charge}
                if event_type == "CHARGE_SATISFIED":
                    severity = "WATCH"
                self._insert_event(cin, event_type, severity or "WATCH", payload)
                emitted.append(payload)

            if not multiple_lenders_emitted and len(open_lenders) >= 3 and len(previous_open_lenders) < 3:
                payload = {"cin": cin, "open_lender_count": len(open_lenders), "charges": charges}
                self._insert_event(cin, "MULTIPLE_LENDERS", "ALERT", payload)
                emitted.append(payload)
                multiple_lenders_emitted = True

        self._store_state(state_key, {"cin": cin, "charges": charges}, record_count=len(charges))
        logger.info("mca_charges: fetched %d charges for cin=%s", len(charges), cin)
        return emitted

    async def fetch_charges(self, cin: str) -> list[dict]:
        api_rows = self._fetch_charges_v3_api(cin)
        if api_rows:
            return api_rows
        return await self._fetch_charges_via_portal(cin)

    def _fetch_charges_v3_api(self, cin: str) -> list[dict]:
        try:
            response = requests.get(
                MCA_CHARGE_API_URL.format(cin=cin),
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"},
            )
            if response.status_code >= 400:
                logger.warning("mca_charges: V3 API returned %s for cin=%s", response.status_code, cin)
                return []
            payload = response.json()
        except Exception as exc:
            logger.warning("mca_charges: V3 API failed for cin=%s: %s", cin, exc)
            return []

        rows = self._extract_charge_rows(payload)
        authorized_capital = self._load_authorized_capital(cin)
        parsed = [self._normalise_charge_row(row, authorized_capital) for row in rows]
        return [row for row in parsed if row.get("charge_id") or row.get("lender_name")]

    async def _fetch_charges_via_portal(self, cin: str) -> list[dict]:
        authorized_capital = self._load_authorized_capital(cin)
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                page = await browser.new_page()
                response = await page.goto(MCA_CHARGE_URL, wait_until="domcontentloaded", timeout=30000)
                if response and response.status >= 400:
                    await browser.close()
                    return []
                if not await self._solve_captcha_with_route(
                    page,
                    "img[id*='captcha'], img[src*='captcha']",
                    "input[id*='captcha'], input[name*='captcha']",
                ):
                    await browser.close()
                    return []

                cin_input = page.locator("input[name*='cin'], input[id*='cin'], input[placeholder*='CIN']")
                if await cin_input.count():
                    await cin_input.first.fill(cin)
                submit_button = page.locator("button:has-text('Submit'), input[type='submit']")
                if await submit_button.count():
                    await submit_button.first.click()
                    await page.wait_for_load_state("networkidle")

                rows = await page.query_selector_all("table tbody tr")
                charges: list[dict] = []
                for row in rows:
                    cells = [self.normalize_text(await cell.inner_text()) for cell in await row.query_selector_all("td")]
                    if cells:
                        charges.append(self.parse_charge_row(cells, authorized_capital=authorized_capital))
                await browser.close()
                return charges
        except Exception as exc:
            logger.warning("mca_charges: MCA portal scrape failed for cin=%s: %s", cin, exc)
            return []

    def _extract_charge_rows(self, payload) -> list[dict]:
        candidates: list[dict] = []
        queue = [payload]
        while queue:
            current = queue.pop(0)
            if isinstance(current, list):
                if current and all(isinstance(item, dict) for item in current):
                    if any(self._looks_like_charge_row(item) for item in current):
                        candidates.extend(item for item in current if isinstance(item, dict))
                        continue
                queue.extend(current)
            elif isinstance(current, dict):
                queue.extend(current.values())
        return candidates

    def _looks_like_charge_row(self, row: dict) -> bool:
        keys = {str(key).lower() for key in row.keys()}
        return any("charge" in key for key in keys) or any("lender" in key or "holder" in key for key in keys)

    def _normalise_charge_row(self, row: dict, authorized_capital: Optional[int]) -> dict:
        charge_id = self._pick_value(row, "chargeid", "charge_id", "srn", "id")
        status = self._pick_value(row, "status", "charge_status")
        lender = self._pick_value(row, "chargeholdername", "lendername", "bankname", "holdername")
        amount = self._pick_value(row, "amount", "chargeamount", "charge_amount")
        creation_date = self._pick_value(row, "createdate", "creationdate", "dateofcreation", "date_of_creation")
        satisfaction_date = self._pick_value(
            row, "satisfactiondate", "dateofsatisfaction", "date_of_satisfaction", "satisfieddate"
        )
        asset_description = self._pick_value(row, "asset", "property", "description", "securedetail")
        return {
            "charge_id": self.normalize_text(str(charge_id or "")) or None,
            "creation_date": self.parse_date(str(creation_date or "")),
            "satisfaction_date": self.parse_date(str(satisfaction_date or "")),
            "lender_name": self.normalize_text(str(lender or "")) or None,
            "charge_amount_inr": self.parse_amount(str(amount or "")),
            "asset_description": self.normalize_text(str(asset_description or "")) or None,
            "status": self._normalise_status(str(status or "")),
            "authorized_capital": authorized_capital,
            "raw_row": row,
        }

    def _normalise_status(self, raw: str) -> Optional[str]:
        lowered = self.normalize_text(raw).lower()
        if not lowered:
            return None
        if "satisf" in lowered or "closed" in lowered:
            return "Satisfied"
        if "open" in lowered or "active" in lowered:
            return "Open"
        return raw.strip() or None

    def _load_authorized_capital(self, cin: str) -> Optional[int]:
        row = self._fetchone("SELECT authorized_capital FROM master_entities WHERE cin = %s", (cin,))
        if not row:
            return None
        return row.get("authorized_capital") if isinstance(row, dict) else row[0]

    def _pick_value(self, payload: dict, *names: str):
        wanted = {name.lower() for name in names}
        for key, value in payload.items():
            key_norm = str(key).lower().replace(" ", "").replace("-", "").replace("_", "")
            if key_norm in wanted:
                return value
        for key, value in payload.items():
            key_norm = str(key).lower().replace(" ", "").replace("-", "").replace("_", "")
            if any(name in key_norm for name in wanted):
                return value
        return None

    def _classify_charge(self, previous: Optional[dict], current: dict) -> tuple[Optional[str], Optional[str]]:
        amount = current.get("charge_amount_inr") or 0
        if not previous:
            if amount > 100_000_000:
                return "CHARGE_EXCEEDS_CAPITAL", "CRITICAL"
            if amount >= 10_000_000:
                return "CHARGE_CREATED", "ALERT"
            return None, None
        if previous.get("status") != "Satisfied" and current.get("status") == "Satisfied":
            return "CHARGE_SATISFIED", "INFO"
        if current.get("authorized_capital") and amount > current["authorized_capital"]:
            return "CHARGE_EXCEEDS_CAPITAL", "CRITICAL"
        return None, None

    def parse_charge_row(self, cells: list[str], authorized_capital: Optional[int] = None) -> dict:
        return {
            "charge_id": cells[0] if len(cells) > 0 else None,
            "creation_date": self.parse_date(cells[1] if len(cells) > 1 else None),
            "satisfaction_date": self.parse_date(cells[2] if len(cells) > 2 else None),
            "lender_name": cells[3] if len(cells) > 3 else None,
            "charge_amount_inr": self.parse_amount(cells[4] if len(cells) > 4 else None),
            "asset_description": cells[5] if len(cells) > 5 else None,
            "status": cells[6] if len(cells) > 6 else None,
            "authorized_capital": authorized_capital,
        }
