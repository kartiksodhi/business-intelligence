from __future__ import annotations

import logging
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

RBI_WILFUL_URL = "https://www.rbi.org.in/Scripts/bs_viewcontent.aspx?Id=2691"


class RBIWilfulDefaulterScraper(BaseSignalScraper):
    source_id = "rbi_wilful_defaulter"
    cadence_hours = 24 * 90

    async def run(self) -> list[dict]:
        rows = await self._fetch_rows()
        if not rows:
            return []

        table_hash = self.compute_digest(rows)
        state_key = "rbi_wilful_defaulter:full_list"
        previous = self._load_state(state_key)
        previous_rows = previous.get("rows", [])
        previous_hash = previous.get("table_hash")

        self._store_state(state_key, {"table_hash": table_hash, "rows": rows}, record_count=len(rows))
        if previous_hash == table_hash:
            return []

        seen = {self._row_key(item) for item in previous_rows}
        fresh = [row for row in rows if self._row_key(row) not in seen]
        emitted: list[dict] = []

        for row in fresh:
            emitted.extend(self._emit_for_row(row))
        return emitted

    async def _fetch_rows(self) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            response = await page.goto(RBI_WILFUL_URL, wait_until="networkidle", timeout=30000)
            if response and response.status >= 400:
                logger.warning("rbi_wilful_defaulter: RBI returned %s", response.status)
                await browser.close()
                return []

            rows = await page.query_selector_all("table tr")
            parsed: list[dict] = []
            for row in rows:
                cells = [self.normalize_text(await cell.inner_text()) for cell in await row.query_selector_all("td")]
                if len(cells) < 2:
                    continue
                parsed.append(
                    {
                        "name": cells[0],
                        "identifier": cells[1] if len(cells) > 1 else None,
                        "lender": cells[2] if len(cells) > 2 else None,
                        "amount": self.parse_amount(cells[3] if len(cells) > 3 else None),
                        "raw_cells": cells,
                    }
                )
            await browser.close()
            return parsed

    def _emit_for_row(self, row: dict) -> list[dict]:
        emitted: list[dict] = []
        name = row.get("name") or ""
        cin = self._resolve_cin_for_row(row)
        director_cin = self._lookup_director_cin(name)

        if cin:
            payload = {"matched_cin": cin, **row}
            self._insert_event(cin, "WILFUL_DEFAULT_ADDED", "CRITICAL", payload)
            emitted.append(payload)
        else:
            self._store_unmapped(name, row)

        if director_cin:
            payload = {"director_name": name, **row}
            self._insert_event(director_cin, "WILFUL_DEFAULT_DIRECTOR", "ALERT", payload)
            emitted.append(payload)
        return emitted

    def _resolve_cin_for_row(self, row: dict) -> Optional[str]:
        identifier = row.get("identifier") or ""
        if isinstance(identifier, str) and len(identifier.strip()) == 21:
            return identifier.strip()
        result = self._resolve_entity(row.get("name") or "")
        if result.cin and result.confidence >= 0.70:
            return result.cin
        return None

    @staticmethod
    def _row_key(row: dict) -> str:
        return "|".join(
            str(row.get(field) or "")
            for field in ("name", "identifier", "lender", "amount")
        )

