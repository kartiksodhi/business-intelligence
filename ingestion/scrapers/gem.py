from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

GEM_BIDS_URL = "https://bidplus.gem.gov.in/all-bids"


class GeMScraper(BaseSignalScraper):
    source_id = "gem"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=7)
        awards = await self._fetch_awards(since)
        if not awards:
            return []

        state = self._load_state("gem:last_award_date")
        seen = set(state.get("bid_numbers", []))
        emitted: list[dict] = []

        for award in awards:
            bid_number = award.get("bid_number")
            if not bid_number or bid_number in seen:
                continue
            seen.add(bid_number)

            cin = self._lookup_cin_by_column("gstin", award.get("seller_gstin"))
            if not cin:
                result = self._resolve_entity(award.get("seller_name") or "")
                cin = result.cin if result.cin and result.confidence >= 0.70 else None
            if not cin:
                self._store_unmapped(award.get("seller_name") or "", award)
                continue

            order_value = award.get("order_value_inr") or 0
            event_type = "GEM_NEW_SELLER"
            severity = "INFO"
            if order_value >= 10_000_000:
                event_type = "GEM_LARGE_CONTRACT"
            elif order_value >= 5_000_000:
                event_type = "GEM_ORDER_WON"
            payload = {**award, "matched_cin": cin}
            self._insert_event(cin, event_type, severity, payload)
            emitted.append(payload)

        latest = max((award["award_date"] for award in awards if award.get("award_date")), default=None)
        self._store_state(
            "gem:last_award_date",
            {
                "last_award_date": latest.isoformat() if latest else None,
                "bid_numbers": sorted(seen),
            },
            record_count=len(awards),
        )
        return emitted

    async def _fetch_awards(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0")
            response = await page.goto(GEM_BIDS_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            if response and response.status >= 400:
                logger.warning("gem: blocked or unavailable with status=%s", response.status)
                await browser.close()
                return []
            html = await page.content()
            await browser.close()

        if "captcha" in html.lower() or "attention required" in html.lower():
            logger.warning("gem: public listing appears blocked")
            return []

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tr")
        awards: list[dict] = []
        for row in rows:
            cells = [self.normalize_text(cell.get_text(" ", strip=True)) for cell in row.select("td")]
            if len(cells) < 4:
                continue
            award_date = self._extract_date(" ".join(cells))
            if not award_date or award_date < since:
                continue
            bid_number = self._extract_bid_number(cells[0])
            awards.append(
                {
                    "bid_number": bid_number or cells[0],
                    "title": cells[1] if len(cells) > 1 else None,
                    "buyer_org": cells[2] if len(cells) > 2 else None,
                    "seller_name": cells[3] if len(cells) > 3 else None,
                    "seller_gstin": self._extract_gstin(" ".join(cells)),
                    "order_value_inr": self.parse_amount(" ".join(cells)),
                    "award_date": award_date,
                    "raw_cells": cells,
                }
            )
        return awards

    def _extract_date(self, text: str) -> Optional[date]:
        match = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_bid_number(self, text: str) -> Optional[str]:
        match = re.search(r"(\d{6,})", text)
        return match.group(1) if match else None

    def _extract_gstin(self, text: str) -> Optional[str]:
        match = re.search(r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z\d]Z[A-Z\d]\b", text)
        return match.group(0) if match else None
