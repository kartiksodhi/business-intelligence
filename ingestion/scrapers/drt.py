from datetime import date
from typing import List

from playwright.async_api import async_playwright

from .base_scraper import BaseScraper, RawCase
import logging
import re

logger = logging.getLogger(__name__)

DRT_URL = "https://drt.gov.in/case-status"

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


class DRTScraper(BaseScraper):
    source_id = "drt"
    cadence_hours = 24

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
        return DRT_BENCHES[offset : offset + 5]

    async def _scrape_bench(self, browser, bench: str, since: date) -> List[RawCase]:
        page = await browser.new_page()
        cases = []

        response = await page.goto(DRT_URL, timeout=30000)
        if response and response.status >= 400:
            await page.close()
            raise RuntimeError(f"drt portal HTTP {response.status} for bench {bench}")

        await page.wait_for_load_state("networkidle")

        # Detect application error pages (200 response with error content)
        page_text = (await page.inner_text("body")).lower()
        if any(phrase in page_text for phrase in ("application error", "server error", "503 service", "an error occurred")):
            await page.close()
            raise RuntimeError(f"drt portal returned application error page for bench {bench}")

        bench_selector = page.locator("select#drt_bench")
        if await bench_selector.count() == 0:
            await page.close()
            raise RuntimeError(f"drt form selectors not found for bench {bench} — site may be down")

        await page.select_option("select#drt_bench", bench)
        await page.select_option("select#case_type_code", "OA")
        await page.fill("input#filing_date_from", since.strftime("%d/%m/%Y"))
        await page.fill("input#filing_date_to", date.today().strftime("%d/%m/%Y"))

        captcha_visible = await page.locator("img#captchaImage").count() > 0 and await page.is_visible(
            "img#captchaImage"
        )
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
            petitioner = texts[1].strip()
            respondent = texts[2].strip()
            filing_date = self._parse_date(texts[3].strip())
            amount_raw = texts[4].strip() if len(texts) > 4 else None
            status = texts[5].strip() if len(texts) > 5 else "Filed"

            if not filing_date or filing_date < since:
                continue

            cases.append(
                RawCase(
                    source="drt",
                    case_number=case_number,
                    case_type="DRT",
                    court=f"DRT {bench}",
                    filing_date=filing_date,
                    respondent_name=respondent,
                    petitioner_name=petitioner,
                    status=status,
                    amount_involved=self._parse_amount(amount_raw),
                    raw_data={"bench": bench, "cells": texts},
                )
            )

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
        raw = raw.replace(",", "").replace("₹", "").strip()
        cr = re.search(r"([\d.]+)\s*[Cc][Rr]", raw)
        if cr:
            return int(float(cr.group(1)) * 10_000_000)
        num = re.search(r"[\d.]+", raw)
        return int(float(num.group())) if num else None

