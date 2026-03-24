from datetime import date
from typing import List

from playwright.async_api import async_playwright

from .base_scraper import BaseScraper, RawCase
import logging
import re

logger = logging.getLogger(__name__)

NCLT_URL = "https://efiling.nclt.gov.in/nclt/public/case_status.php"

NCLT_BENCHES = {
    "Mumbai": "9",
    "New Delhi": "11",
    "Principal Bench": "10",
    "Ahmedabad": "1",
    "Chennai": "5",
    "Kolkata": "8",
    "Hyderabad": "7",
    "Allahabad": "2",
    "Chandigarh": "4",
    "Guwahati": "6",
    "Amaravati": "13",
    "Jaipur": "12",
    "Kochi": "15",
    "Cuttack": "14",
    "Indore": "16",
}

IBC_SECTIONS = {
    "IBC_7": "NCLT_7",
    "IBC_9": "NCLT_9",
    "IBC_10": "NCLT_10",
}


class NCLTScraper(BaseScraper):
    source_id = "nclt"
    cadence_hours = 24

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            for section_label, case_type in IBC_SECTIONS.items():
                for bench, bench_value in NCLT_BENCHES.items():
                    try:
                        batch = await self._scrape_bench(
                            browser, bench, bench_value, section_label, case_type, since
                        )
                        cases.extend(batch)
                    except Exception as e:
                        logger.error(f"nclt {bench} {section_label} failed: {e}")
            await browser.close()
        return cases

    async def _scrape_bench(
        self,
        browser,
        bench: str,
        bench_value: str,
        section_label: str,
        case_type: str,
        since: date,
    ) -> List[RawCase]:
        page = await browser.new_page()
        cases = []

        try:
            await page.goto(NCLT_URL, timeout=30000)
            await page.wait_for_load_state("domcontentloaded")

            await page.select_option("select#schemaname", bench_value)
            await page.select_option("select[name='search_by']", "filing_date")
            await page.fill("input#from", since.strftime("%d/%m/%Y"))
            await page.fill("input#to", date.today().strftime("%d/%m/%Y"))

            solved = await self._solve_captcha(page, "img.imgcaptcha", "input[name='answer']")
            if not solved:
                logger.warning(f"nclt {bench}: CAPTCHA unsolved")
                return []

            await page.click("input#cryptstr")
            # Use domcontentloaded + brief settle — networkidle is fragile on NCLT redirects
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1500)

            rows = await page.query_selector_all(
                "table.table.table-hover.table-bordered.table-striped tbody tr"
            )
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 6:
                    continue
                texts = [await c.inner_text() for c in cells]
                if not self._row_matches_section(texts, section_label):
                    continue

                case_number = texts[0].strip()
                petitioner = texts[1].strip()
                respondent = texts[2].strip()
                filing_date = self._parse_date(texts[3].strip())
                status = texts[4].strip()
                amount_raw = texts[5].strip() if len(texts) > 5 else None

                if not filing_date or filing_date < since:
                    continue

                amount = self._parse_amount(amount_raw) if amount_raw else None
                cases.append(
                    RawCase(
                        source="nclt",
                        case_number=case_number,
                        case_type=case_type,
                        court=f"NCLT {bench}",
                        filing_date=filing_date,
                        respondent_name=respondent,
                        petitioner_name=petitioner,
                        status=status,
                        amount_involved=amount,
                        raw_data={"bench": bench, "section": section_label, "cells": texts},
                    )
                )
        except Exception as exc:
            err = str(exc)
            if "Execution context was destroyed" in err or "Target page, context or browser has been closed" in err:
                raise RuntimeError(f"nclt {bench} page context destroyed mid-scrape — likely redirect") from exc
            raise
        finally:
            await page.close()

        return cases

    def _row_matches_section(self, texts: List[str], section_label: str) -> bool:
        haystack = " ".join(texts).upper()
        section_number = section_label.split("_")[-1]
        patterns = [
            rf"\bIBC\s*{section_number}\b",
            rf"\bSECTION\s*{section_number}\b",
            rf"\bSEC(?:TION)?\.?\s*{section_number}\b",
            rf"\bU/S\s*{section_number}\b",
        ]
        return any(re.search(pattern, haystack) for pattern in patterns)

    def _parse_date(self, raw: str):
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                from datetime import datetime

                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_amount(self, raw: str) -> int:
        raw = raw.replace(",", "").replace("₹", "").strip()
        cr_match = re.search(r"([\d.]+)\s*[Cc][Rr]", raw)
        if cr_match:
            return int(float(cr_match.group(1)) * 10_000_000)
        lakh_match = re.search(r"([\d.]+)\s*[Ll][Aa][Kk][Hh]", raw)
        if lakh_match:
            return int(float(lakh_match.group(1)) * 100_000)
        num = re.search(r"[\d.]+", raw)
        if num:
            return int(float(num.group()))
        return None

