from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

EPFO_PORTAL_URL = "https://unifiedportal-emp.epfindia.gov.in/publicPortal/no-auth/misReport/home/loadEstSearchHome"


class EPFOScraper(BaseSignalScraper):
    source_id = "epfo"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        monitored = self._load_watchlisted_companies()
        if not monitored:
            return []

        emitted: list[dict] = []
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                for company in monitored:
                    result = await self.retry_with_backoff(
                        lambda: self._search_company(browser, company),
                        retries=3,
                        initial_delay=2.0,
                    )
                    if result is None:
                        logger.warning("epfo: blocked or unavailable for cin=%s", company["cin"])
                        continue
                    if not result:
                        await asyncio.sleep(2)
                        continue

                    establishment_code = result.get("establishment_code")
                    if establishment_code and not company.get("epfo_id"):
                        self._store_establishment_code(company["cin"], establishment_code)

                    state_key = f"epfo:{company['cin']}"
                    previous = self._load_state(state_key)
                    self._store_state(state_key, result, record_count=1)

                    event_type, severity = self.classify_change(
                        previous.get("coverage_status"),
                        result.get("coverage_status"),
                        None,
                        None,
                    )
                    if event_type:
                        payload = {"cin": company["cin"], **result}
                        self._insert_event(company["cin"], event_type, severity, payload)
                        emitted.append(payload)

                    logger.info("epfo: contribution data not available on public portal")
                    await asyncio.sleep(2)

                await browser.close()
        except PlaywrightTimeoutError:
            logger.warning("epfo: timeout while probing public portal")
            return []
        except Exception as exc:
            logger.warning("epfo: portal probe failed: %s", exc)
            return []

        return emitted

    def _load_watchlisted_companies(self) -> list[dict]:
        try:
            rows = self._fetchall(
                """
                SELECT DISTINCT me.cin, me.company_name, me.epfo_id
                FROM master_entities me
                JOIN watchlists w ON w.is_active = TRUE
                WHERE me.status = 'Active'
                  AND (w.cin_list IS NULL OR me.cin = ANY(w.cin_list))
                ORDER BY me.cin
                """,
            )
        except Exception as exc:
            logger.warning("epfo: unable to load watchlisted companies: %s", exc)
            return []

        monitored: list[dict] = []
        for row in rows:
            if isinstance(row, dict):
                cin = row.get("cin")
                company_name = row.get("company_name")
                epfo_id = row.get("epfo_id")
            else:
                cin, company_name, epfo_id = row[0], row[1], row[2]
            if cin and company_name:
                monitored.append(
                    {"cin": cin, "company_name": company_name, "epfo_id": epfo_id}
                )
        return monitored

    async def _search_company(self, browser, company: dict) -> Optional[dict]:
        page = await browser.new_page(user_agent="Mozilla/5.0")
        response = await page.goto(EPFO_PORTAL_URL, wait_until="domcontentloaded", timeout=20000)
        if not response or response.status >= 400:
            await page.close()
            return None
        if not await self._has_search_ui(page):
            await page.close()
            return None

        await page.fill("#estName", company["company_name"][:100])
        if company.get("epfo_id"):
            await page.fill("#estCode", str(company["epfo_id"])[:7])
        if not await self._solve_captcha_with_route(page, "#capImg", "#captcha"):
            await page.close()
            return None

        await page.click("#searchEmployer")
        await page.wait_for_timeout(2000)
        result = await self._extract_result(page)
        await page.close()
        return result

    async def _has_search_ui(self, page) -> bool:
        text = (await page.content()).lower()
        if "establishment search" in text and await page.locator("#estName").count():
            return True
        return False

    async def _extract_result(self, page) -> dict:
        candidate_containers = ["#tablecontainer", "#tablecontainer4", "#tablecontainer5"]
        texts: list[str] = []
        for selector in candidate_containers:
            locator = page.locator(selector)
            if await locator.count():
                value = self.normalize_text(await locator.inner_text())
                if value:
                    texts.append(value)
        merged = " ".join(texts)
        if not merged:
            return {}
        establishment_code = self._extract_establishment_code(merged)
        establishment_name = self._extract_establishment_name(merged)
        coverage_status = self._extract_coverage_status(merged)
        if not establishment_code and not establishment_name:
            return {}
        return {
            "establishment_code": establishment_code,
            "establishment_name": establishment_name,
            "coverage_status": coverage_status,
        }

    def _store_establishment_code(self, cin: str, establishment_code: str) -> None:
        try:
            self._execute(
                """
                UPDATE master_entities
                SET epfo_id = %s
                WHERE cin = %s
                  AND (epfo_id IS NULL OR epfo_id = '')
                """,
                (establishment_code, cin),
            )
            self._commit()
        except Exception as exc:
            logger.warning("epfo: failed to store establishment code for %s: %s", cin, exc)

    def _extract_establishment_code(self, text: str) -> Optional[str]:
        match = re.search(r"\b(\d{7})\b", text)
        return match.group(1) if match else None

    def _extract_establishment_name(self, text: str) -> Optional[str]:
        match = re.search(
            r"(?:name of establishment|establishment name)\s*[:\-]?\s*([A-Za-z0-9/&(),.\- ]+)",
            text,
            re.IGNORECASE,
        )
        if match:
            return self.normalize_text(match.group(1))
        line = text.split("Validity Status", 1)[0]
        return self.normalize_text(line[:200]) if line else None

    def _extract_coverage_status(self, text: str) -> Optional[str]:
        lowered = text.lower()
        if "cancelled" in lowered:
            return "Cancelled"
        if "exempted" in lowered:
            return "Exempted"
        if "covered" in lowered or "active" in lowered:
            return "Covered"
        return None

    def classify_change(
        self,
        previous_status: str | None,
        current_status: str | None,
        previous_count: int | None,
        current_count: int | None,
    ) -> tuple[str | None, str | None]:
        before = (previous_status or "").lower()
        after = (current_status or "").lower()
        if before and after in {"cancelled", "exempted"} and before != after:
            return "EPFO_ESTABLISHMENT_DELISTED", "CRITICAL"
        if previous_count and current_count is not None:
            if current_count <= previous_count * 0.8:
                return "EPFO_CONTRIBUTION_DROP", "ALERT"
            if current_count >= previous_count * 1.2:
                return "EPFO_HIRING_SURGE", "INFO"
        return None, None
