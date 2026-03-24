from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from playwright.async_api import async_playwright

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

RBI_PRESS_RELEASE_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"


class RBINBFCScraper(BaseSignalScraper):
    source_id = "rbi_nbfc"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=7)
        releases = await self._fetch_releases(since)
        if not releases:
            return []

        state = self._load_state("rbi_nbfc:last_release_date")
        seen_hashes = set(state.get("hashes", []))
        emitted: list[dict] = []
        for release in releases:
            release_hash = self.compute_digest(
                [
                    str(release.get("press_release_date")),
                    release.get("entity_name"),
                    release.get("action_type"),
                ]
            )
            if release_hash in seen_hashes:
                continue
            seen_hashes.add(release_hash)

            cin = self._resolve_cin(release.get("entity_name") or "")
            if not cin:
                self._store_unmapped(release.get("entity_name") or "", release)
                continue
            event_type, severity = self._classify_action(release.get("action_type") or "")
            payload = {**release, "release_hash": release_hash}
            self._insert_event(cin, event_type, severity, payload)
            emitted.append(payload)

        latest = max(
            (release["press_release_date"] for release in releases if release.get("press_release_date")),
            default=None,
        )
        self._store_state(
            "rbi_nbfc:last_release_date",
            {
                "last_release_date": latest.isoformat() if latest else None,
                "hashes": sorted(seen_hashes),
            },
            record_count=len(releases),
        )
        return emitted

    async def _fetch_releases(self, since: date) -> list[dict]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(RBI_PRESS_RELEASE_URL, wait_until="networkidle", timeout=30000)
            rows = await page.query_selector_all("table tr, .content tr, .list tr, .list li")
            releases: list[dict] = []
            for row in rows:
                text = self.normalize_text(await row.inner_text())
                if not self._looks_relevant(text):
                    continue
                press_date = self._extract_date(text)
                if not press_date or press_date < since:
                    continue
                link = await row.query_selector("a")
                title = self.normalize_text(await link.inner_text()) if link else text
                href = await link.get_attribute("href") if link else None
                entity_name = self._extract_entity_name(title)
                action_type = self._extract_action_type(text)
                releases.append(
                    {
                        "press_release_date": press_date,
                        "title": title,
                        "entity_name": entity_name,
                        "action_type": action_type,
                        "url": href,
                    }
                )
            await browser.close()
            return releases

    def _looks_relevant(self, text: str) -> bool:
        lowered = text.lower()
        return any(
            token in lowered
            for token in ("enforcement", "cancellation of cor", "certificate of registration", "penalty", "restriction", "warning")
        )

    def _extract_date(self, text: str) -> Optional[date]:
        match = re.search(r"(\d{2}[./-]\d{2}[./-]\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
        return self.parse_date(match.group(1)) if match else None

    def _extract_entity_name(self, title: str) -> str:
        title = re.sub(r"^\d{1,2}[./-]\d{1,2}[./-]\d{4}\s*", "", title).strip()
        if "against" in title.lower():
            return title.split("against", 1)[-1].strip()
        return title

    def _extract_action_type(self, text: str) -> str:
        lowered = text.lower()
        if "cancellation" in lowered or "certificate of registration" in lowered:
            return "cancellation"
        if "restriction" in lowered or "restrictions" in lowered:
            return "restriction"
        if "warning" in lowered:
            return "warning"
        return "penalty"

    def _resolve_cin(self, entity_name: str) -> Optional[str]:
        result = self._resolve_entity(entity_name)
        if result.cin and result.confidence >= 0.70:
            return result.cin
        return None

    def _classify_action(self, action_type: str) -> tuple[str, str]:
        lowered = action_type.lower()
        if lowered == "cancellation":
            return "RBI_LICENSE_CANCELLED", "CRITICAL"
        if lowered == "restriction":
            return "RBI_RESTRICTION", "ALERT"
        if lowered == "warning":
            return "RBI_WARNING", "WATCH"
        return "RBI_ENFORCEMENT", "ALERT"
