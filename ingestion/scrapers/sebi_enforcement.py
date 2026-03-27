from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

SEBI_ENFORCEMENT_URL = "https://www.sebi.gov.in/enforcement/orders/"
SEBI_ENFORCEMENT_FALLBACK_URL = "https://www.sebi.gov.in/enforcement/orders.html"
SEBI_NEWSLIST_URL = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"

SEBI_ORDER_CATEGORIES = {
    "2": "Orders of Chairperson/Members",
    "3": "Settlement Order",
    "6": "Orders of AO",
    "133": "Orders of ED / CGM (Quasi-Judicial Authorities)",
    "138": "Orders under Regulation 30A of the SEBI (Intermediaries) Regulations, 2008",
}


class SEBIEnforcementScraper(BaseSignalScraper):
    source_id = "sebi_enforcement"
    cadence_hours = 168

    async def run(self) -> list[dict]:
        since = date.today() - timedelta(days=30)
        orders = self._fetch_orders(since)
        if not orders:
            self._store_state(self.source_id, {"orders_seen": 0, "emitted_events": 0}, record_count=0)
            return []

        state = self._load_state("sebi_enforcement:last_order_date")
        seen_hashes = set(state.get("hashes", []))
        emitted: list[dict] = []

        for order in orders:
            order_hash = self.compute_digest(
                [order.get("entity_name"), str(order.get("order_date")), order.get("order_type"), order.get("document_link")]
            )
            if order_hash in seen_hashes:
                continue
            seen_hashes.add(order_hash)

            cin = self._resolve_cin(order.get("entity_name") or "")
            event_type, severity = self._classify_order(order.get("title") or "", order.get("order_type") or "")
            payload = {**order, "order_hash": order_hash}
            if cin and event_type:
                self._insert_event(cin, event_type, severity, payload)
                emitted.append(payload)
            else:
                self._store_unmapped(order.get("entity_name") or "", payload)

        latest = max((order["order_date"] for order in orders if order.get("order_date")), default=None)
        self._store_state(
            "sebi_enforcement:last_order_date",
            {"last_order_date": latest.isoformat() if latest else None, "hashes": sorted(seen_hashes)},
            record_count=len(orders),
        )
        self._store_state(
            self.source_id,
            {"orders_seen": len(orders), "emitted_events": len(emitted)},
            record_count=len(orders),
        )
        logger.info("sebi_enforcement: fetched %d recent orders", len(orders))
        return emitted

    def _fetch_orders(self, since: date) -> list[dict]:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml,*/*"})

        # Touch the public entrypoints first so we honor the live/fallback URLs.
        try:
            primary = session.get(SEBI_ENFORCEMENT_URL, timeout=20)
            if primary.status_code == 403:
                logger.info("sebi_enforcement: primary URL blocked, falling back to HTML listings")
        except Exception as exc:
            logger.warning("sebi_enforcement: primary URL probe failed: %s", exc)
        try:
            session.get(SEBI_ENFORCEMENT_FALLBACK_URL, timeout=20)
        except Exception as exc:
            logger.warning("sebi_enforcement: fallback URL probe failed: %s", exc)

        orders: list[dict] = []
        seen_links: set[str] = set()
        for smid, category_name in SEBI_ORDER_CATEGORIES.items():
            response = session.post(
                SEBI_NEWSLIST_URL,
                data={
                    "nextValue": "0",
                    "next": "s",
                    "search": "",
                    "fromDate": since.strftime("%d-%m-%Y"),
                    "toDate": date.today().strftime("%d-%m-%Y"),
                    "fromYear": "",
                    "toYear": "",
                    "deptId": "-1",
                    "sid": "2",
                    "ssid": "9",
                    "smid": smid,
                    "ssidhidden": "9",
                    "intmid": "-1",
                    "sText": "Enforcement",
                    "ssText": "Orders",
                    "smText": category_name,
                    "doDirect": "-1",
                },
                timeout=30,
            )
            if response.status_code >= 400:
                logger.warning("sebi_enforcement: category fetch failed for smid=%s status=%s", smid, response.status_code)
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            for row in soup.select("table tr"):
                cells = row.select("td")
                if len(cells) < 2:
                    continue
                order_date = self.parse_date(cells[0].get_text(" ", strip=True))
                if not order_date or order_date < since:
                    continue
                link = cells[1].select_one("a[href]")
                title = self.normalize_text(cells[1].get_text(" ", strip=True))
                document_link = urljoin(SEBI_ENFORCEMENT_FALLBACK_URL, link["href"]) if link else None
                if document_link and document_link in seen_links:
                    continue
                if document_link:
                    seen_links.add(document_link)
                orders.append(
                    {
                        "order_date": order_date,
                        "entity_name": self._extract_entity_name(title),
                        "order_type": self._extract_order_type(title or category_name),
                        "order_category": category_name,
                        "document_link": document_link,
                        "title": title,
                    }
                )
        return orders

    def _resolve_cin(self, entity_name: str) -> Optional[str]:
        result = self._resolve_entity(entity_name)
        if result.cin and result.confidence >= 0.70:
            return result.cin
        return None

    def _extract_entity_name(self, title: str) -> str:
        match = re.search(r"in the matter of\s+(.+)$", title, re.IGNORECASE)
        if match:
            return match.group(1).strip(" .")
        match = re.search(r"in respect of\s+(.+)$", title, re.IGNORECASE)
        if match:
            return match.group(1).strip(" .")
        match = re.search(r"against\s+(.+)$", title, re.IGNORECASE)
        if match:
            return match.group(1).strip(" .")
        return re.sub(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{4}\s*", "", title).strip()

    def _extract_order_type(self, text: str) -> str:
        lowered = text.lower()
        if "show cause" in lowered or "enquiry" in lowered:
            return "show_cause"
        if "interim" in lowered or "confirmatory" in lowered or "ad-interim" in lowered:
            return "interim"
        if "final order" in lowered or lowered.startswith("final"):
            return "final"
        if "settlement" in lowered:
            return "settlement"
        return "order"

    def _classify_order(self, title: str, order_type: str) -> tuple[Optional[str], Optional[str]]:
        lowered = f"{order_type} {title}".lower()
        if "show cause" in lowered or "enquiry" in lowered:
            return "SEBI_SHOW_CAUSE", "ALERT"
        if "interim" in lowered or "confirmatory" in lowered or "ad-interim" in lowered:
            return "SEBI_INTERIM_ORDER", "CRITICAL"
        if "final" in lowered:
            return "SEBI_FINAL_ORDER", "ALERT"
        if "settlement" in lowered:
            return "SEBI_SETTLEMENT", "WATCH"
        return "SEBI_FINAL_ORDER", "ALERT"
