from datetime import date
from typing import List

import httpx
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, RawCase
import logging
import re

logger = logging.getLogger(__name__)

IBBI_BASE = "https://ibbi.gov.in"
IBBI_ORDERS_URL = f"{IBBI_BASE}/search/index/orders"


class IBBIScraper(BaseScraper):
    source_id = "ibbi"
    cadence_hours = 168

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []
        orders = await self._fetch_orders(since)
        cases.extend(orders)
        return cases

    async def _fetch_orders(self, since: date) -> List[RawCase]:
        cases = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            page_num = 1
            while True:
                resp = await client.get(
                    IBBI_ORDERS_URL,
                    params={
                        "page": page_num,
                        "search_year": since.strftime("%Y"),
                        "search_month": since.strftime("%m"),
                        "search_sort_by_date": 1,
                    },
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select("div.search_cont_bx")

                if not rows:
                    break

                hit_cutoff = False
                for row in rows:
                    title_link = row.select_one("a")
                    if title_link is None:
                        continue
                    title = title_link.get_text(" ", strip=True)
                    case_ref = self._extract_case_ref(title)
                    company_name = self._extract_company_name(title)
                    order_type = self._classify_order_type(title)

                    published_on = None
                    for item in row.select("ul li"):
                        text = item.get_text(" ", strip=True)
                        if text.upper().startswith("PUBLISHED ON"):
                            published_on = text.split(":", 1)[-1].strip()
                            break

                    order_date = self._parse_date(published_on or "")
                    if not order_date:
                        href_date = self._parse_href_date(title_link.get("href", ""))
                        order_date = href_date
                    if not order_date:
                        continue
                    if order_date < since:
                        hit_cutoff = True
                        break

                    exists = self.db.execute(
                        "SELECT 1 FROM legal_events WHERE case_number=%s AND source='ibbi'",
                        (case_ref,),
                    ).fetchone()
                    if exists:
                        continue

                    if "LIQUIDATION" in order_type:
                        status = "Liquidation Ordered"
                    elif "CIRP" in order_type or "COMMENCEMENT" in order_type:
                        status = "CIRP Commenced"
                    elif "RESOLUTION PLAN" in order_type:
                        status = "Resolution Plan Approved"
                    else:
                        status = order_type

                    cases.append(
                        RawCase(
                            source="ibbi",
                            case_number=case_ref,
                            case_type="NCLT_7",
                            court="IBBI",
                            filing_date=order_date,
                            respondent_name=company_name,
                            petitioner_name=None,
                            status=status,
                            amount_involved=None,
                            raw_data={
                                "title": title,
                                "order_type": order_type,
                                "published_on": published_on,
                                "document_url": title_link.get("href"),
                            },
                        )
                    )

                if hit_cutoff:
                    break
                page_num += 1

        return cases

    def _extract_company_name(self, title: str) -> str:
        if "-" in title:
            tail = title.split("-", 1)[-1].strip()
            if tail:
                return re.sub(r"\s*\[[^\]]+\]\s*$", "", tail).strip()
        matter_match = re.search(r"In the matter of\s+(.+?)(?:\s*\[|$)", title, re.IGNORECASE)
        if matter_match:
            return matter_match.group(1).strip()
        return title.strip()

    def _extract_case_ref(self, title: str) -> str:
        match = re.search(r"\[([^\]]+)\]", title)
        if match:
            return match.group(1).strip()
        return title.strip()

    def _classify_order_type(self, title: str) -> str:
        title_upper = title.upper()
        if "LIQUIDATION" in title_upper or "(LIQ" in title_upper:
            return "LIQUIDATION"
        if "RESOLUTION PLAN" in title_upper:
            return "RESOLUTION PLAN"
        if "CP(IB)" in title_upper or "IB-" in title_upper or "CIRP" in title_upper:
            return "CIRP"
        return title_upper

    def _parse_href_date(self, href: str):
        match = re.search(r"(\d{4}-\d{2}-\d{2})", href)
        if not match:
            return None
        return self._parse_date(match.group(1))

    def _parse_date(self, raw: str):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
            try:
                from datetime import datetime

                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
        return None
