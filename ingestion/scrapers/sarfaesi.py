from datetime import date
from typing import List

import httpx
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, RawCase
import logging
import re

logger = logging.getLogger(__name__)

IBAPI_AUCTIONS_URL = "https://ibapi.in/auctions"

BANK_NOTICE_PAGES = [
    {
        "bank": "SBI",
        "url": "https://sbi.co.in/web/sbi-in-the-news/auction-notices/sarfaesi-and-others",
        "mode": "sbi_auction_table",
    },
    {"bank": "PNB", "url": "https://www.pnbindia.in/SARFAESI-Notices.html", "mode": "stub"},
    {"bank": "BOB", "url": "https://www.bankofbaroda.in/sarfaesi-notices", "mode": "stub"},
]


class SARFAESIScraper(BaseScraper):
    source_id = "sarfaesi"
    cadence_hours = 24

    async def fetch_new_cases(self, since: date) -> List[RawCase]:
        cases = []

        auction_cases = await self._scrape_ibapi_auctions(since)
        cases.extend(auction_cases)

        for bank_cfg in BANK_NOTICE_PAGES:
            try:
                bank_cases = await self._scrape_bank_notices(bank_cfg, since)
                cases.extend(bank_cases)
            except Exception as e:
                logger.error(f"sarfaesi bank {bank_cfg['bank']} failed: {e}")

        return cases

    async def _scrape_ibapi_auctions(self, since: date) -> List[RawCase]:
        cases = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(
                IBAPI_AUCTIONS_URL,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table.auction-table tbody tr")

            if not rows:
                error_text = soup.get_text(" ", strip=True)
                if "Sorry, an error has occured." in error_text:
                    logger.warning("ibapi auctions returned an application error page")
                return []

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue

                borrower_name = cells[1].get_text(strip=True)
                bank_name = cells[2].get_text(strip=True)
                auction_date_raw = cells[3].get_text(strip=True)
                reserve_price_raw = cells[4].get_text(strip=True)
                case_ref = cells[0].get_text(strip=True)

                auction_date = self._parse_date(auction_date_raw)
                if not auction_date or auction_date < since:
                    continue

                cases.append(
                    RawCase(
                        source="sarfaesi",
                        case_number=case_ref,
                        case_type="SARFAESI_AUCTION",
                        court=f"SARFAESI Auction ({bank_name})",
                        filing_date=auction_date,
                        respondent_name=borrower_name,
                        petitioner_name=bank_name,
                        status="Auction Scheduled",
                        amount_involved=self._parse_amount(reserve_price_raw),
                        raw_data={"cells": [c.get_text(strip=True) for c in cells]},
                    )
                )

        return cases

    async def _scrape_bank_notices(self, bank_cfg: dict, since: date) -> List[RawCase]:
        if bank_cfg.get("mode") == "stub":
            logger.warning("sarfaesi %s structure not yet confirmed; returning stub result", bank_cfg["bank"])
            return []

        cases = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(bank_cfg["url"], headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table#sarfesitable tbody tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                description = cells[0].get_text(" ", strip=True)
                notice_date_raw = cells[1].get_text(strip=True)
                notice_date = self._parse_date(notice_date_raw)
                if not notice_date or notice_date < since:
                    continue

                links = cells[2].select("li a")
                for link in links:
                    borrower = re.sub(r"^\d+\.\s*", "", link.get_text(" ", strip=True))
                    case_num = f"{bank_cfg['bank']}-{notice_date_raw}-{borrower[:20]}"
                    cases.append(
                        RawCase(
                            source="sarfaesi",
                            case_number=case_num,
                            case_type="SARFAESI_AUCTION",
                            court=f"SARFAESI ({bank_cfg['bank']})",
                            filing_date=notice_date,
                            respondent_name=borrower,
                            petitioner_name=bank_cfg["bank"],
                            status="Auction Notice",
                            amount_involved=None,
                            raw_data={
                                "bank": bank_cfg["bank"],
                                "description": description,
                                "document_text": link.get_text(" ", strip=True),
                                "document_url": link.get("href"),
                            },
                        )
                    )

        return cases

    def _parse_date(self, raw: str):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%d.%m.%Y"):
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

