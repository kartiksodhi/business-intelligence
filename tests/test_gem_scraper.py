from datetime import date

from ingestion.scrapers.gem import GeMScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_gem_extract_bid_number():
    scraper = GeMScraper(make_db())
    assert scraper._extract_bid_number("GEM/2026/B/987654") == "987654"


def test_gem_extract_gstin():
    scraper = GeMScraper(make_db())
    assert scraper._extract_gstin("Seller GSTIN 07AABCU9603R1ZP") == "07AABCU9603R1ZP"


def test_gem_extract_date():
    scraper = GeMScraper(make_db())
    assert scraper._extract_date("Award Date 18-03-2026") == date(2026, 3, 18)

