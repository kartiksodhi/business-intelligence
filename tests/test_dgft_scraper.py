from ingestion.scrapers.dgft import DGFTScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_dgft_extract_pan():
    scraper = DGFTScraper(make_db())
    assert scraper._extract_pan("PAN AABCU9603R") == "AABCU9603R"


def test_dgft_extract_status():
    scraper = DGFTScraper(make_db())
    assert scraper._extract_status("IEC status cancelled by authority") == "Cancelled"


def test_dgft_classify_surrendered():
    scraper = DGFTScraper(make_db())
    assert scraper._classify_status("Surrendered") == ("IEC_SURRENDERED", "WATCH")

