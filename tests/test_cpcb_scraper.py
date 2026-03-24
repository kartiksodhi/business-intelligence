from ingestion.scrapers.cpcb import CPCBScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_cpcb_closure_order():
    scraper = CPCBScraper(make_db())
    assert scraper.classify_notice("Closure order issued") == ("CPCB_CLOSURE_ORDER", "CRITICAL")


def test_cpcb_notice():
    scraper = CPCBScraper(make_db())
    assert scraper.classify_notice("Violation notice issued") == ("POLLUTION_NOTICE", "WATCH")

