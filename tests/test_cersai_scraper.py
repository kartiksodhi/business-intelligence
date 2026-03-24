from ingestion.scrapers.cersai import CERSAIScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_cersai_new_security_interest():
    scraper = CERSAIScraper(make_db())
    event = scraper.classify_security_interest(None, {"amount_inr": 15_000_000}, open_count=1)
    assert event == ("CERSAI_NEW_SI", "ALERT")


def test_cersai_multiple_lenders():
    scraper = CERSAIScraper(make_db())
    event = scraper.classify_security_interest({}, {"amount_inr": 1}, open_count=3)
    assert event == ("CERSAI_MULTIPLE_LENDERS", "ALERT")

