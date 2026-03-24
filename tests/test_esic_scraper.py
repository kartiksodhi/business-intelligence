from ingestion.scrapers.esic import ESICScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_esic_new_registration():
    scraper = ESICScraper(make_db())
    assert scraper.classify_change(None, "Covered") == ("ESIC_NEW", "INFO")


def test_esic_cancelled():
    scraper = ESICScraper(make_db())
    assert scraper.classify_change("Covered", "Cancelled") == ("ESIC_CANCELLED", "ALERT")

