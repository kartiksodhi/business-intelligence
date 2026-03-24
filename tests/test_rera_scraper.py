from ingestion.scrapers.rera import RERAScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_rera_revoked():
    scraper = RERAScraper(make_db())
    event = scraper.classify_change({"status": "Registered"}, {"status": "Revoked", "complaints_count": 1})
    assert event == ("RERA_REVOKED", "CRITICAL")


def test_rera_new_project():
    scraper = RERAScraper(make_db())
    event = scraper.classify_change(None, {"status": "Registered", "complaints_count": 0})
    assert event == ("RERA_NEW_PROJECT", "INFO")

