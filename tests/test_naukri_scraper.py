from ingestion.scrapers.naukri import NaukriScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_naukri_extract_role_types():
    scraper = NaukriScraper(make_db())
    roles = scraper.extract_role_types("Hiring legal, engineering and finance staff")
    assert roles == ["finance", "legal", "engineering"]

