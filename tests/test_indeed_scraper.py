from ingestion.scrapers.indeed import IndeedScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_indeed_extract_role_types():
    scraper = IndeedScraper(make_db())
    roles = scraper.extract_role_types("Open operations and hr roles")
    assert roles == ["hr", "operations"]

