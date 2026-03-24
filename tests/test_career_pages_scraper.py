from ingestion.scrapers.career_pages import CareerPagesScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_career_pages_extract_job_count():
    scraper = CareerPagesScraper(make_db())
    assert scraper.extract_job_count("12 open positions across engineering and sales") == 12

