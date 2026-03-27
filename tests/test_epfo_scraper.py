from ingestion.scrapers.epfo import EPFOScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_epfo_active_to_inactive_fires_lapsed():
    scraper = EPFOScraper(make_db())
    assert scraper.classify_change("Active", "Inactive", 100, 95) == (
        "EPFO_COVERAGE_LAPSED",
        "ALERT",
    )


def test_epfo_same_status_is_noop():
    scraper = EPFOScraper(make_db())
    assert scraper.classify_change("Active", "Active", 100, 75) == (None, None)


def test_epfo_extract_coverage_status():
    scraper = EPFOScraper(make_db())
    assert scraper._extract_coverage_status("Establishment Status: Cancelled") == "Inactive"
