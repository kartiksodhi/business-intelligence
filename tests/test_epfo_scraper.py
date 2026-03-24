from ingestion.scrapers.epfo import EPFOScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_epfo_cancelled_status_fires_delisted():
    scraper = EPFOScraper(make_db())
    assert scraper.classify_change("Covered", "Cancelled", 100, 95) == (
        "EPFO_ESTABLISHMENT_DELISTED",
        "CRITICAL",
    )


def test_epfo_contribution_drop():
    scraper = EPFOScraper(make_db())
    assert scraper.classify_change("Covered", "Covered", 100, 75) == (
        "EPFO_CONTRIBUTION_DROP",
        "ALERT",
    )


def test_epfo_hiring_surge():
    scraper = EPFOScraper(make_db())
    assert scraper.classify_change("Covered", "Covered", 100, 125) == (
        "EPFO_HIRING_SURGE",
        "INFO",
    )

