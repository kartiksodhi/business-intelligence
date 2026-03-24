from ingestion.scrapers.udyam import UdyamScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_udyam_new_registration():
    scraper = UdyamScraper(make_db())
    assert scraper._classify_transition({}, {"classification": "Micro", "status": "Active"}) == (
        "UDYAM_NEW",
        "INFO",
    )


def test_udyam_upgrade():
    scraper = UdyamScraper(make_db())
    assert scraper._classify_transition(
        {"classification": "Micro", "status": "Active"},
        {"classification": "Small", "status": "Active"},
    ) == ("UDYAM_CLASSIFICATION_UPGRADE", "INFO")


def test_udyam_cancelled():
    scraper = UdyamScraper(make_db())
    assert scraper._classify_transition(
        {"classification": "Small", "status": "Active"},
        {"classification": "Small", "status": "Cancelled"},
    ) == ("UDYAM_CANCELLED", "WATCH")

