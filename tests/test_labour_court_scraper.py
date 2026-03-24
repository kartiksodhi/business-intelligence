from ingestion.scrapers.labour_court import LabourCourtScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_labour_court_mass_retrenchment():
    scraper = LabourCourtScraper(make_db())
    assert scraper.classify_order("Retrenchment order", 60) == ("LABOUR_MASS_RETRENCHMENT", "ALERT")


def test_labour_court_back_wages():
    scraper = LabourCourtScraper(make_db())
    assert scraper.classify_order("Back wages order", 10) == ("LABOUR_BACK_WAGES", "WATCH")

