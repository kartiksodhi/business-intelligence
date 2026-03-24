from ingestion.scrapers.moef import MOEFScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_moef_extract_status():
    scraper = MOEFScraper(make_db())
    assert scraper._extract_status("Environmental clearance granted") == "Granted"


def test_moef_classify_refused():
    scraper = MOEFScraper(make_db())
    event = scraper.classify_change({"status": "Applied"}, {"status": "Refused", "project_cost_inr": 1})
    assert event == ("EC_REFUSED", "ALERT")

