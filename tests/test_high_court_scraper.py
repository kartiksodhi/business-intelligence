from ingestion.scrapers.high_court import HighCourtScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_high_court_parse_case():
    scraper = HighCourtScraper(make_db())
    row = scraper.parse_case(["CS/1", "01-03-2026", "Bank", "Acme Ltd", "Bombay HC", "2 Cr", "injunction"])

    assert row["case_number"] == "CS/1"
    assert row["claim_amount_inr"] == 20_000_000


def test_high_court_attachment_classification():
    scraper = HighCourtScraper(make_db())
    event = scraper._classify_case({"order_type": "attachment", "court_name": "Bombay HC"})
    assert event == ("HIGH_COURT_ATTACHMENT", "CRITICAL")

