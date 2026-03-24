from ingestion.scrapers.supreme_court import SupremeCourtScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_supreme_court_extract_company_names():
    scraper = SupremeCourtScraper(make_db())
    names = scraper.extract_company_names("Acme Pvt Ltd vs State of X; Beta LLP versus Union of India")
    assert "Acme Pvt Ltd" in names
    assert "Beta LLP" in names


def test_supreme_court_classify_dismissed():
    scraper = SupremeCourtScraper(make_db())
    assert scraper._classify_matter("Appeal dismissed") == ("SC_APPEAL_DISMISSED", "ALERT")

