from ingestion.scrapers.cci import CCIScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_cci_extract_party_names():
    scraper = CCIScraper(make_db())
    names = scraper.extract_party_names("Acme Limited and Beta Pvt Ltd approval order")
    assert "Acme Limited" in names
    assert "Beta Pvt Ltd approval order" in names


def test_cci_classify_penalty():
    scraper = CCIScraper(make_db())
    assert scraper.classify_order("Penalty") == ("CCI_PENALTY", "ALERT")

