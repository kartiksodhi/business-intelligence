from ingestion.scrapers.rbi_wilful_defaulter import RBIWilfulDefaulterScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_rbi_wilful_row_key_stable():
    scraper = RBIWilfulDefaulterScraper(make_db())
    row = {
        "name": "Acme Private Limited",
        "identifier": "U1234567890123456789",
        "lender": "SBI",
        "amount": 1000000,
    }
    assert scraper._row_key(row) == "Acme Private Limited|U1234567890123456789|SBI|1000000"


def test_rbi_wilful_identifier_cin_shortcut():
    scraper = RBIWilfulDefaulterScraper(make_db())
    row = {"name": "Acme Private Limited", "identifier": "U12345MH2020PTC123456"}
    assert scraper._resolve_cin_for_row(row) == "U12345MH2020PTC123456"

