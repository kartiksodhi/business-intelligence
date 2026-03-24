from ingestion.scrapers.gst import GSTScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_gst_classify_cancelled():
    scraper = GSTScraper(make_db())
    assert scraper._classify_transition("Active", "Cancelled") == ("GST_CANCELLED", "CRITICAL")


def test_gst_classify_restored():
    scraper = GSTScraper(make_db())
    assert scraper._classify_transition("Suspended", "Active") == ("GST_RESTORED", "INFO")


def test_gst_normalises_nested_payload():
    scraper = GSTScraper(make_db())
    payload = {
        "tradeName": "Acme Traders",
        "registrationDate": "10-01-2024",
        "taxpayerStatus": "Suspended",
        "cancellationReason": "Non filing",
    }
    normalised = scraper._normalise_taxpayer_payload("07AABCU9603R1ZP", payload)

    assert normalised["gstin"] == "07AABCU9603R1ZP"
    assert normalised["trade_name"] == "Acme Traders"
    assert normalised["gstin_status"] == "Suspended"
    assert normalised["cancellation_reason"] == "Non filing"

