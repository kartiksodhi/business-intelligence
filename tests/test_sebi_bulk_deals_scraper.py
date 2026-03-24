from ingestion.scrapers.sebi_bulk_deals import SEBIBulkDealsScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_sebi_bulk_deals_normalise_row():
    scraper = SEBIBulkDealsScraper(make_db())
    row = scraper.normalise_row(
        {
            "Date": "18-03-2026",
            "Scrip Code": "500001",
            "Security Name": "Acme Ltd",
            "Client Name": "XYZ Capital",
            "Buy/Sell": "Sell",
            "Quantity": "100000",
            "Price": "200",
            "Value": "2 Cr",
        }
    )
    assert row["scrip_code"] == "500001"
    assert row["deal_value_inr"] == 20_000_000


def test_sebi_bulk_deals_institutional_exit():
    scraper = SEBIBulkDealsScraper(make_db())
    event = scraper.classify_deal({"client_name": "ABC Capital", "deal_type": "Sell"}, promoter_match=False)
    assert event == ("SEBI_BULK_DEAL_INSTITUTIONAL_EXIT", "WATCH")

