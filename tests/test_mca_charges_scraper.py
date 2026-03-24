from ingestion.scrapers.mca_charges import MCAChargesScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_mca_charges_parse_row():
    scraper = MCAChargesScraper(make_db())
    row = scraper.parse_charge_row(
        ["CHG1", "01-01-2026", "", "SBI", "2 Cr", "Plant", "Open"],
        authorized_capital=10_000_000,
    )

    assert row["charge_id"] == "CHG1"
    assert row["charge_amount_inr"] == 20_000_000
    assert row["authorized_capital"] == 10_000_000


def test_mca_charges_created_alert():
    scraper = MCAChargesScraper(make_db())
    event = scraper._classify_charge(None, {"charge_amount_inr": 12_000_000, "status": "Open"})
    assert event == ("CHARGE_CREATED", "ALERT")


def test_mca_charges_satisfied():
    scraper = MCAChargesScraper(make_db())
    event = scraper._classify_charge({"status": "Open"}, {"status": "Satisfied", "charge_amount_inr": 100})
    assert event == ("CHARGE_SATISFIED", "INFO")

