from datetime import date

from ingestion.scrapers.sarfaesi import SARFAESIScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_sarfaesi_severity_auction():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_AUCTION") == "CRITICAL"


def test_sarfaesi_severity_13_4():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_13_4") == "CRITICAL"


def test_sarfaesi_severity_13_2():
    s = SARFAESIScraper(make_db())
    assert s._severity_for_case_type("SARFAESI_13_2") == "ALERT"


def test_sarfaesi_parse_date_variants():
    s = SARFAESIScraper(make_db())
    assert s._parse_date("15-03-2024") == date(2024, 3, 15)
    assert s._parse_date("15 Mar 2024") == date(2024, 3, 15)
    assert s._parse_date("07.04.2026") == date(2026, 4, 7)
    assert s._parse_date("garbage") is None


def test_sarfaesi_parse_amount():
    s = SARFAESIScraper(make_db())
    assert s._parse_amount("2.5 Cr") == 25_000_000
    assert s._parse_amount(None) is None

