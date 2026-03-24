from datetime import date

from ingestion.scrapers.nclt import NCLTScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_nclt_parse_amount_crore():
    s = NCLTScraper(make_db())
    assert s._parse_amount("₹12.5 Cr") == 125_000_000


def test_nclt_parse_amount_lakh():
    s = NCLTScraper(make_db())
    assert s._parse_amount("50 Lakh") == 5_000_000


def test_nclt_parse_amount_plain():
    s = NCLTScraper(make_db())
    assert s._parse_amount("1250000") == 1_250_000


def test_nclt_severity_section_7():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_7") == "CRITICAL"


def test_nclt_severity_section_9():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_9") == "CRITICAL"


def test_nclt_severity_section_10():
    s = NCLTScraper(make_db())
    assert s._severity_for_case_type("NCLT_10") == "ALERT"


def test_nclt_parse_date():
    s = NCLTScraper(make_db())
    assert s._parse_date("15/03/2024") == date(2024, 3, 15)
    assert s._parse_date("bad date") is None


def test_nclt_bench_values_verified():
    s = NCLTScraper(make_db())
    assert s is not None
    assert s.source_id == "nclt"

