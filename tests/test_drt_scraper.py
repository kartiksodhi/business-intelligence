from datetime import date

from ingestion.scrapers.drt import DRTScraper
from ingestion.scrapers import _run_counter


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_drt_severity():
    s = DRTScraper(make_db())
    assert s._severity_for_case_type("DRT") == "ALERT"


def test_drt_parse_amount_crore():
    s = DRTScraper(make_db())
    assert s._parse_amount("5 Cr") == 50_000_000


def test_drt_parse_amount_none():
    s = DRTScraper(make_db())
    assert s._parse_amount(None) is None


def test_drt_parse_date():
    s = DRTScraper(make_db())
    assert s._parse_date("01/01/2024") == date(2024, 1, 1)


def test_drt_bench_rotation():
    s = DRTScraper(make_db())
    _run_counter["drt"] = 0
    first = s._benches_for_this_run()
    second = s._benches_for_this_run()
    assert first != second

