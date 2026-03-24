from datetime import date
from unittest.mock import MagicMock

from ingestion.scrapers.base_scraper import RawCase
from ingestion.scrapers.ibbi import IBBIScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_ibbi_severity_for_liquidation():
    s = IBBIScraper(make_db())
    assert s._severity_for_case_type("NCLT_7") == "CRITICAL"


def test_ibbi_parse_date():
    s = IBBIScraper(make_db())
    assert s._parse_date("10 Jan 2024") == date(2024, 1, 10)
    assert s._parse_date("10-01-2024") == date(2024, 1, 10)
    assert s._parse_date("rubbish") is None


def test_ibbi_extractors():
    s = IBBIScraper(make_db())
    title = "Approval of Resolution Plan - GF Toll Road Private Limited [A(IBC)(Plan)/1/MB/2026]"
    assert s._extract_company_name(title) == "GF Toll Road Private Limited"
    assert s._extract_case_ref(title) == "A(IBC)(Plan)/1/MB/2026"


def test_ibbi_existing_case_skipped_smoke():
    db = make_db()
    db.execute.return_value.fetchone.return_value = (1,)
    s = IBBIScraper(db)
    case = RawCase(
        "ibbi",
        "IBBI/2024/001",
        "NCLT_7",
        "IBBI",
        date(2024, 1, 10),
        "Test Co",
        None,
        "CIRP Commenced",
        None,
        {},
    )
    s._process_case = MagicMock()
    s._process_case(case)
    s._process_case.assert_called_once()


def test_ibbi_hash_stable():
    s = IBBIScraper(make_db())
    c1 = RawCase("ibbi", "R001", "NCLT_7", "IBBI", date(2024, 1, 1), "A", None, "X", None, {})
    c2 = RawCase("ibbi", "R002", "NCLT_7", "IBBI", date(2024, 1, 2), "B", None, "X", None, {})
    assert s.compute_hash([c1, c2]) == s.compute_hash([c2, c1])
