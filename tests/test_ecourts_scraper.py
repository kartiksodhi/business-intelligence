from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from ingestion.entity_resolver import ResolutionResult
from ingestion.scrapers.base_scraper import RawCase
from ingestion.scrapers.ecourts import ECourtsScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_ecourts_parse_date_formats():
    scraper = ECourtsScraper(make_db())
    assert scraper._parse_date("15-01-2024") == date(2024, 1, 15)
    assert scraper._parse_date("15/01/2024") == date(2024, 1, 15)
    assert scraper._parse_date("2024-01-15") == date(2024, 1, 15)
    assert scraper._parse_date("invalid") is None


def test_ecourts_severity():
    scraper = ECourtsScraper(make_db())
    assert scraper._severity_for_case_type("SEC_138") == "ALERT"


def test_ecourts_hash_deterministic():
    scraper = ECourtsScraper(make_db())
    c1 = RawCase("ecourts", "C001", "SEC_138", "MH", date(2024, 1, 1), "X", None, "P", None, {})
    c2 = RawCase("ecourts", "C002", "SEC_138", "MH", date(2024, 1, 2), "Y", None, "P", None, {})
    assert scraper.compute_hash([c1, c2]) == scraper.compute_hash([c2, c1])


def test_ecourts_unmapped_stored_on_no_cin():
    db = make_db()
    scraper = ECourtsScraper(db)
    mock_result = ResolutionResult(cin=None, confidence=0.0, method="none")
    with patch("ingestion.scrapers.base_scraper.EntityResolver") as mock_resolver:
        mock_resolver.return_value.resolve.return_value = mock_result
        case = RawCase(
            "ecourts",
            "138/2024",
            "SEC_138",
            "MH",
            date(2024, 1, 1),
            "Unknown Co",
            None,
            "Pending",
            None,
            {},
        )
        scraper._process_case(case)
    insert_sql = " ".join(str(db.execute.call_args.args[0]).split())
    assert "INSERT INTO unmapped_signals" in insert_sql


@pytest.mark.asyncio
async def test_ecourts_zero_cases_updates_source_state():
    db = make_db()
    db.execute.return_value.fetchone.return_value = (datetime(2024, 1, 10), None)
    scraper = ECourtsScraper(db)
    scraper.fetch_new_cases = MagicMock(return_value=[])
    await scraper.run()
    db.commit.assert_called()

