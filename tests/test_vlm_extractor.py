from unittest.mock import patch

import pytest

from ingestion import vlm_extractor


def test_route_document_uses_vlm_for_legal_sources():
    with patch("ingestion.vlm_extractor.vlm_extract", return_value={"ok": 1}) as mock_vlm:
        assert vlm_extractor.route_document("sarfaesi", "doc.png") == {"ok": 1}
        mock_vlm.assert_called_once_with("doc.png")


def test_route_document_uses_pytesseract_for_captcha():
    with patch("ingestion.vlm_extractor.pytesseract_solve", return_value="ABCD") as mock_ocr:
        assert vlm_extractor.route_document("captcha", "captcha.png") == "ABCD"
        mock_ocr.assert_called_once_with("captcha.png")


def test_route_document_uses_playwright_for_other_sources():
    with patch("ingestion.vlm_extractor.playwright_scrape", return_value={"html": "ok"}) as mock_pw:
        assert vlm_extractor.route_document("ecourts", "page.html") == {"html": "ok"}
        mock_pw.assert_called_once_with("page.html")


def test_vlm_validation_logs_and_returns_none(caplog):
    caplog.set_level("ERROR")
    with patch("ingestion.vlm_extractor._claude_api_call", side_effect=["not-json", "still-not-json"]):
        assert vlm_extractor.vlm_extract("doc.png") is None
    assert "VLM validation failed twice" in caplog.text


def test_vlm_currency_conversion():
    raw = (
        '{"demand_amount_inr": "1.5 Crore", "date_of_notice": "2026-03-18", '
        '"lender_name": "State Bank of India", "borrower_cin": null}'
    )
    with patch("ingestion.vlm_extractor._claude_api_call", return_value=raw):
        result = vlm_extractor.vlm_extract("doc.png")
    assert result["demand_amount_inr"] == 15_000_000
    assert result["date_of_notice"] == "2026-03-18"
