from ingestion.scrapers.sebi_enforcement import SEBIEnforcementScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_sebi_extract_order_type_show_cause():
    scraper = SEBIEnforcementScraper(make_db())
    assert scraper._extract_order_type("Show cause notice issued to promoter") == "show_cause"


def test_sebi_classify_interim():
    scraper = SEBIEnforcementScraper(make_db())
    assert scraper._classify_order("Interim order in the matter of Acme Ltd", "interim") == (
        "SEBI_INTERIM_ORDER",
        "CRITICAL",
    )


def test_sebi_extract_entity_name():
    scraper = SEBIEnforcementScraper(make_db())
    assert scraper._extract_entity_name("01-02-2026 In the matter of Acme Limited") == "Acme Limited"
