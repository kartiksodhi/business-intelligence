from ingestion.scrapers.rbi_nbfc import RBINBFCScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_rbi_nbfc_relevance_filter():
    scraper = RBINBFCScraper(make_db())
    assert scraper._looks_relevant("Enforcement action against NBFC") is True


def test_rbi_nbfc_extract_action_type():
    scraper = RBINBFCScraper(make_db())
    assert scraper._extract_action_type("Cancellation of Certificate of Registration") == "cancellation"


def test_rbi_nbfc_classify_warning():
    scraper = RBINBFCScraper(make_db())
    assert scraper._classify_action("warning") == ("RBI_WARNING", "WATCH")

