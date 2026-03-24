from unittest.mock import MagicMock

from ingestion.scrapers.rbi_wilful_defaulter_directors import RBIWilfulDefaulterDirectorsScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_rbi_wilful_defaulter_director_rows_filters():
    scraper = RBIWilfulDefaulterDirectorsScraper(make_db())
    scraper._lookup_director_cin = MagicMock(side_effect=["CIN1", None])
    rows = scraper.director_rows([{"name": "Director One"}, {"name": "Unknown"}])
    assert rows == [{"name": "Director One"}]

