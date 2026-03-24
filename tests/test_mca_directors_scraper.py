from ingestion.scrapers.mca_directors import MCADirectorsScraper


def make_db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


def test_mca_directors_cfo_resigned():
    scraper = MCADirectorsScraper(make_db())
    events = scraper.classify_change(
        {"designation": "CFO", "cessation_date": None},
        {"designation": "CFO", "cessation_date": "2026-03-01"},
    )
    assert ("CFO_RESIGNED", "ALERT") in events


def test_mca_directors_auditor_changed():
    scraper = MCADirectorsScraper(make_db())
    events = scraper.classify_change(
        {"designation": "Auditor", "director_name": "Old Auditor"},
        {"designation": "Auditor", "director_name": "New Auditor"},
    )
    assert ("AUDITOR_CHANGED", "ALERT") in events


def test_mca_directors_overloaded():
    scraper = MCADirectorsScraper(make_db())
    events = scraper.classify_change(None, {"designation": "Director"}, board_count=11)
    assert ("DIRECTOR_OVERLOADED", "WATCH") in events

