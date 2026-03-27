from bi_engine.scripts.setup_db import extract_source_ids


def test_extract_source_ids_matches_runtime_scraper_ids():
    source_ids = set(extract_source_ids())

    expected_runtime_ids = {
        "cci",
        "cpcb",
        "gst",
        "high_court",
        "labour_court",
        "mca_charges",
        "moef",
        "rbi_nbfc",
        "rbi_wilful_defaulter",
        "sebi_bulk_deals",
        "sebi_enforcement",
        "state_vat",
        "supreme_court",
        "udyam",
    }

    assert expected_runtime_ids.issubset(source_ids)
