from __future__ import annotations

from ingestion.scrapers.rbi_wilful_defaulter import RBIWilfulDefaulterScraper


class RBIWilfulDefaulterDirectorsScraper(RBIWilfulDefaulterScraper):
    source_id = "rbi_wilful_defaulter_directors"

    def director_rows(self, rows: list[dict]) -> list[dict]:
        return [row for row in rows if self._lookup_director_cin(row.get("name") or "")]

