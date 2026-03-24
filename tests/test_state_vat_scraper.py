from unittest.mock import AsyncMock, MagicMock

import pytest

from ingestion.scrapers.state_vat import StateVATScraper


def make_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    return db


@pytest.mark.asyncio
async def test_state_vat_skips_when_backfill_complete():
    scraper = StateVATScraper(make_db())
    scraper._load_state = MagicMock(return_value={"backfill_complete": True})
    scraper._backfill_state = AsyncMock()

    result = await scraper.run()

    assert result == []
    scraper._backfill_state.assert_not_called()


@pytest.mark.asyncio
async def test_state_vat_runs_backfill_once():
    scraper = StateVATScraper(make_db())
    scraper._load_state = MagicMock(side_effect=[{}, {}])
    scraper._backfill_state = AsyncMock()
    scraper._store_state = MagicMock()

    await scraper.run()

    assert scraper._backfill_state.call_count == 2
    assert scraper._store_state.call_count == 2

