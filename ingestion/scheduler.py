import asyncio
import logging
import os
import sys
from pathlib import Path

import asyncpg
import psycopg2

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:  # pragma: no cover
    class IntervalTrigger:
        def __init__(self, hours: int):
            self.hours = hours

    class CronTrigger:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class AsyncIOScheduler:
        def __init__(self):
            self.jobs = []

        def add_job(self, func, trigger=None, id=None, name=None):
            self.jobs.append({"func": func, "trigger": trigger, "id": id, "name": name})

        def start(self):
            return None

        def shutdown(self):
            return None

from ingestion.scrapers.drt import DRTScraper
from ingestion.scrapers.ecourts import ECourtsScraper
from ingestion.scrapers.esic import ESICScraper
from ingestion.scrapers.dgft import DGFTScraper
from ingestion.scrapers.epfo import EPFOScraper
from ingestion.scrapers.gem import GeMScraper
from ingestion.scrapers.gst import GSTScraper
from ingestion.scrapers.high_court import HighCourtScraper
from ingestion.scrapers.ibbi import IBBIScraper
from ingestion.scrapers.labour_court import LabourCourtScraper
from ingestion.scrapers.mca_directors import MCADirectorsScraper
from ingestion.scrapers.moef import MOEFScraper
from ingestion.scrapers.nclt import NCLTScraper
from ingestion.scrapers.rera import RERAScraper
from ingestion.scrapers.rbi_nbfc import RBINBFCScraper
from ingestion.scrapers.rbi_wilful_defaulter import RBIWilfulDefaulterScraper
from ingestion.scrapers.sarfaesi import SARFAESIScraper
from ingestion.scrapers.cersai import CERSAIScraper
from ingestion.scrapers.cci import CCIScraper
from ingestion.scrapers.cpcb import CPCBScraper
from ingestion.scrapers.sebi_enforcement import SEBIEnforcementScraper
from ingestion.scrapers.sebi_bulk_deals import SEBIBulkDealsScraper
from ingestion.scrapers.state_vat import StateVATScraper
from ingestion.scrapers.supreme_court import SupremeCourtScraper
from ingestion.scrapers.udyam import UdyamScraper

try:
    from detection.shell_detector import ShellDetector
except ImportError:  # pragma: no cover
    from bi_engine.detection.shell_detector import ShellDetector

try:
    from detection.sector_cluster import SectorClusterDetector
except ImportError:  # pragma: no cover
    from bi_engine.detection.sector_cluster import SectorClusterDetector

logger = logging.getLogger(__name__)


class SyncDB:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def get_db():
    return SyncDB(psycopg2.connect(os.environ["DATABASE_URL"]))


async def run_post_scrape_intelligence():
    """Run health scoring + signal combining for any unscored events."""
    pool = None
    try:
        pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])

        try:
            from detection.health_scorer import HealthScorer
        except ImportError:
            from bi_engine.detection.health_scorer import HealthScorer

        try:
            from detection.signal_combiner import check_combinations
        except ImportError:
            from bi_engine.detection.signal_combiner import check_combinations

        scorer = HealthScorer(pool)
        unscored = await pool.fetch(
            """
            SELECT id, cin FROM events
            WHERE cin IS NOT NULL
              AND health_score_after IS NULL
            ORDER BY detected_at DESC
            LIMIT 50
            """
        )
        scored_count = 0
        for row in unscored:
            try:
                result = await scorer.recompute(row["cin"], row["id"])
                await pool.execute(
                    """
                    UPDATE events
                    SET health_score_before = $1, health_score_after = $2
                    WHERE id = $3
                    """,
                    result.previous_score, result.score, row["id"],
                )
                scored_count += 1
            except Exception as exc:
                logger.warning("Health scoring failed for cin=%s event=%s: %s", row["cin"], row["id"], exc)

        if scored_count:
            logger.info("Post-scrape intelligence: scored %d events", scored_count)

        # Check signal combinations for recently scored CINs
        combo_count = 0
        async with pool.acquire() as conn:
            recent = await conn.fetch(
                """
                SELECT DISTINCT cin, event_type FROM events
                WHERE cin IS NOT NULL AND detected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY cin
                """
            )
            for row in recent:
                try:
                    combos = await check_combinations(row["cin"], row["event_type"], conn)
                    combo_count += len(combos)
                except Exception as exc:
                    logger.warning("Signal combiner failed for cin=%s: %s", row["cin"], exc)
        if combo_count:
            logger.info("Signal combiner fired %d composite events", combo_count)

    except Exception as exc:
        logger.error("Post-scrape intelligence pipeline error: %s", exc)
    finally:
        if pool:
            await pool.close()


async def run_scraper(scraper_class):
    db = get_db()
    scraper = scraper_class(db)
    try:
        await scraper.run()
    except Exception as e:
        logger.error(f"{scraper_class.__name__} failed: {e}")
    finally:
        db.close()
    await run_post_scrape_intelligence()
    await deliver_critical_events()


async def run_shell_detector():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    try:
        detector = ShellDetector(pool)
        await detector.run()
    except Exception as e:
        logger.error("ShellDetector failed: %s", e)
    finally:
        await pool.close()


async def run_sector_cluster():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    try:
        detector = SectorClusterDetector(pool)
        await detector.run()
    except Exception as e:
        logger.error("SectorClusterDetector failed: %s", e)
    finally:
        await pool.close()


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(ECourtsScraper)),
        trigger=IntervalTrigger(hours=168),
        id="ecourts",
        name="e-Courts weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(NCLTScraper)),
        trigger=IntervalTrigger(hours=24),
        id="nclt",
        name="NCLT daily",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(DRTScraper)),
        trigger=IntervalTrigger(hours=24),
        id="drt",
        name="DRT daily",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(SARFAESIScraper)),
        trigger=IntervalTrigger(hours=24),
        id="sarfaesi",
        name="SARFAESI daily",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(IBBIScraper)),
        trigger=IntervalTrigger(hours=168),
        id="ibbi",
        name="IBBI weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(GSTScraper)),
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0),
        id="gst",
        name="GST weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(EPFOScraper)),
        trigger=CronTrigger(day=1, hour=7, minute=0),
        id="epfo",
        name="EPFO monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(RBIWilfulDefaulterScraper)),
        trigger=CronTrigger(month="1,4,7,10", day=1, hour=8, minute=0),
        id="rbi_wilful_defaulter",
        name="RBI wilful defaulter quarterly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(SEBIEnforcementScraper)),
        trigger=CronTrigger(day_of_week="tue", hour=6, minute=0),
        id="sebi_enforcement",
        name="SEBI enforcement weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(GeMScraper)),
        trigger=CronTrigger(day_of_week="wed", hour=6, minute=0),
        id="gem",
        name="GeM weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(DGFTScraper)),
        trigger=CronTrigger(day=5, hour=7, minute=0),
        id="dgft",
        name="DGFT monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(UdyamScraper)),
        trigger=CronTrigger(month="1,4,7,10", day=1, hour=8, minute=0),
        id="udyam",
        name="Udyam quarterly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(RBINBFCScraper)),
        trigger=CronTrigger(day_of_week="thu", hour=6, minute=0),
        id="rbi_nbfc",
        name="RBI NBFC weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_shell_detector()),
        trigger=CronTrigger(day="last", hour=23, minute=0),
        id="shell_detector",
        name="Shell detector monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(MCADirectorsScraper)),
        trigger=CronTrigger(day=2, hour=6, minute=0),
        id="mca_directors",
        name="MCA directors monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(HighCourtScraper)),
        trigger=CronTrigger(day_of_week="fri", hour=6, minute=0),
        id="high_court",
        name="High Court weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(SupremeCourtScraper)),
        trigger=CronTrigger(day_of_week="fri", hour=7, minute=0),
        id="supreme_court",
        name="Supreme Court weekly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(LabourCourtScraper)),
        trigger=CronTrigger(day=10, hour=8, minute=0),
        id="labour_court",
        name="Labour Court monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(SEBIBulkDealsScraper)),
        trigger=CronTrigger(hour=17, minute=0),
        id="sebi_bulk_deals",
        name="SEBI bulk deals daily",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(CERSAIScraper)),
        trigger=CronTrigger(day=15, hour=9, minute=0),
        id="cersai",
        name="CERSAI monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(CCIScraper)),
        trigger=CronTrigger(day=20, hour=8, minute=0),
        id="cci",
        name="CCI monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(ESICScraper)),
        trigger=CronTrigger(day=3, hour=7, minute=0),
        id="esic",
        name="ESIC monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(RERAScraper)),
        trigger=CronTrigger(day=12, hour=8, minute=0),
        id="rera",
        name="RERA monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(MOEFScraper)),
        trigger=CronTrigger(day=8, hour=8, minute=0),
        id="moef",
        name="MOEF monthly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(CPCBScraper)),
        trigger=CronTrigger(month="1,4,7,10", day=15, hour=9, minute=0),
        id="cpcb",
        name="CPCB quarterly",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_scraper(StateVATScraper)),
        trigger=CronTrigger(day=1, hour=5, minute=0),
        id="state_vat",
        name="State VAT backfill",
    )
    scheduler.add_job(
        lambda: asyncio.create_task(run_sector_cluster()),
        trigger=CronTrigger(day="last", hour=23, minute=30),
        id="sector_cluster",
        name="Sector cluster monthly",
    )

    return scheduler


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("Scheduler started - 27 jobs active")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        scheduler.shutdown()
