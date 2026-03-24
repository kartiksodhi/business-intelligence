from __future__ import annotations

import os
from contextlib import asynccontextmanager

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from routing.batch_flusher import BatchFlusher
from routing.daily_digest import DailyDigestSender
from routing.migrations import ensure_alerts_retry_column
from routing.summarizer import AlertSummarizer
from routing.telegram_deliverer import TelegramDeliverer
from routing.watchlist_matcher import WatchlistMatcher


def create_scheduler(
    pool: asyncpg.Pool,
    batch_flusher: BatchFlusher,
    daily_digest: DailyDigestSender,
) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        batch_flusher.flush,
        trigger="interval",
        minutes=30,
        id="batch_flush",
        replace_existing=True,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        daily_digest.send_digest,
        trigger="cron",
        hour=7,
        minute=0,
        id="daily_digest",
        replace_existing=True,
        misfire_grace_time=300,
    )
    return scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    await ensure_alerts_retry_column(pool)

    summarizer = AlertSummarizer(pool)
    telegram = TelegramDeliverer(pool)
    flusher = BatchFlusher(pool, summarizer, telegram)
    digest = DailyDigestSender(pool)

    scheduler = create_scheduler(pool, flusher, digest)
    scheduler.start()

    app.state.pool = pool
    app.state.matcher = WatchlistMatcher(pool)
    app.state.flusher = flusher

    yield

    scheduler.shutdown(wait=False)
    await pool.close()
