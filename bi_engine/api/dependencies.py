"""
FastAPI dependencies.

Provides the asyncpg connection pool via get_db().
Pool is created once at startup via the lifespan context manager in main.py
and stored on app.state.pool.
"""

from __future__ import annotations

import logging
from typing import AsyncGenerator

import asyncpg
from fastapi import Request


logger = logging.getLogger(__name__)


async def get_db(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Yield a single asyncpg connection from the pool.
    Released automatically after the request completes.
    """
    async with request.app.state.pool.acquire() as conn:
        yield conn
