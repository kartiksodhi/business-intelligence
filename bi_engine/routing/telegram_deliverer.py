from __future__ import annotations

import logging
import os
import sys
import types

import asyncpg

try:
    import telegram
except ImportError:  # pragma: no cover
    telegram = types.ModuleType("telegram")

    class _FallbackBot:
        def __init__(self, token: str):
            self.token = token

        async def send_message(self, chat_id: int, text: str) -> None:
            raise RuntimeError("telegram package is not installed")

    telegram.Bot = _FallbackBot  # type: ignore[attr-defined]
    sys.modules["telegram"] = telegram


logger = logging.getLogger(__name__)


class TelegramDeliverer:
    def __init__(self, pool: asyncpg.Pool):
        self.bot = telegram.Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])
        self.chat_id = int(os.environ["TELEGRAM_OPERATOR_CHAT_ID"])
        self.pool = pool

    async def send(self, digest: "AlertDigest", summary: str) -> bool:
        primary_event = digest.events[0]
        text = (
            f"🚨 CRITICAL ALERT\n"
            f"{digest.company_name}\n"
            f"CIN: {digest.cin}\n\n"
            f"{summary}\n\n"
            f"Score: {digest.health_score_previous}→{digest.health_score_current} ({digest.health_band})\n"
            f"Source: {primary_event.source}\n"
            f"Detected: {primary_event.detected_at}"
        )

        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
            return True
        except Exception as exc:
            logger.error("Telegram send failed for %s: %s", digest.cin, exc)
            return False
