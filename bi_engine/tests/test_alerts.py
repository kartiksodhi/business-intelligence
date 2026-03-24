from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from routing.alert_delivery import (
    AlertDeliveryService,
    AlertSynthesizer,
    BrevoEmailChannel,
    SubscriberRecord,
)
from routing.batch_flusher import AlertDigest, BatchFlusher, EventSummary
from routing.daily_digest import DailyDigestSender
from routing.summarizer import AlertSummarizer
from routing.telegram_deliverer import TelegramDeliverer
from routing.watchlist_matcher import EventRow, WatchlistMatcher


def _make_digest(severity: str = "ALERT") -> AlertDigest:
    return AlertDigest(
        watchlist_id=1,
        cin="U12345MH2001PTC123456",
        company_name="Acme Pvt Ltd",
        company_state="MH",
        company_sector="6201",
        events=[
            EventSummary(
                event_id=1,
                event_type="NCLT_FILING",
                source="nclt",
                detected_at="2026-03-16T10:00:00",
                severity=severity,
                data_json={"case_number": "IB/123/2026"},
                health_score_before=65,
                health_score_after=30,
            )
        ],
        health_score_current=30,
        health_score_previous=65,
        health_band="RED",
        severity=severity,
        contagion_result=None,
        alert_ids=[1],
    )


def _make_alert_row(
    *,
    alert_id: int = 1,
    watchlist_id: int = 1,
    cin: str = "U12345MH2001PTC123456",
    severity: str = "WATCH",
    created_at: datetime | None = None,
    delivery_status: str = "PENDING",
    retry_count: int = 0,
    event_id: int = 1,
    event_type: str = "NCLT_FILING",
) -> dict:
    if created_at is None:
        created_at = datetime.now(timezone.utc) - timedelta(hours=25)
    payload = {
        "event_id": event_id,
        "cin": cin,
        "event_type": event_type,
        "severity": severity,
        "source": "nclt",
        "detected_at": "2026-03-16T10:00:00",
        "data_json": {"case_number": f"IB/{event_id}/2026"},
        "health_score_before": 65,
        "health_score_after": 30,
        "contagion_chain": None,
    }
    return {
        "alert_id": alert_id,
        "event_id": event_id,
        "watchlist_id": watchlist_id,
        "cin": cin,
        "severity": severity,
        "alert_payload": payload,
        "created_at": created_at,
        "delivery_status": delivery_status,
        "retry_count": retry_count,
        "company_name": "Acme Pvt Ltd",
        "registered_state": "MH",
        "industrial_class": "6201",
        "health_score_current": 30,
        "health_band": "RED",
    }


class _AcquireContext:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AcquireContext(self.conn)


class FakeResponse:
    def raise_for_status(self):
        return None


class FakeAsyncClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, headers=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return FakeResponse()


@pytest.mark.asyncio
async def test_watchlist_cin_filter_matches():
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[{"id": 7}])
    pool.fetchrow = AsyncMock(return_value={"id": 101})

    matcher = WatchlistMatcher(pool)
    event = EventRow(
        id=1,
        cin="U12345MH2001PTC123456",
        event_type="NCLT_FILING",
        severity="ALERT",
        detected_at="2026-03-16T10:00:00",
        data_json={},
        health_score_before=65,
        health_score_after=30,
        contagion_chain=None,
        source="nclt",
    )

    result = await matcher.match_event(event)

    assert result == [7]


@pytest.mark.asyncio
async def test_watchlist_state_filter_excludes():
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock()

    matcher = WatchlistMatcher(pool)
    event = EventRow(
        id=1,
        cin="U12345DL2001PTC123456",
        event_type="NCLT_FILING",
        severity="ALERT",
        detected_at="2026-03-16T10:00:00",
        data_json={},
        health_score_before=65,
        health_score_after=30,
        contagion_chain=None,
        source="nclt",
    )

    result = await matcher.match_event(event)

    assert result == []


@pytest.mark.asyncio
async def test_watchlist_severity_min_excludes_info():
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock()

    matcher = WatchlistMatcher(pool)
    event = EventRow(
        id=1,
        cin="U12345MH2001PTC123456",
        event_type="NEW_COMPANY",
        severity="INFO",
        detected_at="2026-03-16T10:00:00",
        data_json={},
        health_score_before=None,
        health_score_after=None,
        contagion_chain=None,
        source="mca_ogd",
    )

    result = await matcher.match_event(event)

    assert result == []


@pytest.mark.asyncio
async def test_alert_batch_window_not_expired():
    pool = MagicMock()
    row = _make_alert_row(severity="ALERT", created_at=datetime.now(timezone.utc) - timedelta(hours=2))
    pool.fetch = AsyncMock(return_value=[row])
    pool.execute = AsyncMock()

    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value="summary")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=True)

    flusher = BatchFlusher(pool, summarizer, telegram)
    await flusher.flush()

    summarizer.summarize.assert_not_called()
    pool.execute.assert_not_called()


@pytest.mark.asyncio
async def test_critical_flushed_immediately():
    pool = MagicMock()
    pool.fetchrow = AsyncMock(return_value=_make_alert_row(severity="CRITICAL", created_at=datetime.now(timezone.utc)))
    pool.execute = AsyncMock()

    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value="summary")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=True)

    flusher = BatchFlusher(pool, summarizer, telegram)
    await flusher.flush_critical(alert_id=1)

    telegram.send.assert_called_once()
    calls = [str(call) for call in pool.execute.call_args_list]
    assert any("delivery_status = 'DELIVERED'" in call for call in calls)


@pytest.mark.asyncio
async def test_same_cin_grouped_into_one_digest():
    pool = MagicMock()
    row1 = _make_alert_row(alert_id=1, event_id=1)
    row2 = _make_alert_row(alert_id=2, event_id=2)
    pool.fetch = AsyncMock(return_value=[row1, row2])
    pool.execute = AsyncMock()

    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value="summary")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=True)

    flusher = BatchFlusher(pool, summarizer, telegram)
    await flusher.flush()

    summarizer.summarize.assert_called_once()
    digest = summarizer.summarize.call_args.args[0]
    assert len(digest.events) == 2


@pytest.mark.asyncio
async def test_summarizer_calls_api_once():
    with patch("anthropic.AsyncAnthropic") as mock_anthropic:
        mock_client = AsyncMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create = AsyncMock(
            return_value=MagicMock(
                content=[MagicMock(text="Summary sentence one. Sentence two. Sentence three.")]
            )
        )

        pool = MagicMock()
        pool.execute = AsyncMock()
        pool.fetchval = AsyncMock(return_value=10.0)

        summarizer = AlertSummarizer(pool)
        digest = _make_digest()

        result = await summarizer.summarize(digest)

        mock_client.messages.create.assert_called_once()
        assert len(result) > 0


@pytest.mark.asyncio
async def test_summarizer_fallback_on_api_failure():
    with patch("anthropic.AsyncAnthropic") as mock_anthropic:
        mock_client = AsyncMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create = AsyncMock(side_effect=Exception("API timeout"))

        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=0.0)

        summarizer = AlertSummarizer(pool)
        digest = _make_digest()

        result = await summarizer.summarize(digest)

        assert isinstance(result, str)
        assert len(result) > 0
        assert digest.events[0].event_type in result
        assert digest.company_name in result


@pytest.mark.asyncio
async def test_telegram_message_format():
    with patch("telegram.Bot") as mock_bot_class:
        mock_bot = AsyncMock()
        mock_bot_class.return_value = mock_bot
        mock_bot.send_message = AsyncMock()

        with patch.dict(
            "os.environ",
            {
                "TELEGRAM_BOT_TOKEN": "fake-token",
                "TELEGRAM_OPERATOR_CHAT_ID": "12345",
            },
        ):
            pool = MagicMock()
            deliverer = TelegramDeliverer(pool)
            digest = _make_digest(severity="CRITICAL")
            summary = "Fact one. Fact two. Action required."

            result = await deliverer.send(digest, summary)

        assert result is True
        call_args = mock_bot.send_message.call_args
        text = call_args.kwargs["text"] if "text" in call_args.kwargs else call_args.args[1]
        assert "🚨 CRITICAL ALERT" in text
        assert digest.company_name in text
        assert digest.cin in text
        assert summary in text
        assert f"{digest.health_score_previous}→{digest.health_score_current}" in text


@pytest.mark.asyncio
async def test_telegram_permanent_failure_after_3_retries():
    pool = MagicMock()
    row = _make_alert_row(
        severity="CRITICAL",
        delivery_status="FAILED",
        retry_count=2,
        created_at=datetime.now(timezone.utc),
    )
    pool.fetch = AsyncMock(return_value=[row])
    pool.fetchval = AsyncMock(return_value=2)
    pool.execute = AsyncMock()

    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value="summary")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=False)

    flusher = BatchFlusher(pool, summarizer, telegram)
    await flusher.flush()

    calls = [str(call) for call in pool.execute.call_args_list]
    assert any("PERMANENTLY_FAILED" in call for call in calls)
    assert any("TELEGRAM_DELIVERY_FAILED" in call for call in calls)


@pytest.mark.asyncio
async def test_alert_delivery_synthesizer_falls_back_on_invalid_bullets():
    client = MagicMock()
    client.messages.create = AsyncMock(
        return_value=MagicMock(content=[MagicMock(text="Not bullets\nStill not bullets")])
    )
    synthesizer = AlertSynthesizer(client=client)

    summary = await synthesizer.summarize_events(
        company_name="Acme Pvt Ltd",
        cin="U12345MH2001PTC123456",
        severity="CRITICAL",
        event_type="NCLT_FILING",
        events=[{"source": "nclt"}],
    )

    assert summary.count("\n") == 2
    assert summary.startswith("- ")


@pytest.mark.asyncio
async def test_alert_delivery_skips_below_threshold():
    conn = MagicMock()
    conn.execute = AsyncMock()
    pool = FakePool(conn)

    synthesizer = MagicMock()
    synthesizer.summarize_events = AsyncMock(return_value="- a\n- b\n- c")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=True)
    email = MagicMock()
    email.send = AsyncMock(return_value=True)

    service = AlertDeliveryService(pool, synthesizer=synthesizer, telegram=telegram, email=email)
    delivered = await service.deliver_to_subscriber(
        subscriber=SubscriberRecord(
            id=1,
            name="Asha",
            email="asha@example.com",
            severity_threshold="CRITICAL",
        ),
        cin="U12345MH2001PTC123456",
        company_name="Acme Pvt Ltd",
        severity="WATCH",
        event_type="NCLT_FILING",
        events=[{"source": "nclt"}],
    )

    assert delivered is False
    synthesizer.summarize_events.assert_not_called()
    telegram.send.assert_not_called()
    email.send.assert_not_called()


@pytest.mark.asyncio
async def test_alert_delivery_deduplicates_within_24_hours():
    conn = MagicMock()

    async def mock_fetchrow(query, *args):
        if "FROM delivered_alerts" in query:
            return {"id": 99}
        return None

    conn.fetchrow = AsyncMock(side_effect=mock_fetchrow)
    conn.execute = AsyncMock()
    pool = FakePool(conn)

    synthesizer = MagicMock()
    synthesizer.summarize_events = AsyncMock(return_value="- a\n- b\n- c")
    telegram = MagicMock()
    telegram.send = AsyncMock(return_value=True)
    email = MagicMock()
    email.send = AsyncMock(return_value=True)

    service = AlertDeliveryService(pool, synthesizer=synthesizer, telegram=telegram, email=email)
    delivered = await service.deliver_to_subscriber(
        subscriber=SubscriberRecord(
            id=1,
            name="Asha",
            email="asha@example.com",
            severity_threshold="WATCH",
        ),
        cin="U12345MH2001PTC123456",
        company_name="Acme Pvt Ltd",
        severity="CRITICAL",
        event_type="NCLT_FILING",
        events=[{"source": "nclt"}],
    )

    assert delivered is False
    telegram.send.assert_not_called()
    email.send.assert_not_called()


@pytest.mark.asyncio
async def test_brevo_email_channel_uses_brevo_api_contract():
    created_clients: list[FakeAsyncClient] = []

    def client_factory(**kwargs):
        client = FakeAsyncClient(**kwargs)
        created_clients.append(client)
        return client

    channel = BrevoEmailChannel(client_factory=client_factory)
    with patch.dict(
        "os.environ",
        {"BREVO_API_KEY": "bk", "OPERATOR_EMAIL": "ops@example.com"},
    ):
        result = await channel.send(
            to_email="subscriber@example.com",
            company_name="Acme Pvt Ltd",
            cin="U12345MH2001PTC123456",
            severity="CRITICAL",
            summary="- one\n- two\n- three",
        )

    assert result is True
    call = created_clients[0].calls[0]
    assert call["url"] == "https://api.brevo.com/v3/smtp/email"
    assert call["headers"]["api-key"] == "bk"
    assert call["json"]["sender"] == {"email": "ops@example.com"}
    assert call["json"]["to"] == [{"email": "ops@example.com"}]
    assert call["json"]["htmlContent"] == "- one<br>- two<br>- three"


@pytest.mark.asyncio
async def test_daily_digest_24h_window():
    with patch("resend.Emails.send") as mock_send:
        with patch.dict(
            "os.environ",
            {"RESEND_API_KEY": "rk", "OPERATOR_EMAIL": "ops@example.com"},
        ):
            pool = MagicMock()
            pool.fetch = AsyncMock(side_effect=[[], [], []])
            pool.fetchrow = AsyncMock(return_value={"total_red": 0, "confirmed_count": 0, "top_false_positive_reason": None})
            pool.fetchval = AsyncMock(return_value=0)

            sender = DailyDigestSender(pool)
            await sender.send_digest()

            mock_send.assert_called_once()
            call_args = mock_send.call_args[0][0]
            assert "Daily Digest" in call_args["subject"]


@pytest.mark.asyncio
async def test_daily_digest_failure_logged_to_events():
    with patch("resend.Emails.send", side_effect=Exception("Resend timeout")):
        with patch.dict(
            "os.environ",
            {"RESEND_API_KEY": "rk", "OPERATOR_EMAIL": "ops@example.com"},
        ):
            pool = MagicMock()
            pool.fetch = AsyncMock(side_effect=[[], [], []])
            pool.fetchrow = AsyncMock(return_value={"total_red": 0, "confirmed_count": 0, "top_false_positive_reason": None})
            pool.fetchval = AsyncMock(return_value=0)
            pool.execute = AsyncMock()

            sender = DailyDigestSender(pool)
            await sender.send_digest()

            calls = [str(call) for call in pool.execute.call_args_list]
            assert any("DIGEST_FAILED" in call for call in calls)
