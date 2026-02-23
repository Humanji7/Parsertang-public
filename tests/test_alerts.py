import asyncio
import logging

from parsertang.alerts import AlertService


class DummyBot:
    async def send_message(self, chat_id: str, text: str, parse_mode: str):
        return {"ok": True}


class RecordingBot:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def send_message(self, chat_id: str, text: str, parse_mode: str):
        self.calls.append((chat_id, text, parse_mode))
        return {"ok": True}


def test_send_tech_does_not_fallback_to_main_chat():
    service = AlertService(token="x", chat_id="work", tech_chat_id=None)
    bot = RecordingBot()
    service.bot = bot

    asyncio.run(service.send_tech("hello"))

    assert bot.calls == []


def test_send_tech_uses_tech_chat_id_when_configured():
    service = AlertService(token="x", chat_id="work", tech_chat_id="tech")
    bot = RecordingBot()
    service.bot = bot

    asyncio.run(service.send_tech("hello"))

    assert bot.calls[0][0] == "tech"


def test_alert_send_logs_success(caplog):
    service = AlertService(token="x", chat_id="y")
    service.bot = DummyBot()

    with caplog.at_level(logging.INFO):
        asyncio.run(service._send_async("hello"))

    assert any("ALERT SENT" in rec.message for rec in caplog.records)
