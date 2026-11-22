"""Async Telegram alert notifier with per-key throttling and silent fallback.

Credentials (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) are injected from .env.
When either is missing, all calls silently no-op without blocking or logging
errors. Never logs the bot token.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional, Set

try:
    import aiohttp
except ImportError:  # pragma: no cover - allows offline unit tests without aiohttp
    aiohttp = None  # type: ignore

log = logging.getLogger("notifier")

TELEGRAM_API_BASE = "https://api.telegram.org"
SEND_TIMEOUT_SEC = 8.0


class TelegramNotifier:
    """Non-blocking Telegram Bot API notifier with per-key rate throttling."""

    def __init__(
        self,
        bot_token: Optional[str],
        chat_id: Optional[str],
        *,
        throttle_sec: float = 60.0,
        session: Optional["aiohttp.ClientSession"] = None,
        prefix: str = "",
    ) -> None:
        self._token = (bot_token or "").strip()
        self._chat_id = (chat_id or "").strip()
        self.throttle_sec = max(float(throttle_sec), 0.0)
        self._session = session
        self.prefix = prefix
        self._last_sent: Dict[str, float] = {}
        self._tasks: Set[asyncio.Task] = set()

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    def attach_session(self, session: Optional["aiohttp.ClientSession"]) -> None:
        self._session = session

    def _sanitize(self, msg: str) -> str:
        if self._token and self._token in msg:
            return msg.replace(self._token, "<REDACTED>")
        return msg

    def should_send(self, key: str, *, throttle_sec: Optional[float] = None,
                    force: bool = False, now: Optional[float] = None) -> bool:
        if not self.enabled:
            return False
        if force:
            return True
        window = self.throttle_sec if throttle_sec is None else max(float(throttle_sec), 0.0)
        if window <= 0.0:
            return True
        ts = time.monotonic() if now is None else now
        last = self._last_sent.get(key)
        if last is not None and (ts - last) < window:
            return False
        return True

    def _format_text(self, text: str) -> str:
        if self.prefix:
            return f"{self.prefix}\n{text}"
        return text

    async def send(
        self,
        key: str,
        text: str,
        *,
        throttle_sec: Optional[float] = None,
        force: bool = False,
        now: Optional[float] = None,
    ) -> bool:
        """Send an alert asynchronously. Returns True if delivered."""
        if not self.should_send(key, throttle_sec=throttle_sec, force=force, now=now):
            return False
        ts = time.monotonic() if now is None else now
        self._last_sent[key] = ts
        return await self._post(self._format_text(text))

    def notify(
        self,
        key: str,
        text: str,
        *,
        throttle_sec: Optional[float] = None,
        force: bool = False,
    ) -> None:
        """Fire-and-forget non-blocking notification on the active event loop."""
        if not self.should_send(key, throttle_sec=throttle_sec, force=force):
            return
        self._last_sent[key] = time.monotonic()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._post(self._format_text(text)), name=f"tg-{key}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _post(self, text: str) -> bool:
        if not self.enabled:
            return False
        if aiohttp is None:
            log.debug("aiohttp unavailable; skipping Telegram send")
            return False
        url = f"{TELEGRAM_API_BASE}/bot{self._token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        own_session = self._session is None or getattr(self._session, "closed", False)
        session = self._session
        try:
            if own_session:
                session = aiohttp.ClientSession()
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=SEND_TIMEOUT_SEC),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(
                        "telegram send failed: HTTP %d %s",
                        resp.status,
                        self._sanitize(body[:200]),
                    )
                    return False
                return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("telegram send error: %s", self._sanitize(repr(e)))
            return False
        finally:
            if own_session and session is not None:
                try:
                    await session.close()
                except Exception:
                    pass

    async def flush(self, timeout: float = 5.0) -> None:
        """Wait briefly for any pending fire-and-forget notifications to complete."""
        if not self._tasks:
            return
        done, pending = await asyncio.wait(list(self._tasks), timeout=timeout)
        for t in pending:
            t.cancel()
