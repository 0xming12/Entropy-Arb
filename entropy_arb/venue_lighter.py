"""zkLighter venue adapter (Lighter mainnet / Lighter Robinhood chain).

Market data and account state come from Lighter's public REST + websocket
APIs via plain aiohttp/websockets, so --record-only data collection works
without the SDK. Trading lazily imports the official `lighter` SDK
(https://github.com/elliottech/lighter-python) for transaction signing only.

Market orders carry mandatory avg-execution-price protection and settle
asynchronously on the authenticated account_orders websocket; send_taker()
returns {status, outcome, filled_base, avg_px, err, unresolved, client_order_id}
and query_order() enables P0-1 terminal reconciliation on timeouts/restarts.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
from collections import OrderedDict
from typing import Callable, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - allows offline unit tests without aiohttp
    aiohttp = None  # type: ignore

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    try:
        from websockets import connect as ws_connect  # type: ignore
    except ImportError:  # pragma: no cover - allows offline unit tests without websockets
        ws_connect = None  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import LighterBookFeed

log = logging.getLogger("lighter")
AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0

_AMBIGUOUS_ERR_TOKENS = (
    "timeout", "timed out", "time out",
    "bad gateway", "service unavailable", "gateway",
    "temporarily unavailable", "internal server error",
    "connection", "disconnect", "reset by peer",
    "broken pipe", "eof", "server closed", "network",
)
_AMBIGUOUS_HTTP_CODE_RE = re.compile(
    r"(?:http|status|code|error)[^\d]{0,8}\b(?:408|500|502|503|504)\b|\b(?:502|503|504)\b"
)


def _is_ambiguous_submit_error(msg: str) -> bool:
    low = msg.lower()
    return (any(tok in low for tok in _AMBIGUOUS_ERR_TOKENS)
            or bool(_AMBIGUOUS_HTTP_CODE_RE.search(low)))


def _terminal_order(status: str) -> bool:
    return status in {"filled", "canceled"} or status.startswith("canceled-")


def _terminal_fill(order: dict) -> tuple[float, float]:
    base = float(order["filled_base_amount"])
    quote = float(order["filled_quote_amount"])
    if not (math.isfinite(base) and math.isfinite(quote) and base >= 0 and quote >= 0):
        raise ValueError("invalid executed quantities")
    if (base == 0) != (quote == 0) or (order["status"] == "filled" and base == 0):
        raise ValueError("contradictory fill status")
    return base, quote


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer,
                 on_disconnect: Optional[Callable[[str, str], None]] = None) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
        self.on_disconnect = on_disconnect
        self.ready = asyncio.Event()
        self._pending: dict[int, asyncio.Future] = {}
        self._terminal: OrderedDict[int, dict] = OrderedDict()

    def watch(self, coi: int) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if coi in self._terminal:
            fut.set_result(self._terminal[coi])
            return fut
        self._pending[coi] = fut
        return fut

    def unwatch(self, coi: int) -> None:
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def _resolve(self, coi: int, info: dict) -> None:
        self._terminal[coi] = info
        while len(self._terminal) > 512:
            self._terminal.popitem(last=False)
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.set_result(info)

    def _handle_orders(self, msg: dict) -> None:
        for lst in (msg.get("orders") or {}).values():
            for o in lst or []:
                status = str(o.get("status", ""))
                if not _terminal_order(status):
                    continue
                try:
                    coi = int(o.get("client_order_index"))
                except (TypeError, ValueError):
                    continue
                try:
                    fb, fq = _terminal_fill(o)
                except (KeyError, TypeError, ValueError):
                    continue
                self._resolve(coi, {"status": status, "filled_base": fb,
                                    "filled_quote": fq,
                                    "avg_px": (fq / fb) if fb > 0 else None})

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None:
                    raise RuntimeError(f"auth token: {err}")
                connected_at = time.time()
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        t = msg.get("type")
                        if t in ("subscribed/account_orders", "update/account_orders"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account orders stream ready", self.name)
                            self.ready.set()
                            self._handle_orders(msg)
                        elif t == "connected":
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_id}/"
                                           f"{self.account_index}",
                                "auth": auth}))
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
                        if (time.time() - connected_at > AUTH_REFRESH_SEC
                                and not self._pending):
                            log.info("[%s] refreshing account ws auth", self.name)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, e, backoff)
                if self.on_disconnect is not None and not stop.is_set():
                    self.on_disconnect(
                        self.name,
                        f"account orders ws error: {e} (retry in {backoff:.0f}s)")
                self.ready.clear()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            self.ready.clear()


class LighterVenue:
    kind = "lighter"

    def __init__(self, conf: VenueConf, session: Optional["aiohttp.ClientSession"],
                 settle_timeout_sec: float) -> None:
        assert conf.lighter_profile is not None
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.profile = conf.lighter_profile
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0
        self.equity = None
        self.free = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.price_decimals = 2
        self.size_decimals = 4
        self.min_base = 0.0
        self.min_quote = 10.0
        self.signer = None
        self.orders_feed: Optional[AccountOrdersFeed] = None
        self._book_feed: Optional[LighterBookFeed] = None
        self._coi = int(time.time() * 1000)

    # ------------------------------------------------------------------ REST

    async def _get(self, path: str, params: Optional[dict] = None):
        async with self.session.get(
                self.profile.api_url + path, params=params,
                timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        data = await self._get("/api/v1/orderBooks")
        for ob in data.get("order_books") or []:
            if ob.get("symbol") != self.conf.symbol:
                continue
            if ob.get("status") != "active":
                raise RuntimeError(f"[{self.name}] market status={ob.get('status')}")
            self.market_id = int(ob["market_id"])
            self.price_decimals = int(ob["supported_price_decimals"])
            self.size_decimals = int(ob["supported_size_decimals"])
            self.min_base = float(ob["min_base_amount"])
            self.min_quote = float(ob["min_quote_amount"])
            raw_tf = ob.get("taker_fee")
            try:
                if raw_tf is not None and float(raw_tf) > 0.0 and self.fee_bps <= 0.0:
                    log.warning("[%s] exchange reports non-zero taker_fee=%s while "
                                "config taker_fee_bps=%.2f — verify your fee config",
                                self.name, raw_tf, self.fee_bps)
            except (TypeError, ValueError):
                pass
            log.info("[%s] %s market_id=%d px_dec=%d sz_dec=%d min_base=%s "
                     "min_quote=%s taker_fee=%s", self.name, ob["symbol"],
                     self.market_id, self.price_decimals, self.size_decimals,
                     ob["min_base_amount"], ob["min_quote_amount"],
                     ob.get("taker_fee"))
            return
        raise RuntimeError(f"[{self.name}] {self.conf.symbol} not found on "
                           f"{self.profile.name}")

    def init_signer(self) -> None:
        c = self.conf.lighter_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from lighter import SignerClient
        except ImportError as e:
            raise RuntimeError(
                "live trading on Lighter needs the pinned official SDK — "
                "pip install -r requirements-live.txt") from e
        signer = SignerClient(
            url=self.profile.api_url,
            account_index=c.account_index,
            api_private_keys={c.api_key_index: c.api_private_key},
            chain_id=self.profile.chain_id,
        )
        err = signer.check_client()
        if err is not None:
            raise RuntimeError(f"[{self.name}] API key check failed: {err}")
        self.signer = signer
        log.info("[%s] signer ready (account %d)", self.name, c.account_index)

    def start_tasks(
        self,
        stop: asyncio.Event,
        notify,
        live: bool,
        on_disconnect: Optional[Callable[[str, str], None]] = None,
    ) -> list:
        self._book_feed = LighterBookFeed(
            self.name, self.profile.ws_url, self.market_id,
            self.book, notify, on_disconnect=on_disconnect,
        )
        tasks = [asyncio.create_task(self._book_feed.run(stop), name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer,
                on_disconnect=on_disconnect)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def request_reconnect(self) -> None:
        if self._book_feed is not None:
            self._book_feed.request_reconnect()

    def ready_to_trade(self) -> bool:
        return self.orders_feed is not None and self.orders_feed.ready.is_set()

    async def warm_http(self) -> None:
        try:
            await self._get("/api/v1/status")
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)
        if self.signer is None:
            return
        try:
            sess = self.signer.api_client.rest_client.pool_manager
            async with sess.get(self.profile.api_url + "/api/v1/status",
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
        except Exception as e:
            log.debug("[%s] signer keepalive failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        f = 10 ** self.price_decimals
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def _next_coi(self) -> int:
        self._coi += 1
        return self._coi

    def state_identity(self) -> str:
        return f"{self.profile.api_url}/{self.conf.lighter_creds.account_index}/{self.market_id}"

    def signer_identity(self) -> Optional[str]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None or c.api_key_index is None:
            return None
        return f"lighter-signer:{self.profile.api_url}/{c.account_index}/{c.api_key_index}"

    def reserve_order_id(self) -> str:
        return str(self._next_coi())

    async def query_order(self, client_order_id: str) -> dict:
        """Query terminal status of an order by client_order_index (P0-1).

        Checks the authenticated WebSocket cache, then queries by exact client ID.
        """
        try:
            coi = int(client_order_id)
        except (TypeError, ValueError):
            return {"status": "bad-id", "outcome": "unknown", "filled_base": 0.0,
                    "avg_px": None, "err": "invalid coi", "unresolved": True,
                    "client_order_id": str(client_order_id)}
        if self.orders_feed is not None and coi in self.orders_feed._terminal:
            info = self.orders_feed._terminal[coi]
            fb = float(info.get("filled_base") or 0.0)
            outcome = "filled" if fb > 0 else "canceled"
            return {"status": info["status"], "outcome": outcome,
                    "filled_base": fb, "avg_px": info.get("avg_px"),
                    "err": None, "unresolved": False,
                    "client_order_id": str(coi)}
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return {"status": "unknown", "outcome": "unknown", "filled_base": 0.0,
                    "avg_px": None, "err": "no account_index", "unresolved": True,
                    "client_order_id": str(coi)}
        try:
            params = {"account_index": str(c.account_index), "market_id": str(self.market_id),
                      "limit": "50"}
            if self.signer is None:
                data = await self._get("/api/v1/accountInactiveOrders", params=params)
            else:
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None or not auth:
                    raise RuntimeError("could not authorize Lighter order lookup")
                try:
                    orders = await self.signer.order_api.account_orders(
                        authorization=auth, client_order_indexes=str(coi),
                        account_index=c.account_index, _request_timeout=REST_TIMEOUT)
                except Exception as exc:
                    if (not isinstance(exc, (AttributeError, TypeError))
                            and getattr(exc, "status", None) not in (404, 405, 501)):
                        raise
                    orders = await self.signer.order_api.account_inactive_orders(
                        authorization=auth, account_index=c.account_index,
                        market_id=self.market_id, limit=50, _request_timeout=REST_TIMEOUT)
                data = orders.to_dict()
            if data.get("code", 200) != 200:
                raise RuntimeError("Lighter order query did not succeed")
            for o in data.get("orders") or []:
                try:
                    if (int(o.get("client_order_index", -1)) != coi
                            or int(o.get("market_index", self.market_id)) != self.market_id):
                        continue
                except (TypeError, ValueError):
                    continue
                status = str(o.get("status", ""))
                if not _terminal_order(status):
                    return {"status": status, "outcome": "unknown",
                            "filled_base": 0.0, "avg_px": None,
                            "err": None, "unresolved": True,
                            "client_order_id": str(coi)}
                try:
                    fb, fq = _terminal_fill(o)
                except (KeyError, TypeError, ValueError):
                    continue
                outcome = "filled" if fb > 0 else "canceled"
                return {"status": status, "outcome": outcome,
                        "filled_base": fb, "avg_px": (fq / fb) if fb > 0 else None,
                        "err": None, "unresolved": False,
                        "client_order_id": str(coi)}
            return {"status": "not-found", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True,
                    "client_order_id": str(coi)}
        except Exception as e:
            return {"status": "query-failed", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None,
                    "err": type(e).__name__, "unresolved": True,
                    "client_order_id": str(coi)}

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         client_order_id: Optional[str] = None) -> dict:
        """Send an IOC market order; attaches monotonic ``timing`` marks to the result."""
        timing: dict = {}
        res = await self._send_taker(is_buy=is_buy, qty=qty, limit_px=limit_px,
                                     reduce_only=reduce_only,
                                     client_order_id=client_order_id, timing=timing)
        if isinstance(res, dict):
            res["timing"] = timing
        return res

    async def _send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                          reduce_only: bool, client_order_id: Optional[str],
                          timing: dict) -> dict:
        assert self.signer is not None
        from lighter import SignerClient
        coi_s = client_order_id or self.reserve_order_id()
        coi = int(coi_s)
        base_amount = int(round(qty * 10 ** self.size_decimals))
        price = int(round(limit_px * 10 ** self.price_decimals))
        if base_amount <= 0 or price <= 0:
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": f"non-positive order size/price ({base_amount}, {price})",
                    "unresolved": False, "client_order_id": coi_s}
        fut = self.orders_feed.watch(coi) if self.orders_feed else None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settle_timeout
        timing["submit_start_mono"] = time.monotonic()
        try:
            _tx, resp, err = await asyncio.wait_for(self.signer.create_order(
                market_index=self.market_id,
                client_order_index=coi,
                base_amount=base_amount,
                price=price,
                is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_MARKET,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            ), timeout=max(0.0, deadline - loop.time()))
            timing["submit_end_mono"] = time.monotonic()
        except asyncio.CancelledError:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            raise
        except Exception as e:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            if isinstance(e, asyncio.TimeoutError):
                nm = getattr(self.signer, "nonce_manager", None)
                if nm is not None and hasattr(nm, "acknowledge_failure"):
                    try:
                        c = self.conf.lighter_creds
                        if c is not None and c.api_key_index is not None:
                            nm.acknowledge_failure(c.api_key_index)
                    except Exception:
                        pass
            msg = type(e).__name__
            status_code = getattr(e, "status", None)
            # Only a definite HTTP rejection proves the submission failed.
            if status_code not in (400, 401, 403, 404, 422, 429):
                return {"status": "send-unknown", "outcome": "unknown",
                        "filled_base": 0.0, "avg_px": None,
                        "err": msg, "unresolved": True,
                        "client_order_id": coi_s}
            if status_code == 429:
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False,
                    "client_order_id": coi_s}
        if err is None and getattr(resp, "code", None) != 200:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            return {"status": "send-unknown", "outcome": "unknown", "filled_base": 0.0,
                    "avg_px": None, "err": "unconfirmed Lighter submit response",
                    "unresolved": True, "client_order_id": coi_s}
        if err is not None:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = str(err)
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            elif _is_ambiguous_submit_error(msg):
                # SDK may swallow 5xx/timeout/connection errors into `err`; treat as
                # unresolved so P0-1 reconciliation checks actual fill/position state.
                return {"status": "send-unknown", "outcome": "unknown",
                        "filled_base": 0.0, "avg_px": None,
                        "err": msg, "unresolved": True,
                        "client_order_id": coi_s}
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False,
                    "client_order_id": coi_s}
        if fut is None:
            return {"status": "sent-unconfirmed", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True,
                    "client_order_id": coi_s}
        try:
            info = await asyncio.wait_for(fut, timeout=max(0.0, deadline - loop.time()))
            timing["settle_mono"] = time.monotonic()
            fb = float(info["filled_base"])
            outcome = "filled" if fb > 0 else "canceled"
            return {"status": info["status"], "outcome": outcome,
                    "filled_base": fb, "avg_px": info.get("avg_px"),
                    "err": None, "unresolved": False,
                    "client_order_id": coi_s}
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(coi)
            log.warning("[%s] no settle confirmation for coi %d in %.1fs",
                        self.name, coi, self.settle_timeout)
            return {"status": "timeout", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True,
                    "client_order_id": coi_s}
        except asyncio.CancelledError:
            self.orders_feed.unwatch(coi)
            raise

    # -------------------------------------------------------------- accounts

    async def _account(self) -> Optional[dict]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params={"by": "index",
                                       "value": str(c.account_index)})
        if (not isinstance(data, dict) or data.get("code") != 200
                or not isinstance(data.get("accounts"), list)):
            raise RuntimeError(f"[{self.name}] invalid account snapshot")
        for acct in data["accounts"]:
            if int(acct.get("account_index", acct.get("index", -1))) == c.account_index:
                return acct
        return None

    async def fetch_equity(self):
        acct = await self._account()
        if acct is None:
            return None
        return (float(acct.get("total_asset_value") or 0.0),
                float(acct.get("available_balance") or 0.0))

    async def fetch_position(self) -> float:
        acct = await self._account()
        if acct is None:
            raise RuntimeError(f"[{self.name}] account not found")
        if not isinstance(acct.get("positions"), list):
            raise RuntimeError(f"[{self.name}] invalid position snapshot")
        for p in acct["positions"]:
            if int(p["market_id"]) == self.market_id:
                size, sign = float(p["position"]), float(p["sign"])
                if not math.isfinite(size) or size < 0 or (size and sign not in (-1.0, 1.0)):
                    raise RuntimeError(f"[{self.name}] invalid position quantities")
                return sign * size if size else 0.0
        return 0.0

    async def fetch_external_transfers(self, start: float, end: float) -> Optional[float]:
        if self.signer is None or self.conf.lighter_creds is None:
            return None
        index = self.conf.lighter_creds.account_index
        auth, err = self.signer.create_auth_token_with_expiry()
        if err is not None or not auth:
            return None
        account = await self._account()
        if not account or not account.get("l1_address"):
            return None
        assets = await self.signer.order_api.asset_details(_request_timeout=10.0)
        if assets.code != 200:
            return None
        collateral = "USDG" if self.profile.name == "robinhood" else "USDC"
        asset_ids = {asset.asset_id for asset in assets.asset_details if asset.symbol == collateral}
        if not asset_ids:
            return None

        async def total(method, field, signed_amount, **params):
            amounts = {}
            cursor = None
            seen_cursors = set()
            while True:
                page = await method(account_index=index, authorization=auth, cursor=cursor,
                                    _request_timeout=10.0, **params)
                if page.code != 200:
                    raise RuntimeError("cashflow history request failed")
                passed_start = False
                for row in getattr(page, field):
                    timestamp = row.timestamp / 1000 if row.timestamp >= 10**11 else row.timestamp
                    passed_start |= timestamp < start
                    if start <= timestamp <= end and row.asset_id in asset_ids:
                        amounts[row.id] = signed_amount(row)
                if not page.cursor or passed_start:
                    return sum(amounts.values())
                if page.cursor in seen_cursors:
                    raise RuntimeError("cashflow history cursor repeated")
                seen_cursors.add(page.cursor)
                cursor = page.cursor

        tx = self.signer.tx_api
        amounts = await asyncio.gather(
            total(tx.deposit_history, "deposits",
                  lambda row: float(row.amount) if row.status == "completed" else 0.0,
                  l1_address=account["l1_address"]),
            total(tx.withdraw_history, "withdraws",
                  lambda row: -float(row.amount)
                  if row.status in {"completed", "claimable"} else 0.0),
            total(tx.transfer_history, "transfers",
                  lambda row: (int(row.to_account_index == index)
                               - int(row.from_account_index == index)) * float(row.amount)
                  if row.type in {"L2TransferInflow", "L2TransferOutflow"} else 0.0),
        )
        return sum(amounts)

    async def fetch_funding_pnl(self, start: float, end: float) -> Optional[float]:
        from lighter import AccountApi

        if self.signer is None or self.conf.lighter_creds is None:
            return None
        auth, err = self.signer.create_auth_token_with_expiry()
        if err is not None or not auth:
            return None
        api = AccountApi(self.signer.api_client)
        amounts = {}
        cursor = None
        seen_cursors = set()
        while True:
            page = await api.position_funding(
                account_index=self.conf.lighter_creds.account_index,
                authorization=auth, market_ids=[self.market_id], limit=100,
                cursor=cursor, _request_timeout=10.0,
            )
            if page.code != 200:
                return None
            passed_start = False
            for row in page.position_fundings:
                timestamp = row.timestamp / 1000 if row.timestamp >= 10**11 else row.timestamp
                passed_start |= timestamp < start
                if row.market_id == self.market_id and start <= timestamp <= end:
                    amounts[row.funding_id] = float(row.change)
            if not page.next_cursor or passed_start:
                return sum(amounts.values())
            if page.next_cursor in seen_cursors:
                return None
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor

    async def fetch_funding_rate_hourly(self) -> Optional[float]:
        try:
            data = await self._get("/api/v1/funding-rates")
            rates = data.get("funding_rates") or []
            for item in rates:
                if str(item.get("exchange", "")).lower() != "lighter":
                    continue
                if int(item.get("market_id", -1)) != self.market_id:
                    continue
                sym = item.get("symbol")
                if sym is not None and str(sym) != self.conf.symbol:
                    continue
                raw_rate = item.get("rate")
                if raw_rate is not None:
                    return float(raw_rate) / 8.0
            log.warning("[%s] lighter funding rate not found for market_id=%d (%s)",
                        self.name, self.market_id, self.conf.symbol)
            return None
        except Exception as e:
            log.warning("[%s] funding rate fetch failed: %r", self.name, e)
            return None

    async def close(self) -> None:
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
