"""Hyperliquid HIP-3 dex venue adapter (Entropy = dex "io", trade.xyz = "xyz").

Market metadata, account state and order posting use Hyperliquid's public
/info and /exchange REST endpoints via plain aiohttp; the book comes from the
OFFICIAL websocket (see feeds.HLBookFeed). Trading lazily imports the
official `hyperliquid-python-sdk` signing helpers + eth_account —
--record-only data collection needs neither.

IOC limit orders settle synchronously in the /exchange response; unknown
outcomes (timeout/5xx) fall back to orderStatus-by-cloid polling inside
send_taker() and preserve client_order_id + outcome for P0-1 reconciliation.
When IOC receipts omit fees, fetch_fill_fee() verifies actual fee via
userFills by order_id / cloid and coin (P1-3).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Callable, Optional
from uuid import uuid4

try:
    import aiohttp
except ImportError:  # pragma: no cover - allows offline unit tests without aiohttp
    aiohttp = None  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import HLBookFeed

log = logging.getLogger("hl")

INFO_TIMEOUT = 10.0
USD_FEE_TOKENS = frozenset({"USDC", "USDH", "USDT", "USDE", "USDG", "USDS"})
FILL_FEE_LOOKBACK_SEC = 300.0


class NonceAllocator:
    def __init__(self) -> None:
        self._last = 0

    def next(self) -> int:
        self._last = max(self._last + 1, int(time.time() * 1000))
        return self._last


class HLAccount:
    def __init__(self, private_key: str, account_address: Optional[str],
                 api_url: str) -> None:
        from eth_account import Account
        if not account_address:
            raise RuntimeError(
                "HL_ACCOUNT_ADDRESS is required for live trading — never fall back to "
                "the agent wallet address (positions/margins live on the main account)")
        self.wallet = Account.from_key(private_key)
        self.query_address = account_address.strip().lower()
        self.is_mainnet = api_url == "https://api.hyperliquid.xyz"
        self.nonces = NonceAllocator()

    def describe(self) -> str:
        s = f"signer={self.wallet.address} account={self.query_address}"
        if self.wallet.address.lower() != self.query_address:
            s += " (agent mode)"
        return s


class HLVenue:
    kind = "hl"

    def __init__(self, conf: VenueConf, api_url: str, ws_url: str,
                 session: Optional["aiohttp.ClientSession"], settle_timeout_sec: float) -> None:
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.api_url = api_url
        self.ws_url = ws_url
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0
        self.equity = None
        self.free = None
        self.start_equity = None
        self.include_core_equity = True
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.account: Optional[HLAccount] = None
        self.coin = ""
        self.asset_id = -1
        self.size_decimals = 0
        self.min_base = 0.0
        self.min_quote = 10.0
        self.collateral_coin = "USDC"
        self.deployer_fee_scale: Optional[float] = None
        self._user_abstraction: Optional[str] = None
        self._signing = None
        self._book_feed: Optional[HLBookFeed] = None

    async def _info(self, payload: dict):
        async with self.session.post(
                self.api_url + "/info", json=payload,
                timeout=aiohttp.ClientTimeout(total=INFO_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    async def load_market(self) -> None:
        dexs = await self._info({"type": "perpDexs"})
        names = [(d or {}).get("name", "") for d in dexs]
        if self.conf.hl_dex not in names:
            raise RuntimeError(f"[{self.name}] dex '{self.conf.hl_dex}' not "
                               f"found on Hyperliquid (available: "
                               f"{[n for n in names if n][:20]}...)")
        dex_index = names.index(self.conf.hl_dex)
        dex_info = dexs[dex_index] or {}
        if dex_info.get("deployerFeeScale") is not None:
            try:
                self.deployer_fee_scale = float(dex_info["deployerFeeScale"])
            except (TypeError, ValueError):
                pass
        meta = await self._info({"type": "meta", "dex": self.conf.hl_dex})
        if meta.get("deployerFeeScale") is not None:
            try:
                self.deployer_fee_scale = float(meta["deployerFeeScale"])
            except (TypeError, ValueError):
                pass
        col_idx = meta.get("collateralToken")
        if isinstance(col_idx, int):
            try:
                spot_meta = await self._info({"type": "spotMeta"})
                for tok in (spot_meta or {}).get("tokens") or []:
                    if tok.get("index") == col_idx and tok.get("name"):
                        self.collateral_coin = str(tok["name"]).upper()
                        break
            except Exception as e:
                log.debug("[%s] spotMeta collateral lookup skipped: %r", self.name, e)
        want = f"{self.conf.hl_dex}:{self.conf.symbol}"
        for idx, a in enumerate(meta["universe"]):
            if a["name"] not in (want, self.conf.symbol):
                continue
            if a.get("isDelisted"):
                raise RuntimeError(f"[{self.name}] {a['name']} is delisted")
            self.coin = a["name"]
            self.asset_id = 110000 + (dex_index - 1) * 10000 + idx
            self.size_decimals = int(a["szDecimals"])
            self.min_base = 10 ** -self.size_decimals
            if a.get("deployerFeeScale") is not None:
                try:
                    self.deployer_fee_scale = float(a["deployerFeeScale"])
                except (TypeError, ValueError):
                    pass
            log.info("[%s] %s asset_id=%d szDecimals=%d collateral=%s maxLev=%sx %s",
                     self.name, self.coin, self.asset_id, self.size_decimals,
                     self.collateral_coin, a.get("maxLeverage"),
                     "isolated-only" if a.get("onlyIsolated") else "")
            return
        raise RuntimeError(f"[{self.name}] {want} not found")

    def init_signer(self) -> None:
        c = self.conf.hl_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from hyperliquid.utils import signing as hl_signing
        except ImportError as e:
            raise RuntimeError(
                "live trading on Hyperliquid needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(hyperliquid-python-sdk)") from e
        self._signing = hl_signing
        self.account = HLAccount(c.private_key, c.account_address, self.api_url)
        log.info("[%s] %s", self.name, self.account.describe())

    async def verify_account(self) -> None:
        """Verify HL signer <-> HL_ACCOUNT_ADDRESS binding via userRole & extraAgents.

        Blocks startup if an agent wallet key is paired with a missing, wrong, or
        self-referential HL_ACCOUNT_ADDRESS, or if the agent authorization has expired.
        """
        assert self.account is not None
        signer_addr = self.account.wallet.address.lower()
        query_addr = self.account.query_address
        try:
            role_resp = await self._info({"type": "userRole", "user": signer_addr})
        except Exception as e:
            raise RuntimeError(
                f"[{self.name}] failed to verify Hyperliquid signer role ({signer_addr}): {e!r}"
            ) from e
        if not isinstance(role_resp, dict) or not isinstance(role_resp.get("role"), str):
            raise RuntimeError(
                f"[{self.name}] unexpected Hyperliquid userRole response: {role_resp!r}"
            )
        role = role_resp["role"]
        if role == "agent":
            owner = str((role_resp.get("data") or {}).get("user") or "").lower()
            if not owner:
                raise RuntimeError(
                    f"[{self.name}] signer {signer_addr} is an agent wallet with unknown owner"
                )
            if owner != query_addr:
                raise RuntimeError(
                    f"[{self.name}] HL_ACCOUNT_ADDRESS mismatch: signer {signer_addr} is an "
                    f"approved agent for {owner}, NOT {query_addr}. Fix HL_ACCOUNT_ADDRESS in .env"
                )
            try:
                agents = await self._info({"type": "extraAgents", "user": query_addr})
            except Exception as e:
                raise RuntimeError(
                    f"[{self.name}] failed to verify extraAgents for {query_addr}: {e!r}"
                ) from e
            if isinstance(agents, list):
                now_ms = int(time.time() * 1000)
                for ag in agents:
                    if str((ag or {}).get("address", "")).lower() == signer_addr:
                        valid_until = ag.get("validUntil")
                        if valid_until is not None and int(valid_until) <= now_ms:
                            raise RuntimeError(
                                f"[{self.name}] agent wallet {signer_addr} authorization expired "
                                f"(validUntil={valid_until})"
                            )
                        break
            log.info("[%s] verified agent %s -> main account %s",
                     self.name, signer_addr, query_addr)
            return
        if role == "user":
            if signer_addr != query_addr:
                raise RuntimeError(
                    f"[{self.name}] signer {signer_addr} is a main user wallet, not an approved "
                    f"agent of HL_ACCOUNT_ADDRESS={query_addr}"
                )
            log.info("[%s] verified main wallet %s", self.name, query_addr)
            return
        raise RuntimeError(
            f"[{self.name}] Hyperliquid signer {signer_addr} has unsupported/unapproved role "
            f"{role!r} for HL_ACCOUNT_ADDRESS={query_addr}"
        )

    def share_nonces_with(self, other: "HLVenue") -> None:
        if (self.account and other.account and
                self.account.wallet.address == other.account.wallet.address):
            other.account.nonces = self.account.nonces
            log.info("[%s]/[%s] same signer — shared nonce allocator",
                     self.name, other.name)

    def start_tasks(
        self,
        stop: asyncio.Event,
        notify,
        live: bool,
        on_disconnect: Optional[Callable[[str, str], None]] = None,
    ) -> list:
        self._book_feed = HLBookFeed(
            self.name, self.ws_url, self.coin, self.book,
            notify, on_disconnect=on_disconnect,
        )
        return [asyncio.create_task(self._book_feed.run(stop), name=f"book-{self.key}")]

    def request_reconnect(self) -> None:
        if self._book_feed is not None:
            self._book_feed.request_reconnect()

    def ready_to_trade(self) -> bool:
        return self.account is not None

    async def warm_http(self) -> None:
        try:
            await self._info({"type": "exchangeStatus"})
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        if px <= 0:
            return px
        max_dec = max(0, 6 - self.size_decimals)
        sig_dec = 4 - math.floor(math.log10(px))
        dec = max(0, min(max_dec, sig_dec))
        f = 10.0 ** dec
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def state_identity(self) -> str:
        return f"{self.api_url}/{self._query_address()}/{self.coin}"

    def signer_identity(self) -> Optional[str]:
        if self.account is None:
            return None
        return f"hl-signer:{self.api_url}/{self.account.wallet.address.lower()}"

    def reserve_order_id(self) -> str:
        return f"0x{uuid4().hex}"

    async def query_order(self, client_order_id: str) -> dict:
        """Query terminal status of an order by cloid (P0-1)."""
        addr = self._query_address()
        if addr is None:
            return {"status": "unknown", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None, "err": "no query address",
                    "unresolved": True, "client_order_id": client_order_id}
        try:
            st = await self._info({"type": "orderStatus",
                                   "user": addr,
                                   "oid": client_order_id})
        except Exception as e:
            return {"status": "query-failed", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None, "err": repr(e),
                    "unresolved": True, "client_order_id": client_order_id}
        if not isinstance(st, dict):
            return {"status": "query-failed", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None, "err": "bad orderStatus shape",
                    "unresolved": True, "client_order_id": client_order_id}
        top_status = st.get("status")
        if top_status == "unknownOid":
            return {"status": "not-found", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None, "err": None,
                    "unresolved": True, "client_order_id": client_order_id}
        if top_status == "order":
            o = st.get("order") or {}
            status = str(o.get("status", ""))
            inner = o.get("order") or {}
            oid = inner.get("oid")
            try:
                original, remaining = float(inner["origSz"]), float(inner["sz"])
                filled = original - remaining if original >= remaining >= 0 else -1.0
            except (KeyError, TypeError, ValueError):
                filled = -1.0
            terminal = (status in {"filled", "canceled", "rejected"}
                        or status.endswith(("Canceled", "Rejected")))
            if (terminal and math.isfinite(filled) and filled >= 0
                    and (status != "filled" or filled > 0)):
                outcome = "filled" if filled > 0 else "canceled"
                result = {"status": status, "outcome": outcome,
                          "filled_base": filled, "avg_px": None,
                          "order_id": oid, "err": None,
                          "unresolved": False, "client_order_id": client_order_id}
                if filled > 0 and oid is not None:
                    execution = await self.fetch_fill_fee(
                        order_id=oid, use_full_history=True)
                    if execution is not None and math.isclose(
                            execution["filled_base"], filled, rel_tol=0.0, abs_tol=1e-9):
                        for key in ("avg_px", "fee_usd", "fee_bps", "fee_token"):
                            result[key] = execution[key]
                return result
        return {"status": "open", "outcome": "unknown",
                "filled_base": 0.0, "avg_px": None, "err": None,
                "unresolved": True, "client_order_id": client_order_id}

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         client_order_id: Optional[str] = None) -> dict:
        from hyperliquid.utils.types import Cloid
        assert self.account is not None and self.asset_id >= 0
        s = self._signing
        cloid_raw = client_order_id or self.reserve_order_id()
        cloid = Cloid.from_str(cloid_raw)
        order_req = {"coin": self.coin, "is_buy": is_buy, "sz": round(qty, 8),
                     "limit_px": limit_px,
                     "order_type": {"limit": {"tif": "Ioc"}},
                     "reduce_only": reduce_only, "cloid": cloid}
        try:
            wire = s.order_request_to_order_wire(order_req, self.asset_id)
            action = s.order_wires_to_order_action([wire])
            nonce = self.account.nonces.next()
            sig = s.sign_l1_action(self.account.wallet, action, None, nonce,
                                   None, self.account.is_mainnet)
            payload = {"action": action, "nonce": nonce, "signature": sig,
                       "vaultAddress": None, "expiresAfter": None}
        except Exception as e:
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": f"signing failed: {e!r}", "unresolved": False,
                    "client_order_id": cloid_raw}

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settle_timeout
        try:
            body, err, unresolved = await asyncio.wait_for(
                self._post_exchange(payload), timeout=max(0.0, deadline - loop.time()))
        except asyncio.TimeoutError:
            body, err, unresolved = None, None, True
        if err is not None:
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": err, "unresolved": False,
                    "client_order_id": cloid_raw}
        if not unresolved:
            res = self._parse(body)
            res["client_order_id"] = cloid_raw
            if not res.get("unresolved"):
                return res
        # unknown outcome: poll orderStatus by cloid until the deadline
        while (remaining := deadline - loop.time()) > 0.0:
            try:
                q = await asyncio.wait_for(self.query_order(cloid_raw), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if not q.get("unresolved"):
                return q
            await asyncio.sleep(max(0.0, min(0.5, deadline - loop.time())))
        return {"status": "timeout", "outcome": "unknown",
                "filled_base": 0.0, "avg_px": None,
                "err": None, "unresolved": True,
                "client_order_id": cloid_raw}

    async def _post_exchange(self, payload: dict):
        try:
            async with self.session.post(
                    self.api_url + "/exchange", json=payload,
                    timeout=aiohttp.ClientTimeout(total=INFO_TIMEOUT)) as r:
                text = await r.text()
                if r.status == 429:
                    return None, f"RATE_LIMITED: HTTP 429 {text[:150]}", False
                if r.status in (408, 409):
                    return None, None, True
                if 400 <= r.status < 500:
                    return None, f"HTTP {r.status}: {text[:250]}", False
                if r.status >= 500:
                    return None, None, True
                return json.loads(text), None, False
        except (asyncio.TimeoutError, aiohttp.ClientError, json.JSONDecodeError):
            return None, None, True

    @staticmethod
    def _parse(body: dict) -> dict:
        def fail(msg: str) -> dict:
            low = msg.lower()
            if "rate limit" in low or "too many" in low:
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "outcome": "rejected",
                    "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False}
        def unknown(msg: str) -> dict:
            return {"status": "unknown", "outcome": "unknown", "filled_base": 0.0,
                    "avg_px": None, "err": msg, "unresolved": True}

        if not isinstance(body, dict):
            return unknown("unexpected response type")
        if body.get("status") == "err":
            return fail(str(body.get("response")))
        if body.get("status") != "ok":
            return unknown("unexpected order response")
        try:
            st = body["response"]["data"]["statuses"][0]
        except (KeyError, IndexError, TypeError):
            return unknown("malformed order response")
        if not isinstance(st, dict):
            return unknown("malformed order status")
        if "filled" in st:
            f = st["filled"]
            try:
                filled_base = float(f["totalSz"])
                avg_px = float(f["avgPx"])
            except (KeyError, TypeError, ValueError):
                return unknown("malformed fill response")
            if not (math.isfinite(filled_base) and filled_base > 0
                    and math.isfinite(avg_px) and avg_px > 0):
                return unknown("invalid fill quantity or price")
            out = {"status": "filled", "outcome": "filled",
                   "filled_base": filled_base,
                   "avg_px": avg_px,
                   "order_id": f.get("oid"),
                   "err": None, "unresolved": False}
            if f.get("fee") is not None and filled_base > 0 and avg_px:
                try:
                    ft = f.get("feeToken")
                    if ft is None or str(ft).upper() in USD_FEE_TOKENS:
                        fee_usd = abs(float(f["fee"]))
                        out["fee_usd"] = fee_usd
                        out["fee_bps"] = (fee_usd / (filled_base * avg_px)) * 1e4
                        out["fee_token"] = ft
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
            return out
        if "error" in st:
            msg = str(st["error"])
            if "could not immediately match" in msg.lower():
                return {"status": "canceled", "outcome": "canceled",
                        "filled_base": 0.0, "avg_px": None,
                        "err": None, "unresolved": False}
            return fail(msg)
        if "resting" in st:
            return {"status": "resting?", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True}
        return {"status": "unknown", "outcome": "unknown",
                "filled_base": 0.0, "avg_px": None,
                "err": f"unknown status: {str(st)[:150]}", "unresolved": True}

    # -------------------------------------------------------------- accounts

    def _query_address(self):
        if self.account is not None:
            return self.account.query_address
        c = self.conf.hl_creds
        return c.account_address.lower() if c and c.account_address else None

    async def fetch_fill_fee(
        self,
        *,
        order_id=None,
        client_order_id: Optional[str] = None,
        coin: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        use_full_history: bool = False,
    ) -> Optional[dict]:
        """Verify actual paid fee from Hyperliquid fills by oid/cloid & coin (P1-3).

        Uses userFillsByTime (recent window) by default to avoid the high
        rate-limit weight of fetching 2,000 historical fills on every trade.
        """
        addr = self._query_address()
        if addr is None:
            return None
        target_coin = coin or self.coin or f"{self.conf.hl_dex}:{self.conf.symbol}"
        try:
            if use_full_history:
                fills = await self._info({"type": "userFills", "user": addr})
            else:
                since_ms = (
                    int(start_time_ms)
                    if start_time_ms is not None
                    else int((time.time() - FILL_FEE_LOOKBACK_SEC) * 1000)
                )
                fills = await self._info({
                    "type": "userFillsByTime",
                    "user": addr,
                    "startTime": max(0, since_ms),
                })
            if not isinstance(fills, list):
                return None
            total_fee = 0.0
            total_notional = 0.0
            total_base = 0.0
            fee_token = None
            matched_any = False
            for fl in fills:
                if fl.get("coin") != target_coin:
                    continue
                oid_match = (order_id is not None and str(fl.get("oid")) == str(order_id))
                cloid_match = (
                    client_order_id is not None
                    and fl.get("cloid") is not None
                    and str(fl.get("cloid")).lower() == str(client_order_id).lower()
                )
                if not (oid_match or cloid_match):
                    continue
                if fl.get("fee") is None:
                    continue
                ft = fl.get("feeToken")
                if ft is not None and str(ft).upper() not in USD_FEE_TOKENS:
                    log.warning("[%s] ignoring non-USD feeToken %r in fill fee verification",
                                self.name, ft)
                    return None
                sz = abs(float(fl.get("sz") or 0.0))
                px = float(fl.get("px") or 0.0)
                if sz <= 0.0 or px <= 0.0:
                    continue
                total_fee += abs(float(fl["fee"]))
                total_notional += sz * px
                total_base += sz
                fee_token = ft or fee_token
                matched_any = True
            if not matched_any or total_notional <= 0.0:
                return None
            return {
                "fee_usd": total_fee,
                "filled_base": total_base,
                "avg_px": total_notional / total_base,
                "fee_bps": (total_fee / total_notional) * 1e4,
                "fee_token": fee_token,
                "order_id": order_id,
                "client_order_id": client_order_id,
                "coin": target_coin,
                "estimated": False,
            }
        except Exception as e:
            log.warning("[%s] userFills fee lookup failed: %r", self.name, e)
            return None

    async def _fetch_unified_free_margin(self, addr: str) -> Optional[float]:
        """Read free collateral from spotClearinghouseState for Unified/Portfolio accounts."""
        try:
            mode = self._user_abstraction
            if mode is None:
                raw_mode = await self._info({"type": "userAbstraction", "user": addr})
                if isinstance(raw_mode, str):
                    mode = raw_mode
                elif isinstance(raw_mode, dict):
                    mode = str(raw_mode.get("abstraction") or raw_mode.get("mode") or "")
                else:
                    mode = "default"
                self._user_abstraction = mode
            if mode not in ("unifiedAccount", "portfolioMargin"):
                return None
            spot = await self._info({"type": "spotClearinghouseState", "user": addr})
            if not isinstance(spot, dict):
                return None
            balances = spot.get("balances") or []
            target = (self.collateral_coin or "USDC").upper()
            for bal in balances:
                if str(bal.get("coin", "")).upper() == target:
                    total = float(bal.get("total") or 0.0)
                    hold = float(bal.get("hold") or 0.0)
                    if math.isfinite(total) and math.isfinite(hold):
                        return max(total - hold, 0.0)
            return 0.0
        except Exception as e:
            log.debug("[%s] unified account margin lookup skipped: %r", self.name, e)
            return None

    async def fetch_equity(self):
        addr = self._query_address()
        if addr is None:
            return None
        unified_free = await self._fetch_unified_free_margin(addr)
        if self.include_core_equity:
            try:
                p = await self._info({"type": "portfolio", "user": addr})
                for period, d in p:
                    if period == "day":
                        hist = d.get("accountValueHistory") or []
                        if hist:
                            free_val = unified_free
                            if free_val is None:
                                try:
                                    st = await self._info(
                                        {"type": "clearinghouseState", "user": addr,
                                         "dex": self.conf.hl_dex})
                                    if st.get("withdrawable") is not None:
                                        free_val = float(st["withdrawable"])
                                except Exception:
                                    pass
                            return float(hist[-1][1]), free_val
            except Exception as e:
                log.debug("[%s] portfolio fetch failed, falling back: %r",
                          self.name, e)
        dexs = [self.conf.hl_dex] + ([""] if self.include_core_equity else [])
        eq = fr = 0.0
        for dex in dexs:
            st = await self._info({"type": "clearinghouseState", "user": addr,
                                   "dex": dex})
            ms = st.get("marginSummary") or {}
            eq += float(ms.get("accountValue") or 0.0)
            fr += float(st.get("withdrawable") or 0.0)
        return eq, (unified_free if unified_free is not None else fr)

    async def fetch_position(self) -> float:
        addr = self._query_address()
        assert addr is not None
        st = await self._info({"type": "clearinghouseState", "user": addr,
                               "dex": self.conf.hl_dex})
        if not isinstance(st, dict) or not isinstance(st.get("assetPositions"), list):
            raise RuntimeError(f"[{self.name}] invalid position snapshot")
        for ap in st["assetPositions"]:
            pos = ap["position"]
            if not isinstance(pos["coin"], str):
                raise RuntimeError(f"[{self.name}] invalid position row")
            if pos.get("coin") == self.coin:
                return float(pos["szi"])
        return 0.0

    async def fetch_taker_fee_bps(self) -> Optional[float]:
        """Compute live effective taker fee (bps) using Hyperliquid's official HIP-3 formula.

        Official HIP-3 taker formula:
          scale_if_hip3 = deployerFeeScale + 1   (if deployerFeeScale < 1)
                        = deployerFeeScale * 2   (if deployerFeeScale >= 1)
          growth_scale  = 0.1 if growthMode enabled else 1.0
          eff_rate      = userCrossRate * scale_if_hip3 * growth_scale * (1 - activeReferralDiscount)

        Fails closed (returns None) if metadata lookup fails or target asset is missing.
        """
        addr = self._query_address()
        if addr is None:
            return None
        try:
            fees = await self._info({"type": "userFees", "user": addr})
            if not isinstance(fees, dict):
                return None
            base_rate = float(fees.get("userCrossRate") or 0.0)
            if not math.isfinite(base_rate) or base_rate <= 0.0:
                return None
            ref_disc = float(fees.get("activeReferralDiscount") or 0.0)
            if not math.isfinite(ref_disc):
                ref_disc = 0.0
            ref_disc = min(max(ref_disc, 0.0), 1.0)

            if not self.conf.hl_dex:
                return base_rate * (1.0 - ref_disc) * 1e4

            meta = await self._info({"type": "meta", "dex": self.conf.hl_dex})
            if not isinstance(meta, dict):
                return None
            want = self.coin or f"{self.conf.hl_dex}:{self.conf.symbol}"
            asset = None
            for a in meta.get("universe") or []:
                if a.get("isDelisted"):
                    continue
                if a.get("name") in (want, self.conf.symbol):
                    asset = a
                    break
            if asset is None:
                log.warning("[%s] asset %s not found in meta(dex=%s); fee unknown",
                            self.name, want, self.conf.hl_dex)
                return None

            raw_dfs = asset.get("deployerFeeScale")
            if raw_dfs is None:
                raw_dfs = meta.get("deployerFeeScale")
            if raw_dfs is None:
                raw_dfs = self.deployer_fee_scale
            dfs = 1.0 if raw_dfs is None else float(raw_dfs)
            if not math.isfinite(dfs) or dfs < 0.0:
                return None
            scale_if_hip3 = (dfs + 1.0) if dfs < 1.0 else (dfs * 2.0)

            gm = asset.get("growthMode")
            growth_scale = 0.1 if (gm == "enabled" or gm is True) else 1.0
            return base_rate * scale_if_hip3 * growth_scale * (1.0 - ref_disc) * 1e4
        except Exception as e:
            log.warning("[%s] taker fee fetch failed: %r", self.name, e)
            return None

    async def fetch_external_transfers(self, start: float, end: float) -> Optional[float]:
        address = self._query_address()
        if address is None:
            return None
        rows = await self._info({"type": "userNonFundingLedgerUpdates", "user": address,
                                 "startTime": int(start * 1000),
                                 "endTime": int(end * 1000)})
        if not isinstance(rows, list):
            return None
        amounts = {}
        for row in rows:
            delta = row["delta"]
            kind = delta.get("type")
            if kind == "deposit":
                signed = float(delta["usdc"])
            elif kind == "withdraw":
                signed = -float(delta["usdc"])
            elif kind in {"internalTransfer", "subAccountTransfer"}:
                direction = (int(delta["destination"].lower() == address.lower())
                             - int(delta["user"].lower() == address.lower()))
                signed = direction * float(delta["usdc"])
            else:
                continue
            amounts[(row["time"], row["hash"], kind)] = signed
        return sum(amounts.values())

    async def fetch_funding_pnl(self, start: float, end: float) -> Optional[float]:
        address = self._query_address()
        if address is None:
            return None
        rows = await self._info({"type": "userFunding", "user": address,
                                 "startTime": int(start * 1000),
                                 "endTime": int(end * 1000)})
        if not isinstance(rows, list):
            return None
        amounts = {}
        for row in rows:
            delta = row["delta"]
            if delta.get("coin") == self.coin:
                amounts[(row["time"], row["hash"])] = float(delta["usdc"])
        return sum(amounts.values())

    async def fetch_funding_rate_hourly(self) -> Optional[float]:
        try:
            data = await self._info(
                {"type": "metaAndAssetCtxs", "dex": self.conf.hl_dex})
            if not isinstance(data, list) or len(data) < 2:
                raise ValueError(f"unexpected metaAndAssetCtxs shape: {type(data).__name__}")
            meta, ctxs = data[0], data[1]
            universe = (meta or {}).get("universe") or []
            want = self.coin or f"{self.conf.hl_dex}:{self.conf.symbol}"
            for idx, a in enumerate(universe):
                if a.get("isDelisted"):
                    continue
                if a.get("name") in (want, self.conf.symbol) and idx < len(ctxs):
                    funding = (ctxs[idx] or {}).get("funding")
                    if funding is not None:
                        return float(funding)
            log.warning("[%s] current funding rate not found for %s (dex=%s)",
                        self.name, want, self.conf.hl_dex)
            return None
        except Exception as e:
            log.warning("[%s] funding rate fetch failed: %r", self.name, e)
            return None

    async def close(self) -> None:
        pass
