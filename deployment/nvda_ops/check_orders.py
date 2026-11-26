"""Read the running pair's order state without submitting or canceling orders."""
import asyncio
from decimal import Decimal
from datetime import datetime, time, timezone
import json
import logging
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import aiohttp

from entropy_arb.config import load_config
from entropy_arb.risk import SessionFilter
from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import LighterVenue

logging.disable(logging.CRITICAL)
NY = ZoneInfo("America/New_York")


def configured_session(cfg, now):
    session = cfg.session_filter
    return SessionFilter(
        enabled=session.enabled, tz_name=session.timezone,
        regular_start=session.regular_start, regular_end=session.regular_end,
        early_close_end=session.early_close_end, off_hours_mode=session.off_hours_mode,
        off_hours_cap_multiplier=session.off_hours_cap_multiplier,
        off_hours_spread_buffer_bps=session.off_hours_spread_buffer_bps,
        weekend_reduce_only=session.weekend_reduce_only, holidays=session.holidays,
        early_close_dates=session.early_close_dates,
    ).evaluate(now.timestamp())


async def entropy_snapshot(cfg, session):
    venue = HLVenue(cfg.entropy, cfg.hl_api_url, cfg.hl_ws_url, session, 6)
    await venue.load_market()
    args = {"user": venue._query_address(), "dex": cfg.entropy.hl_dex}
    orders, account = await asyncio.gather(
        venue._info({"type": "openOrders", **args}),
        venue._info({"type": "clearinghouseState", **args}),
    )
    if (not isinstance(orders, list) or not isinstance(account, dict) or
            not isinstance(account.get("assetPositions"), list)):
        raise RuntimeError("invalid Hyperliquid position or order response")
    positions = []
    for row in account["assetPositions"]:
        item = row["position"]
        size = Decimal(item["szi"])
        if not size.is_finite() or not isinstance(item["coin"], str):
            raise RuntimeError("invalid Hyperliquid position")
        if size:
            positions.append({"symbol": item["coin"], "signed_size": str(size)})
    return len(orders), positions


async def lighter_snapshot(cfg, session):
    venue = LighterVenue(cfg.hedge, session, 6)
    try:
        await venue.load_market()
        venue.init_signer()
        token, error = venue.signer.create_auth_token_with_expiry()
        if not token or error:
            raise RuntimeError("Lighter read authentication failed")
        orders, account = await asyncio.gather(
            venue.signer.order_api.account_active_orders(
                authorization=token, account_index=cfg.hedge.lighter_creds.account_index,
                market_id=venue.market_id, _request_timeout=8,
            ),
            venue._account(),
        )
        if (orders.code != 200 or not isinstance(orders.orders, list) or
                not isinstance(account, dict) or not isinstance(account.get("positions"), list)):
            raise RuntimeError("invalid Lighter position or order response")
        positions = []
        for item in account["positions"]:
            size = Decimal(item["position"])
            if not size.is_finite() or size < 0:
                raise RuntimeError("invalid Lighter position")
            if size:
                sign = int(item["sign"])
                market_id = int(item["market_id"])
                if sign not in (-1, 1):
                    raise RuntimeError("invalid Lighter position direction")
                symbol = item.get("symbol") or ("NVDA" if market_id == venue.market_id else str(market_id))
                positions.append({"symbol": symbol, "signed_size": str(sign * size)})
        return len(orders.orders), positions
    finally:
        await venue.close()


async def check():
    cfg = load_config("config.live.yaml", ".env", symbol="NVDA", hedge_venue="lighter")
    now = datetime.now(timezone.utc)
    ny = now.astimezone(NY)
    session = configured_session(cfg, now)
    state_dir = Path(cfg.persistence.state_dir)
    state_file = state_dir / "NVDA_lighter_state.json"
    state = json.loads(state_file.read_text())
    pending = state.get("pending_orders")
    reconcile = state.get("reconcile_required")
    if not cfg.persistence.enabled or not isinstance(pending, (dict, list)) or type(reconcile) is not bool:
        raise RuntimeError("unrecognized persisted order state")
    result = {
        "checked_utc": now.isoformat(),
        "checked_et": ny.isoformat(),
        "session": session.session,
        "reduce_only": session.reduce_only,
        "pending_orders": len(pending),
        "reconcile_required": reconcile,
        "state_dir": str(state_dir),
    }
    if "--cutover" in sys.argv:
        if ny.date().isoformat() != "2026-11-24" or not time(16, 0) <= ny.time() < time(20, 0):
            raise RuntimeError("outside authorized after-hours window")
        if session.session != "off_hours" or not session.reduce_only:
            raise RuntimeError("strategy is not in after-hours reduce-only mode")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as client:
        entropy, lighter = await asyncio.gather(entropy_snapshot(cfg, client), lighter_snapshot(cfg, client))
    result["entropy_open_orders"], result["entropy_positions"] = entropy
    result["lighter_open_orders"], result["lighter_positions"] = lighter
    print(json.dumps(result), flush=True)
    requires_flat = "--require-flat" in sys.argv
    has_positions = requires_flat and (result["entropy_positions"] or result["lighter_positions"])
    return 2 if (has_positions or len(pending) or reconcile or result["entropy_open_orders"]
                 or result["lighter_open_orders"]) else 0


try:
    sys.exit(asyncio.run(asyncio.wait_for(check(), 22)))
except Exception as error:
    print("Order/session check failed:", type(error).__name__, file=sys.stderr)
    sys.exit(2)
