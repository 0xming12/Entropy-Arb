"""Configuration: strategy from a YAML file, credentials from .env, market
selection (symbol + hedge venue) from the command line.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from dotenv import load_dotenv

from .risk import _parse_hhmm, _validate_yyyy_mm_dd

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz")
BREAKER_ACTIONS = ("reduce_only", "halt")
OFF_HOURS_MODES = ("reduce_only", "throttle")
DYNAMIC_MIDLINE_METHODS = ("ewma", "rolling_median")


@dataclass(frozen=True)
class LighterProfile:
    name: str
    api_url: str
    ws_url: str
    chain_id: int


LIGHTER_PROFILES: Dict[str, LighterProfile] = {
    "lighter": LighterProfile(
        "mainnet", "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream", 304),
    "lighter-rh": LighterProfile(
        "robinhood", "https://api.rh.lighter.xyz",
        "wss://api.rh.lighter.xyz/stream", 466324),
}


@dataclass
class LighterCreds:
    account_index: Optional[int]
    api_key_index: Optional[int]
    api_private_key: Optional[str]

    @property
    def complete(self) -> bool:
        return (self.account_index is not None and self.api_key_index is not None
                and bool(self.api_private_key))


def _is_eth_address(addr: Optional[str]) -> bool:
    if not addr or len(addr) != 42 or not addr.startswith(("0x", "0X")):
        return False
    try:
        int(addr[2:], 16)
        return True
    except ValueError:
        return False


@dataclass
class HLCreds:
    private_key: Optional[str]
    account_address: Optional[str]

    @property
    def complete(self) -> bool:
        return bool(self.private_key) and _is_eth_address(self.account_address)


@dataclass
class TelegramCreds:
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None

    @property
    def complete(self) -> bool:
        return bool(self.bot_token and self.chat_id)


@dataclass
class RiskConf:
    max_daily_loss_usd: float = 0.0
    max_consecutive_losses: int = 0
    breaker_action: str = "reduce_only"
    max_taker_fee_bps: float = 0.0
    fee_check_interval_sec: float = 60.0
    reduce_only_ignore_session_buffer: bool = True
    max_book_age_sec: float = 0.0
    max_reduce_book_age_sec: float = 15.0
    max_spread_bps: float = 0.0
    min_available_margin_usd: float = 0.0
    reconcile_drift_reduce_only: bool = True


@dataclass
class SessionFilterConf:
    enabled: bool = False
    timezone: str = "America/New_York"
    regular_start: str = "09:30"
    regular_end: str = "16:00"
    early_close_end: str = "13:00"
    off_hours_mode: str = "reduce_only"
    off_hours_cap_multiplier: float = 0.3
    off_hours_spread_buffer_bps: float = 5.0
    weekend_reduce_only: bool = True
    holidays: List[str] = field(default_factory=list)
    early_close_dates: List[str] = field(default_factory=list)


@dataclass
class DynamicMidlineConf:
    enabled: bool = False
    method: str = "ewma"
    window_minutes: float = 180.0
    min_samples: int = 60
    sample_interval_sec: float = 5.0
    std_band_multiplier: float = 0.0


@dataclass
class FundingSkewConf:
    enabled: bool = False
    expected_hold_hours: float = 4.0
    poll_interval_sec: float = 300.0
    max_skew_bps: float = 10.0
    min_net_edge_bps: float = 0.0


@dataclass
class PersistenceConf:
    enabled: bool = False
    state_dir: str = "data"
    lock_enabled: bool = False


@dataclass
class VenueConf:
    key: str
    kind: str
    label: str
    symbol: str
    fee_bps: float
    cap_usd: float
    orders_per_min: int
    hl_dex: str = ""
    hl_creds: Optional[HLCreds] = None
    lighter_profile: Optional[LighterProfile] = None
    lighter_creds: Optional[LighterCreds] = None


@dataclass
class Config:
    symbol: str
    hedge_venue: str
    entropy: VenueConf
    hedge: VenueConf
    midline_bps: float
    upper_bps: float
    lower_bps: float
    take_fraction: float
    max_order_notional: float
    min_order_notional: float
    inventory_scale_bps: float
    inventory_floor_frac: float
    premium_persist_sec: float
    cooldown_sec: float
    settle_timeout_sec: float
    leg_slippage_bps: float
    hedge_slippage_bps: float
    net_tolerance_base: float
    max_consecutive_errors: int
    rate_limit_pause_sec: float
    staleness_sec: float
    reconcile_sec: float
    venue_probe_sec: float
    http_keepalive_sec: float
    recorder_enabled: bool
    recorder_csv: str
    log_level: str
    status_interval_sec: float
    trades_csv: str
    dashboard: bool
    log_file: str
    # P0/P1 extensions (defaulted for strict backward compatibility)
    latency_buffer_bps: float = 0.0
    hedge_max_retries: int = 3
    hedge_retry_delay_sec: float = 1.0
    hedge_max_slippage_bps: float = 50.0
    check_worst_case_spread: bool = True
    reduce_only_min_worst_bps: float = -50.0
    reduce_only_exit_threshold_bps: Optional[float] = None
    pending_not_found_expire_sec: float = 120.0
    risk: RiskConf = field(default_factory=RiskConf)
    session_filter: SessionFilterConf = field(default_factory=SessionFilterConf)
    dynamic_midline: DynamicMidlineConf = field(default_factory=DynamicMidlineConf)
    funding_skew: FundingSkewConf = field(default_factory=FundingSkewConf)
    persistence: PersistenceConf = field(default_factory=PersistenceConf)
    telegram: TelegramCreds = field(default_factory=TelegramCreds)
    notify_throttle_sec: float = 60.0
    replay_only: bool = False
    hl_api_url: str = HL_API_URL
    hl_ws_url: str = HL_WS_URL

    @property
    def creds_complete(self) -> bool:
        for v in (self.entropy, self.hedge):
            if v.kind == "hl" and not (v.hl_creds and v.hl_creds.complete):
                return False
            if v.kind == "lighter" and not (v.lighter_creds
                                            and v.lighter_creds.complete):
                return False
        return True


_SCHEMA: Dict[str, Any] = {
    "replay_only": bool,
    "thresholds": {
        "midline_bps": float,
        "upper_bps": float,
        "lower_bps": float,
        "latency_buffer_bps": float,
    },
    "entropy": {
        "dex": str,
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "hedge": {
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "sizing": {
        "take_fraction": float,
        "max_order_notional_usd": float,
        "min_order_notional_usd": float,
    },
    "inventory": {
        "scale_bps": float,
        "floor_frac": float,
    },
    "execution": {
        "premium_persist_sec": float,
        "cooldown_sec": float,
        "settle_timeout_sec": float,
        "leg_slippage_bps": float,
        "hedge_slippage_bps": float,
        "hedge_max_retries": int,
        "hedge_retry_delay_sec": float,
        "hedge_max_slippage_bps": float,
        "check_worst_case_spread": bool,
        "reduce_only_min_worst_bps": float,
        "reduce_only_exit_threshold_bps": (Optional, float),
        "net_tolerance_base": float,
        "max_consecutive_errors": int,
        "rate_limit_pause_sec": float,
        "staleness_sec": float,
        "reconcile_sec": float,
        "venue_probe_sec": float,
        "http_keepalive_sec": float,
        "pending_not_found_expire_sec": float,
    },
    "risk": {
        "max_daily_loss_usd": float,
        "max_consecutive_losses": int,
        "breaker_action": str,
        "max_taker_fee_bps": float,
        "fee_check_interval_sec": float,
        "reduce_only_ignore_session_buffer": bool,
        "max_book_age_sec": float,
        "max_reduce_book_age_sec": float,
        "max_spread_bps": float,
        "min_available_margin_usd": float,
        "reconcile_drift_reduce_only": bool,
    },
    "session_filter": {
        "enabled": bool,
        "timezone": str,
        "regular_start": str,
        "regular_end": str,
        "early_close_end": str,
        "off_hours_mode": str,
        "off_hours_cap_multiplier": float,
        "off_hours_spread_buffer_bps": float,
        "weekend_reduce_only": bool,
        "holidays": (list, str),
        "early_close_dates": (list, str),
    },
    "dynamic_midline": {
        "enabled": bool,
        "method": str,
        "window_minutes": float,
        "min_samples": int,
        "sample_interval_sec": float,
        "std_band_multiplier": float,
    },
    "funding_skew": {
        "enabled": bool,
        "expected_hold_hours": float,
        "poll_interval_sec": float,
        "max_skew_bps": float,
        "min_net_edge_bps": float,
    },
    "persistence": {
        "enabled": bool,
        "state_dir": str,
        "lock_enabled": bool,
    },
    "notifications": {
        "throttle_sec": float,
    },
    "recorder": {
        "enabled": bool,
        "csv": str,
    },
    "logging": {
        "level": str,
        "status_interval_sec": float,
        "trades_csv": str,
        "dashboard": bool,
        "file": str,
    },
}


class ConfigError(ValueError):
    pass


def _validate(node: Any, schema: Dict[str, Any], path: str = "") -> None:
    if not isinstance(node, dict):
        raise ConfigError(f"'{path or '<root>'}' must be a mapping")
    for key, val in node.items():
        here = f"{path}.{key}" if path else str(key)
        if key not in schema:
            raise ConfigError(f"unknown config key '{here}' "
                              f"(valid: {', '.join(sorted(schema))})")
        want = schema[key]
        if isinstance(want, dict):
            _validate(val, want, here)
        elif isinstance(want, tuple) and want[0] is Optional:
            if val is None:
                continue
            inner = want[1]
            if inner is float:
                if not isinstance(val, (int, float)) or isinstance(val, bool):
                    raise ConfigError(f"'{here}' must be a number or null, got {val!r}")
                try:
                    finite = math.isfinite(val)
                except OverflowError:
                    finite = False
                if not finite:
                    raise ConfigError(f"'{here}' must be finite, got {val!r}")
        elif isinstance(want, tuple) and want[0] is list:
            if not isinstance(val, list):
                raise ConfigError(f"'{here}' must be a list, got {val!r}")
            elem_type = want[1]
            for idx, item in enumerate(val):
                if not isinstance(item, elem_type):
                    raise ConfigError(
                        f"'{here}[{idx}]' must be a {elem_type.__name__}, got {item!r}")
        elif want is float:
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be a number, got {val!r}")
            try:
                finite = math.isfinite(val)
            except OverflowError:
                finite = False
            if not finite:
                raise ConfigError(f"'{here}' must be finite, got {val!r}")
        elif want is int:
            if not isinstance(val, int) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be an integer, got {val!r}")
        elif want is bool:
            if not isinstance(val, bool):
                raise ConfigError(f"'{here}' must be true/false, got {val!r}")
        elif want is str:
            if not isinstance(val, str):
                raise ConfigError(f"'{here}' must be a string, got {val!r}")


def _get(d: dict, section: str, key: str, default):
    return (d.get(section) or {}).get(key, default)


def _env_s(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v not in (None, "") else None


def _env_i(name: str) -> Optional[int]:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else None


def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str) -> Config:
    load_dotenv(env_file)
    try:
        with open(config_file) as fh:
            raw = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigError(
            f"config file '{config_file}' not found — copy config.example.yaml "
            f"to config.yaml and edit it / 未找到配置文件，请先复制 "
            f"config.example.yaml 为 config.yaml 并修改")
    _validate(raw, _SCHEMA)

    symbol = (symbol or "").strip()
    if not symbol:
        raise ConfigError("--symbol is required, e.g. --symbol SNDK / "
                          "必须用 --symbol 指定交易品种")
    if hedge_venue not in HEDGE_VENUES:
        raise ConfigError(
            f"--hedge must be one of {list(HEDGE_VENUES)}, got "
            f"{hedge_venue!r} / --hedge 必须是 {list(HEDGE_VENUES)} 之一")

    thr = raw.get("thresholds") or {}
    for k in ("midline_bps", "upper_bps", "lower_bps"):
        if k not in thr:
            raise ConfigError(f"'thresholds.{k}' is required — derive it from "
                              f"recorded minute data / 必须填写，请用采集的分钟"
                              f"数据计算后填入")
    upper, lower = float(thr["upper_bps"]), float(thr["lower_bps"])
    if upper <= 0 or lower <= 0:
        raise ConfigError("thresholds.upper_bps and lower_bps must be > 0 "
                          "(the round trip nets upper+lower bps after fees)")
    latency_buffer_bps = float(thr.get("latency_buffer_bps", 0.0))
    if latency_buffer_bps < 0:
        raise ConfigError("thresholds.latency_buffer_bps must be >= 0")

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail / 必须在 (0, 1] 之间")

    entropy_dex = _get(raw, "entropy", "dex", "io")
    if hedge_venue == "tradexyz" and entropy_dex == "xyz":
        raise ConfigError("entropy.dex 'xyz' with hedge_venue 'tradexyz' is "
                          "the same market on both legs / 两条腿是同一个市场")

    hedge_max_retries = int(_get(raw, "execution", "hedge_max_retries", 3))
    hedge_retry_delay_sec = float(_get(raw, "execution", "hedge_retry_delay_sec", 1.0))
    hedge_max_slippage_bps = float(_get(raw, "execution", "hedge_max_slippage_bps", 50.0))
    check_worst_case_spread = bool(_get(raw, "execution", "check_worst_case_spread", True))
    reduce_only_min_worst_bps = float(_get(raw, "execution", "reduce_only_min_worst_bps", -50.0))
    raw_ro_exit = _get(raw, "execution", "reduce_only_exit_threshold_bps", None)
    reduce_only_exit_threshold_bps = None if raw_ro_exit is None else float(raw_ro_exit)
    settle_timeout_sec = float(_get(raw, "execution", "settle_timeout_sec", 5.0))
    leg_slippage_bps = float(_get(raw, "execution", "leg_slippage_bps", 50.0))
    hedge_slippage_bps = float(_get(raw, "execution", "hedge_slippage_bps", 20.0))
    if settle_timeout_sec <= 0:
        raise ConfigError("execution.settle_timeout_sec must be > 0")
    for name, value in (("leg_slippage_bps", leg_slippage_bps),
                        ("hedge_slippage_bps", hedge_slippage_bps),
                        ("hedge_max_slippage_bps", hedge_max_slippage_bps)):
        if not 0 <= value < 10_000:
            raise ConfigError(f"execution.{name} must be in [0, 10000)")
    if hedge_max_retries < 1:
        raise ConfigError("execution.hedge_max_retries must be >= 1")
    if hedge_retry_delay_sec < 0:
        raise ConfigError("execution.hedge_retry_delay_sec must be >= 0")

    risk_conf = RiskConf(
        max_daily_loss_usd=float(_get(raw, "risk", "max_daily_loss_usd", 0.0)),
        max_consecutive_losses=int(_get(raw, "risk", "max_consecutive_losses", 0)),
        breaker_action=str(_get(raw, "risk", "breaker_action", "reduce_only")),
        max_taker_fee_bps=float(_get(raw, "risk", "max_taker_fee_bps", 0.0)),
        fee_check_interval_sec=float(_get(raw, "risk", "fee_check_interval_sec", 60.0)),
        reduce_only_ignore_session_buffer=bool(
            _get(raw, "risk", "reduce_only_ignore_session_buffer", True)),
        max_book_age_sec=float(_get(raw, "risk", "max_book_age_sec", 0.0)),
        max_reduce_book_age_sec=float(_get(raw, "risk", "max_reduce_book_age_sec", 15.0)),
        max_spread_bps=float(_get(raw, "risk", "max_spread_bps", 0.0)),
        min_available_margin_usd=float(
            _get(raw, "risk", "min_available_margin_usd", 0.0)),
        reconcile_drift_reduce_only=bool(
            _get(raw, "risk", "reconcile_drift_reduce_only", True)),
    )
    if risk_conf.max_daily_loss_usd < 0:
        raise ConfigError("risk.max_daily_loss_usd must be >= 0")
    if risk_conf.max_consecutive_losses < 0:
        raise ConfigError("risk.max_consecutive_losses must be >= 0")
    if risk_conf.breaker_action not in BREAKER_ACTIONS:
        raise ConfigError(
            f"risk.breaker_action must be one of {list(BREAKER_ACTIONS)}, "
            f"got {risk_conf.breaker_action!r}")
    if risk_conf.max_taker_fee_bps < 0:
        raise ConfigError("risk.max_taker_fee_bps must be >= 0")
    if risk_conf.fee_check_interval_sec <= 0:
        raise ConfigError("risk.fee_check_interval_sec must be > 0")
    if risk_conf.max_book_age_sec < 0:
        raise ConfigError("risk.max_book_age_sec must be >= 0")
    if risk_conf.max_reduce_book_age_sec <= 0:
        raise ConfigError("risk.max_reduce_book_age_sec must be > 0")
    if risk_conf.max_spread_bps < 0:
        raise ConfigError("risk.max_spread_bps must be >= 0")
    if risk_conf.min_available_margin_usd < 0:
        raise ConfigError("risk.min_available_margin_usd must be >= 0")

    raw_holidays = list(_get(raw, "session_filter", "holidays", []))
    raw_early = list(_get(raw, "session_filter", "early_close_dates", []))
    try:
        holidays = [_validate_yyyy_mm_dd(d) for d in raw_holidays]
        early_close_dates = [_validate_yyyy_mm_dd(d) for d in raw_early]
    except ValueError as e:
        raise ConfigError(f"session_filter date must be YYYY-MM-DD: {e}") from e

    sf_conf = SessionFilterConf(
        enabled=bool(_get(raw, "session_filter", "enabled", False)),
        timezone=str(_get(raw, "session_filter", "timezone", "America/New_York")),
        regular_start=str(_get(raw, "session_filter", "regular_start", "09:30")),
        regular_end=str(_get(raw, "session_filter", "regular_end", "16:00")),
        early_close_end=str(_get(raw, "session_filter", "early_close_end", "13:00")),
        off_hours_mode=str(_get(raw, "session_filter", "off_hours_mode", "reduce_only")),
        off_hours_cap_multiplier=float(
            _get(raw, "session_filter", "off_hours_cap_multiplier", 0.3)),
        off_hours_spread_buffer_bps=float(
            _get(raw, "session_filter", "off_hours_spread_buffer_bps", 5.0)),
        weekend_reduce_only=bool(
            _get(raw, "session_filter", "weekend_reduce_only", True)),
        holidays=holidays,
        early_close_dates=early_close_dates,
    )
    try:
        ZoneInfo(sf_conf.timezone)
    except ZoneInfoNotFoundError as e:
        raise ConfigError(
            f"session_filter.timezone invalid: {sf_conf.timezone!r}") from e
    try:
        t_start = _parse_hhmm(sf_conf.regular_start)
        t_end = _parse_hhmm(sf_conf.regular_end)
        t_early = _parse_hhmm(sf_conf.early_close_end)
    except ValueError as e:
        raise ConfigError(f"session_filter time invalid: {e}") from e
    if t_start >= t_end:
        raise ConfigError(
            "session_filter.regular_start must be earlier than regular_end")
    if not (t_start < t_early <= t_end):
        raise ConfigError(
            "session_filter.early_close_end must be between regular_start and regular_end")
    if sf_conf.off_hours_mode not in OFF_HOURS_MODES:
        raise ConfigError(
            f"session_filter.off_hours_mode must be one of {list(OFF_HOURS_MODES)}, "
            f"got {sf_conf.off_hours_mode!r}")
    if not 0.0 < sf_conf.off_hours_cap_multiplier <= 1.0:
        raise ConfigError(
            "session_filter.off_hours_cap_multiplier must be in (0, 1]")
    if sf_conf.off_hours_spread_buffer_bps < 0:
        raise ConfigError(
            "session_filter.off_hours_spread_buffer_bps must be >= 0")

    dm_conf = DynamicMidlineConf(
        enabled=bool(_get(raw, "dynamic_midline", "enabled", False)),
        method=str(_get(raw, "dynamic_midline", "method", "ewma")),
        window_minutes=float(_get(raw, "dynamic_midline", "window_minutes", 180.0)),
        min_samples=int(_get(raw, "dynamic_midline", "min_samples", 60)),
        sample_interval_sec=float(
            _get(raw, "dynamic_midline", "sample_interval_sec", 5.0)),
        std_band_multiplier=float(
            _get(raw, "dynamic_midline", "std_band_multiplier", 0.0)),
    )
    if dm_conf.method not in DYNAMIC_MIDLINE_METHODS:
        raise ConfigError(
            f"dynamic_midline.method must be one of {list(DYNAMIC_MIDLINE_METHODS)}, "
            f"got {dm_conf.method!r}")
    if dm_conf.window_minutes <= 0:
        raise ConfigError("dynamic_midline.window_minutes must be > 0")
    if dm_conf.min_samples < 1:
        raise ConfigError("dynamic_midline.min_samples must be >= 1")
    if dm_conf.sample_interval_sec < 0:
        raise ConfigError("dynamic_midline.sample_interval_sec must be >= 0")
    if dm_conf.std_band_multiplier < 0:
        raise ConfigError("dynamic_midline.std_band_multiplier must be >= 0")

    fs_conf = FundingSkewConf(
        enabled=bool(_get(raw, "funding_skew", "enabled", False)),
        expected_hold_hours=float(
            _get(raw, "funding_skew", "expected_hold_hours", 4.0)),
        poll_interval_sec=float(
            _get(raw, "funding_skew", "poll_interval_sec", 300.0)),
        max_skew_bps=float(_get(raw, "funding_skew", "max_skew_bps", 10.0)),
        min_net_edge_bps=float(
            _get(raw, "funding_skew", "min_net_edge_bps", 0.0)),
    )
    if fs_conf.expected_hold_hours < 0:
        raise ConfigError("funding_skew.expected_hold_hours must be >= 0")
    if fs_conf.poll_interval_sec <= 0:
        raise ConfigError("funding_skew.poll_interval_sec must be > 0")
    if fs_conf.max_skew_bps < 0:
        raise ConfigError("funding_skew.max_skew_bps must be >= 0")
    if fs_conf.min_net_edge_bps < 0:
        raise ConfigError("funding_skew.min_net_edge_bps must be >= 0")

    pers_conf = PersistenceConf(
        enabled=bool(_get(raw, "persistence", "enabled", False)),
        state_dir=str(_get(raw, "persistence", "state_dir", "data")),
        lock_enabled=bool(_get(raw, "persistence", "lock_enabled", False)),
    )

    notify_throttle_sec = float(_get(raw, "notifications", "throttle_sec", 60.0))
    if notify_throttle_sec < 0:
        raise ConfigError("notifications.throttle_sec must be >= 0")
    telegram_creds = TelegramCreds(
        bot_token=_env_s("TELEGRAM_BOT_TOKEN"),
        chat_id=_env_s("TELEGRAM_CHAT_ID"),
    )

    entropy_hl_creds = HLCreds(_env_s("HL_PRIVATE_KEY"),
                               _env_s("HL_ACCOUNT_ADDRESS"))
    entropy = VenueConf(
        key="entropy", kind="hl", label="ENTROPY",
        symbol=symbol,
        fee_bps=float(_get(raw, "entropy", "taker_fee_bps", 0.0)),
        cap_usd=float(_get(raw, "entropy", "max_position_usd", 1000.0)),
        orders_per_min=int(_get(raw, "entropy", "max_orders_per_min", 120)),
        hl_dex=entropy_dex,
        hl_creds=entropy_hl_creds,
    )

    if hedge_venue == "tradexyz":
        hedge = VenueConf(
            key="hedge", kind="hl", label="XYZ",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 1.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 120)),
            hl_dex="xyz",
            hl_creds=HLCreds(
                _env_s("HL_PRIVATE_KEY_XYZ") or _env_s("HL_PRIVATE_KEY"),
                _env_s("HL_ACCOUNT_ADDRESS_XYZ") or _env_s("HL_ACCOUNT_ADDRESS")),
        )
    else:
        hedge = VenueConf(
            key="hedge", kind="lighter",
            label="LIGHTER" if hedge_venue == "lighter" else "RH",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 0.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 30)),
            lighter_profile=LIGHTER_PROFILES[hedge_venue],
            lighter_creds=LighterCreds(_env_i("LIGHTER_ACCOUNT_INDEX"),
                                       _env_i("LIGHTER_API_KEY_INDEX"),
                                       _env_s("LIGHTER_API_PRIVATE_KEY")),
        )

    max_order_notional = float(_get(raw, "sizing", "max_order_notional_usd", 500.0))
    min_order_notional = float(_get(raw, "sizing", "min_order_notional_usd", 10.0))
    if max_order_notional <= 0 or not 0 <= min_order_notional <= max_order_notional:
        raise ConfigError("sizing notional must satisfy 0 <= min <= max, with max > 0")
    if entropy.cap_usd <= 0 or hedge.cap_usd <= 0:
        raise ConfigError("entropy and hedge position caps must both be > 0")
    if entropy.fee_bps < 0 or hedge.fee_bps < 0:
        raise ConfigError("entropy.taker_fee_bps and hedge.taker_fee_bps must be >= 0")
    if entropy.orders_per_min < 1 or hedge.orders_per_min < 1:
        raise ConfigError("entropy.max_orders_per_min and hedge.max_orders_per_min must be >= 1")

    inventory_scale_bps = float(_get(raw, "inventory", "scale_bps", 10.0))
    inventory_floor_frac = float(_get(raw, "inventory", "floor_frac", 0.5))
    if inventory_scale_bps < 0:
        raise ConfigError("inventory.scale_bps must be >= 0")
    if not 0.0 <= inventory_floor_frac < 1.0:
        raise ConfigError("inventory.floor_frac must be in [0, 1)")

    premium_persist_sec = float(_get(raw, "execution", "premium_persist_sec", 0.3))
    cooldown_sec = float(_get(raw, "execution", "cooldown_sec", 0.0))
    net_tolerance_base = float(_get(raw, "execution", "net_tolerance_base", 0.001))
    max_consecutive_errors = int(_get(raw, "execution", "max_consecutive_errors", 3))
    rate_limit_pause_sec = float(_get(raw, "execution", "rate_limit_pause_sec", 10.0))
    staleness_sec = float(_get(raw, "execution", "staleness_sec", 10.0))
    reconcile_sec = float(_get(raw, "execution", "reconcile_sec", 15.0))
    venue_probe_sec = float(_get(raw, "execution", "venue_probe_sec", 30.0))
    http_keepalive_sec = float(_get(raw, "execution", "http_keepalive_sec", 10.0))
    pending_not_found_expire_sec = float(
        _get(raw, "execution", "pending_not_found_expire_sec", 120.0))
    status_interval_sec = float(_get(raw, "logging", "status_interval_sec", 30.0))

    if premium_persist_sec < 0:
        raise ConfigError("execution.premium_persist_sec must be >= 0")
    if cooldown_sec < 0:
        raise ConfigError("execution.cooldown_sec must be >= 0")
    if net_tolerance_base < 0:
        raise ConfigError("execution.net_tolerance_base must be >= 0")
    if max_consecutive_errors < 1:
        raise ConfigError("execution.max_consecutive_errors must be >= 1")
    if rate_limit_pause_sec < 0:
        raise ConfigError("execution.rate_limit_pause_sec must be >= 0")
    if staleness_sec <= 0:
        raise ConfigError("execution.staleness_sec must be > 0")
    if reconcile_sec <= 0:
        raise ConfigError("execution.reconcile_sec must be > 0")
    if venue_probe_sec <= 0:
        raise ConfigError("execution.venue_probe_sec must be > 0")
    if http_keepalive_sec < 0:
        raise ConfigError("execution.http_keepalive_sec must be >= 0")
    if pending_not_found_expire_sec < 0:
        raise ConfigError("execution.pending_not_found_expire_sec must be >= 0")
    if status_interval_sec <= 0:
        raise ConfigError("logging.status_interval_sec must be > 0")

    return Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        entropy=entropy,
        hedge=hedge,
        midline_bps=float(thr["midline_bps"]),
        upper_bps=upper,
        lower_bps=lower,
        latency_buffer_bps=latency_buffer_bps,
        take_fraction=take_fraction,
        max_order_notional=max_order_notional,
        min_order_notional=min_order_notional,
        inventory_scale_bps=inventory_scale_bps,
        inventory_floor_frac=inventory_floor_frac,
        premium_persist_sec=premium_persist_sec,
        cooldown_sec=cooldown_sec,
        settle_timeout_sec=settle_timeout_sec,
        leg_slippage_bps=leg_slippage_bps,
        hedge_slippage_bps=hedge_slippage_bps,
        hedge_max_retries=hedge_max_retries,
        hedge_retry_delay_sec=hedge_retry_delay_sec,
        hedge_max_slippage_bps=hedge_max_slippage_bps,
        check_worst_case_spread=check_worst_case_spread,
        reduce_only_min_worst_bps=reduce_only_min_worst_bps,
        reduce_only_exit_threshold_bps=reduce_only_exit_threshold_bps,
        pending_not_found_expire_sec=pending_not_found_expire_sec,
        net_tolerance_base=net_tolerance_base,
        max_consecutive_errors=max_consecutive_errors,
        rate_limit_pause_sec=rate_limit_pause_sec,
        staleness_sec=staleness_sec,
        reconcile_sec=reconcile_sec,
        venue_probe_sec=venue_probe_sec,
        http_keepalive_sec=http_keepalive_sec,
        recorder_enabled=bool(_get(raw, "recorder", "enabled", True)),
        recorder_csv=_get(raw, "recorder", "csv", "logs/minutes.csv"),
        log_level=str(_get(raw, "logging", "level", "INFO")).upper(),
        status_interval_sec=status_interval_sec,
        trades_csv=_get(raw, "logging", "trades_csv", "logs/trades.csv"),
        dashboard=bool(_get(raw, "logging", "dashboard", True)),
        log_file=_get(raw, "logging", "file", "logs/engine.log"),
        risk=risk_conf,
        session_filter=sf_conf,
        dynamic_midline=dm_conf,
        funding_skew=fs_conf,
        persistence=pers_conf,
        telegram=telegram_creds,
        notify_throttle_sec=notify_throttle_sec,
        replay_only=raw.get("replay_only", False),
    )
