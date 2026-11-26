"""Lightweight JSON state persistence and single-instance file lock (stdlib only).

Used in live mode to:
1. Persist CircuitBreaker daily PnL / consecutive loss counters and DynamicMidline
   rolling samples across process restarts (atomic write via tmp + os.replace).
2. Prevent accidentally launching two live bots for the same (symbol, hedge_venue)
   pair via POSIX fcntl.flock.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore

log = logging.getLogger("state")


def global_lock_dir() -> str:
    """Fixed absolute directory for cross-config/cross-CWD account+market locks."""
    override = os.getenv("ENTROPY_ARB_LOCK_DIR")
    if override and override.strip():
        return os.path.abspath(os.path.expanduser(override.strip()))
    return os.path.join(os.path.expanduser("~"), ".entropy-arb", "locks")


class StateStore:
    """Atomic JSON file store for engine risk & strategy state."""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self, *, strict: bool = False) -> Optional[Dict[str, Any]]:
        if not self.path:
            return None
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                raise ValueError("state file root is not a JSON object")
            return data
        except FileNotFoundError:
            return None
        except Exception as e:
            if strict:
                raise RuntimeError(f"cannot recover order state from {self.path}") from e
            log.warning("failed to load state file %s (%r) — starting fresh", self.path, e)
            return None

    def save(self, payload: Dict[str, Any]) -> bool:
        if not self.path:
            return False
        try:
            d = os.path.dirname(self.path)
            if d:
                os.makedirs(d, exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
            parent = os.open(d or ".", os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
            return True
        except Exception as e:
            log.warning("failed to save state file %s: %r", self.path, e)
            return False


class InstanceLock:
    """Non-blocking exclusive file lock (fcntl.flock) to prevent duplicate live bots."""

    def __init__(self, lock_path: str, *, label: str = "") -> None:
        self.lock_path = lock_path
        self.label = label
        self._fh = None

    def acquire(self) -> None:
        if not self.lock_path:
            return
        d = os.path.dirname(self.lock_path)
        if d:
            os.makedirs(d, exist_ok=True)
        fh = open(self.lock_path, "a+", encoding="utf-8")
        if fcntl is not None:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as e:
                fh.seek(0)
                holder = fh.read().strip()
                fh.close()
                scope = f" ({self.label})" if self.label else ""
                raise RuntimeError(
                    f"another live instance is already running for this market/account/signer{scope} "
                    f"(lock: {self.lock_path}, holder={holder or 'unknown'}) / "
                    f"检测到同品种、同账户或同签名密钥的实盘实例已在运行，禁止共享 nonce 或重复启动"
                ) from e
        fh.seek(0)
        fh.truncate()
        info = f"{os.getpid()} {self.label}".strip()
        fh.write(f"{info}\n")
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None
