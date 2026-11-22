"""Inspect and perform supervised, backup-first recovery on saved order state.

Default (read-only):
    python3 tools/inspect_order_state.py data/NVDA_lighter_state.json

Supervised recovery (always creates a timestamped .bak backup first):
    python3 tools/inspect_order_state.py data/NVDA_lighter_state.json \
        --resolve-zero-fill hedge:1795271661001 --confirm
    python3 tools/inspect_order_state.py data/NVDA_lighter_state.json \
        --set-position entropy=0.0 --set-position hedge=0.0 --clear-reconcile --confirm
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from entropy_arb.state import StateStore  # noqa: E402


def inspect_state(path: Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        state = json.load(stream)
    if not isinstance(state, dict):
        raise ValueError("expected a state JSON object")
    pending = state.get("pending_orders", {})
    ledger = state.get("venue_ledger", {})
    if not isinstance(pending, dict) or not isinstance(ledger, dict):
        raise ValueError("invalid order journal or position ledger")
    return {
        "symbol": state.get("symbol"),
        "hedge_venue": state.get("hedge_venue"),
        "saved_at": state.get("saved_at"),
        "reconciliation_required": bool(state.get("reconcile_required") or pending),
        "saved_positions": {key: venue["position"] for key, venue in ledger.items()},
        "pending_orders": [{
            "key": order_key,
            "venue": entry["venue_key"],
            "client_order_id": entry["client_order_id"],
            "submission": entry.get("submission", "unknown-legacy"),
            "side": "buy" if entry["is_buy"] else "sell",
            "quantity": entry["qty"],
            "known_filled": entry.get("known_filled", 0),
            "limit": entry["limit_px"],
            "purpose": entry.get("purpose"),
        } for order_key, entry in pending.items()],
    }


def apply_supervised_recovery(
    path: Path,
    *,
    resolve_zero_fill: list[str] | None = None,
    set_positions: list[str] | None = None,
    clear_reconcile: bool = False,
) -> dict:
    """Create a timestamped backup and atomically apply audited state edits."""
    with path.open(encoding="utf-8") as stream:
        state = json.load(stream)
    if not isinstance(state, dict):
        raise ValueError("expected a state JSON object")
    pending = state.setdefault("pending_orders", {})
    ledger = state.setdefault("venue_ledger", {})
    if not isinstance(pending, dict) or not isinstance(ledger, dict):
        raise ValueError("invalid order journal or position ledger")

    removed = []
    for key in resolve_zero_fill or []:
        if key not in pending:
            raise ValueError(f"pending order key not found: {key!r}")
        entry = pending[key]
        if float(entry.get("known_filled") or 0.0) != 0.0:
            raise ValueError(
                f"refusing zero-fill clear for {key!r} with known_filled={entry.get('known_filled')}"
            )
        pending.pop(key)
        removed.append(key)

    updated_positions = {}
    for spec in set_positions or []:
        if "=" not in spec:
            raise ValueError(f"expected venue=qty, got {spec!r}")
        vkey, raw_qty = spec.split("=", 1)
        vkey = vkey.strip()
        if vkey not in ledger or not isinstance(ledger[vkey], dict):
            raise ValueError(f"venue {vkey!r} not in saved venue_ledger ({sorted(ledger)})")
        qty = float(raw_qty)
        if not math.isfinite(qty):
            raise ValueError(f"position for {vkey!r} must be finite")
        ledger[vkey]["position"] = qty
        updated_positions[vkey] = qty

    if clear_reconcile or (removed and not pending):
        if pending:
            raise ValueError("cannot clear reconcile_required while pending_orders is non-empty")
        state["reconcile_required"] = False

    backup = Path(f"{path}.bak.{int(time.time())}")
    shutil.copy2(path, backup)
    store = StateStore(str(path))
    if not store.save(state):
        raise OSError(f"atomic save failed for {path}; backup preserved at {backup}")
    return {
        "backup": str(backup),
        "removed_pending": removed,
        "updated_positions": updated_positions,
        "state": inspect_state(path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_file", type=Path)
    parser.add_argument(
        "--resolve-zero-fill", action="append", default=[],
        metavar="VENUE:CLIENT_ORDER_ID",
        help="remove a manually verified unfilled pending order key after backing up state",
    )
    parser.add_argument(
        "--set-position", action="append", default=[],
        metavar="VENUE=QTY",
        help="sync a venue's saved position after verifying on-chain (e.g. entropy=0.0)",
    )
    parser.add_argument(
        "--clear-reconcile", action="store_true",
        help="clear reconcile_required (only allowed when pending_orders is empty)",
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="required to mutate the state file (a .bak backup is always written first)",
    )
    args = parser.parse_args()
    mutating = bool(args.resolve_zero_fill or args.set_position or args.clear_reconcile)
    if mutating and not args.confirm:
        parser.error("state mutation requires --confirm after manually verifying exchange state")
    try:
        if mutating:
            report = apply_supervised_recovery(
                args.state_file,
                resolve_zero_fill=args.resolve_zero_fill,
                set_positions=args.set_position,
                clear_reconcile=args.clear_reconcile,
            )
        else:
            report = inspect_state(args.state_file)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        parser.error(f"cannot process order state: {exc}")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
