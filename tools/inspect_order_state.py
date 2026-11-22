"""Display saved order IDs and positions to support manual exchange reconciliation."""
import argparse
import json
from pathlib import Path


def inspect_state(path):
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
            "venue": entry["venue_key"], "client_order_id": entry["client_order_id"],
            "submission": entry.get("submission", "unknown-legacy"),
            "side": "buy" if entry["is_buy"] else "sell", "quantity": entry["qty"],
            "known_filled": entry.get("known_filled", 0), "limit": entry["limit_px"],
            "purpose": entry.get("purpose"),
        } for entry in pending.values()],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_file", type=Path)
    args = parser.parse_args()
    try:
        report = inspect_state(args.state_file)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        parser.error(f"cannot read order state: {exc}")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
