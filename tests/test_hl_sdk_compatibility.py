"""Signing compatibility checks use synthetic SDKs and never load a real key."""
import sys
from types import ModuleType, SimpleNamespace

import pytest

from entropy_arb import venue_hl
from entropy_arb.config import HLCreds
from test_live_risk_regression import make_cfg


def make_venue(monkeypatch, sign_l1_action):
    sdk = ModuleType("hyperliquid")
    sdk.__path__ = []
    utils = ModuleType("hyperliquid.utils")
    utils.signing = SimpleNamespace(sign_l1_action=sign_l1_action)
    sdk.utils = utils
    monkeypatch.setitem(sys.modules, sdk.__name__, sdk)
    monkeypatch.setitem(sys.modules, utils.__name__, utils)
    cfg = make_cfg(hedge="lighter")
    cfg.entropy.hl_creds = HLCreds("offline-test-key", "0x" + "a" * 40)
    return venue_hl.HLVenue(cfg.entropy, cfg.hl_api_url, cfg.hl_ws_url, None, 5.0)


def test_legacy_sdk_fails_before_loading_wallet(monkeypatch):
    def legacy(wallet, action, active_pool, nonce, is_mainnet):
        pytest.fail("compatibility check should not invoke signer")

    def load_wallet(*args):
        pytest.fail("legacy signing must be rejected before wallet setup")

    monkeypatch.setattr(venue_hl, "HLAccount", load_wallet)
    venue = make_venue(monkeypatch, legacy)
    with pytest.raises(RuntimeError, match=r"hyperliquid-python-sdk>=0\.12\.0"):
        venue.init_signer()
    assert venue.account is None and venue._signing is None


def test_current_signing_contract_passes_startup_check(monkeypatch):
    def current(wallet, action, active_pool, nonce, expires_after, is_mainnet):
        pytest.fail("compatibility check should not invoke signer")

    account = SimpleNamespace(describe=lambda: "offline test account")
    monkeypatch.setattr(venue_hl, "HLAccount", lambda *_: account)
    venue = make_venue(monkeypatch, current)
    venue.init_signer()
    assert venue.account is account and venue._signing.sign_l1_action is current
