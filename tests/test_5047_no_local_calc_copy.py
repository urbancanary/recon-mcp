"""Backlog 5047 — recon keeps no local copy of the calc static.

Handed over by bond_data_mcp: coupon / maturity_date / day_count / frequency are
read from v_bond_static at calc time, and what Bloomberg told us is OFFERED to
POST /tools/static_corrections as evidence rather than written to the mirror.
"""
import asyncio

import pytest

import recon_db
from recon_db import (IDENTITY_CALC_FIELDS, REFERENCE_CALC_FIELDS,
                      overlay_canonical_static)


def _run(coro):
    return asyncio.run(coro)


# ── the mirror holds no calc value the view did not give it ─────────────────

def test_view_value_replaces_the_base_table_value():
    row = {"isin": "X1", "coupon": 6.63, "maturity_date": "2036-05-16"}
    view = {"X1": {"isin": "X1", "coupon": 6.625, "maturity_date": "2036-05-16"}}
    out = overlay_canonical_static(row, view, IDENTITY_CALC_FIELDS)
    assert out["coupon"] == 6.625
    assert row["coupon"] == 6.63          # input not mutated


def test_a_calc_field_the_view_does_not_carry_is_dropped_not_mirrored():
    """The defect: a base-table calc value left in the mirror is still priced
    on, because the recon views COALESCE against the local row."""
    row = {"isin": "X1", "coupon": 6.63, "day_count": "30/360", "frequency": 1}
    view = {"X1": {"isin": "X1", "coupon": None, "day_count": "ACT/ACT",
                   "frequency": None}}
    out = overlay_canonical_static(row, view, REFERENCE_CALC_FIELDS)
    assert out["day_count"] == "ACT/ACT"
    assert "coupon" not in out and "frequency" not in out


def test_a_bond_the_view_lacks_keeps_its_row():
    row = {"isin": "X2", "coupon": 5.0}
    assert overlay_canonical_static(row, {}, IDENTITY_CALC_FIELDS) == row


# ── BBG findings are evidence, never a local write ──────────────────────────

@pytest.fixture
def captured(monkeypatch):
    """Capture the findings posted and the local PATCHes attempted."""
    sent, patched = {}, []

    async def fake_offer(findings, timeout=20):
        sent["findings"] = findings
        return {"status": "ok", "source": "v_bond_static",
                "corrections": sent.get("reply", []), "drift": []}

    async def fake_patch(table, isin, payload):
        patched.append((table, isin, payload))
        return 1

    monkeypatch.setattr(recon_db, "offer_static_corrections", fake_offer)
    monkeypatch.setattr(recon_db, "_patch", fake_patch)
    return sent, patched


def test_bbg_values_go_out_as_findings(captured):
    sent, patched = captured
    _run(recon_db.enrich_bond_data_from_bbg(
        {"X1": "2036-05-16"}, {"X1": 6.625},
        cpn_freq_bonds={"X1": "2"}, day_count_bonds={"X1": "ACT/ACT"},
    ))
    got = {(f["isin"], f["field"]): f["value"] for f in sent["findings"]}
    assert got[("X1", "maturity_date")] == "2036-05-16"
    assert got[("X1", "coupon")] == 6.625
    assert got[("X1", "frequency")] == "Semiannual"
    assert got[("X1", "day_count")] == "ACT/ACT"
    assert all(f["source"] and f["observed_at"] for f in sent["findings"])


def test_nothing_is_patched_when_the_canonical_path_offers_nothing(captured):
    """The whole defect: BBG used to overwrite maturity/frequency/day_count on
    every unlocked local row. Now a disagreement the canonical path does not
    offer back changes no local row at all."""
    sent, patched = captured
    sent["reply"] = []
    _run(recon_db.enrich_bond_data_from_bbg({"X1": "2036-05-16"}, {}))
    assert patched == []


def test_only_the_canonical_paths_own_corrections_are_applied(captured):
    sent, patched = captured
    sent["reply"] = [
        {"isin": "X1", "field": "day_count", "observed_value": "ACT/ACT"},
        {"isin": "X1", "field": "not_a_calc_field", "observed_value": "junk"},
    ]
    _run(recon_db.enrich_bond_data_from_bbg({"X1": "2036-05-16"}, {}))
    assert patched == [("local_bond_reference", "X1", {"day_count": "ACT/ACT"})]


def test_a_blank_is_not_offered_as_evidence(captured):
    sent, _ = captured
    _run(recon_db.enrich_bond_data_from_bbg({"X1": "2036-05-16"}, {}))
    assert {f["field"] for f in sent["findings"]} == {"maturity_date"}


# ── an unreachable canonical path means no corrections, not a local fallback ─

def test_unavailable_path_yields_no_corrections(monkeypatch):
    class Boom:
        async def post(self, *a, **k):
            raise RuntimeError("no route to host")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(recon_db, "_bond_data_service_url", lambda: "https://bd")
    monkeypatch.setattr(recon_db, "_bond_data_headers", lambda: {})
    monkeypatch.setattr(recon_db.httpx, "AsyncClient", lambda **k: Boom())
    out = _run(recon_db.offer_static_corrections(
        [{"isin": "X1", "field": "coupon", "value": 6.6}]))
    assert out["status"] == "unavailable"
    assert out["corrections"] == []


def test_the_endpoint_saying_unavailable_is_honoured(monkeypatch):
    class Resp:
        status_code = 200

        def json(self):
            return {"status": "unavailable",
                    "error": "v_bond_static could not be read"}

    class Ok:
        async def post(self, *a, **k):
            return Resp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(recon_db, "_bond_data_service_url", lambda: "https://bd")
    monkeypatch.setattr(recon_db, "_bond_data_headers", lambda: {})
    monkeypatch.setattr(recon_db.httpx, "AsyncClient", lambda **k: Ok())
    out = _run(recon_db.offer_static_corrections(
        [{"isin": "X1", "field": "coupon", "value": 6.6}]))
    assert out["status"] == "unavailable" and out["corrections"] == []


# ── the service URL is resolved, never baked in ─────────────────────────────

def test_service_url_is_not_hardcoded():
    import inspect
    src = inspect.getsource(recon_db._bond_data_service_url)
    assert "get_service_url" in src
    assert "http" not in src.replace("BOND_DATA_MCP_URL", "")
