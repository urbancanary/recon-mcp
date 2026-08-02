"""The par/price/FX decomposition must be exact by construction, and a
stale Ticker→ISIN bridge must suppress coverage claims, never invent
missing positions."""
import pytest

import bond_recon
import recon_attribution
from conftest import admin_parsed, write_priced_view, write_reference_view


def test_decomposition_exact(tmp_path, priced_view, reference_view):
    parsed = admin_parsed()
    a = recon_attribution.attribute(parsed, priced_view, reference_view)
    assert a["dates_match"] is True
    assert a["decomposition_exact"] is True
    assert len(a["rows"]) == 2, (a["unmatched"], a["missing_fx"])
    t = a["totals"]
    for r in a["rows"]:
        recon = r["effect_par"] + r["effect_price"] + r["effect_fx"]
        assert recon == pytest.approx(r["mv_diff"], abs=0.02)
    assert t["par"] + t["price"] + t["fx"] == pytest.approx(
        t["maia_mv"] - t["admin_mv"], abs=0.05)


def test_price_divisor_snaps_for_per_unit_securities(tmp_path):
    """A CNY-CCD-style per-unit security (price ≈ market value per unit)
    must get divisor 1, not 100 — the qty×px/100 convention overstates it
    100×."""
    parsed = admin_parsed()
    # Make the first bond per-unit quoted: price == unit value, MV = qty*px*fx
    h = parsed["holdings"][0]
    h["price"] = 0.99
    h["market_value"] = h["par_amount"] * 0.99
    a = recon_attribution.attribute(
        parsed,
        str(write_priced_view(tmp_path / "p.xlsx")),
        str(write_reference_view(tmp_path / "r.xlsx")))
    row = next(r for r in a["rows"] if r["isin"] == "US0000000001")
    assert row["price_divisor"] == 1.0


def test_stale_bridge_suppresses_coverage(tmp_path):
    """Bridge dated differently from the priced view → coverage_reliable
    False and warnings say so. The 31-Jul incident: a stale bridge reported
    four live bonds as 'missing from Maia' (-2.88m phantom)."""
    priced = write_priced_view(tmp_path / "p.xlsx",
                               valuation="31/07/2026 14:00:00")
    ref = write_reference_view(tmp_path / "r.xlsx",
                               valuation="24/07/2026 10:00:00")
    out = bond_recon.compare(admin_parsed(), str(priced), str(ref))
    assert out["bridge_stale"] is True
    assert out["coverage_reliable"] is False
    assert any("bridge" in w.lower() for w in out["warnings"])


def test_fresh_bridge_joins_all(priced_view, reference_view):
    out = bond_recon.compare(admin_parsed(), priced_view, reference_view)
    assert out["bridge_stale"] is False
    assert out["matched"] == 2
    assert out["admin_only"] == [] and out["maia_only"] == []
