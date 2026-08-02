"""The comparison payload's honesty guards are load-bearing: other_net is
None (never 0) on the Maia side, cash is flagged not-like-for-like, and the
Athena-is-identity warning must be present while Athena's column derives
from the admin pack."""
import aum_recon
import nav_comparison
from conftest import admin_parsed


def _build(priced_view):
    parsed = admin_parsed()
    breakdown = aum_recon.maia_breakdown(priced_view)
    owner = nav_comparison._owner_by_settlement(parsed["fx_forwards"])
    maia = nav_comparison.maia_side(breakdown, owner, breakdown.get("forwards"))
    return nav_comparison.build(
        maia, nav_comparison.athena_side(parsed),
        nav_comparison.waystone_side(parsed),
        meta={"portfolio_id": "gdbf", "fund_name": "Test",
              "valuation_date": parsed["valuation_date"],
              "base_currency": "USD", "admin_file": "nav_test.xls",
              "maia": {"available": True, "file": "m.xlsx"},
              "dates_match": True, "maia_valuation_date": "2026-07-31"})


def test_row_schema_and_flags(priced_view):
    r = _build(priced_view)
    keys = {row["key"] for row in r["rows"]}
    assert {"clean_bond_mv", "accrued_income", "cash", "fx_forward_pnl_fund",
            "fx_forward_pnl_share_class", "other_net"} <= keys
    by_key = {row["key"]: row for row in r["rows"]}
    assert by_key["cash"]["not_like_for_like"] is True
    assert by_key["other_net"]["not_like_for_like"] is True
    # Maia has no receivables concept: other_net must be None, never 0.0 —
    # a zero would put a claim in Maia's mouth.
    assert by_key["other_net"]["maia"] is None


def test_identity_warning_present(priced_view):
    r = _build(priced_view)
    assert any("identity" in n.lower() for n in r["notes"])


def test_total_diffs_consistent(priced_view):
    r = _build(priced_view)
    t = r["total"]
    assert t["diff_maia_waystone"] is not None
    assert abs((t["maia"] - t["waystone"]) - t["diff_maia_waystone"]) < 0.01


def test_athena_waystone_identity_is_zero(priced_view):
    r = _build(priced_view)
    for row in r["rows"]:
        if row["athena"] is not None and row["waystone"] is not None:
            assert abs(row["diff_athena_waystone"]) < 0.01
