"""aum_passes: the pass-2 (admin-priced) table — identity, par variation,
unmatched buckets, per-unit normalisation, null-never-zero."""
import pytest

import aum_passes


def _parsed():
    """Two normal bonds (one EUR at 1.10) + one per-unit security whose
    embedded factor (mv/(par×px/100)) is far above any FX rate."""
    holdings = [
        {"isin": "US0000000001", "currency": "USD", "par_amount": 1_000_000,
         "price": 99.0, "market_value": 990_000.0, "accrued_income": 5_000.0},
        {"isin": "DE0000000002", "currency": "EUR", "par_amount": 500_000,
         "price": 90.0, "market_value": 495_000.0, "accrued_income": 2_200.0},
        # per-unit: 1,700 units, price 100 → naive par×px/100 = 1,700 but
        # mv = 170,000 → factor 100
        {"isin": "GB00BBQ33664", "currency": "GBP", "par_amount": 1_700,
         "price": 100.0, "market_value": 170_000.0, "accrued_income": 0.0},
    ]
    return {
        "base_currency": "USD",
        "fx_rates": {"rates": {"EUR": 1 / 1.10, "GBP": 1 / 1.25}},
        "holdings": holdings,
        "summary": {"aum_breakdown": {
            "clean_bond_mv": 990_000.0 + 495_000.0 + 170_000.0,
            # balance sheet accrued ABOVE the per-bond sum (declared-but-
            # unpaid income) — the identity must target the per-bond sum.
            "accrued_income": 9_000.0,
        }},
    }


def _sides():
    way = {"cash": 100_000.0, "fx_forward_pnl_fund": 500.0,
           "fx_forward_pnl_share_class": -300.0, "other_net": 42.0}
    mai = {"cash": 101_000.0, "fx_forward_pnl_fund": 480.0,
           "fx_forward_pnl_share_class": None, "other_net": None}
    return {"maia": mai, "athena": dict(way), "waystone": way}


def test_waystone_identity_to_the_cent():
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=None, athena_par=None)
    ic = p["identity_check"]
    assert ic["holds"] is True
    assert ic["clean_diff"] == 0.0
    assert ic["recomputed_accrued"] == ic["per_bond_accrued_sum"] == 7_200.0
    assert ic["balance_sheet_accrued"] == 9_000.0
    assert ic["declared_unpaid_income_gap"] == 1_800.0


def test_derived_athena_equals_waystone():
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=None, athena_par=None)
    rows = {r["key"]: r for r in p["rows"]}
    assert rows["clean_bond_mv"]["athena"] == rows["clean_bond_mv"]["waystone"]
    assert rows["accrued_income"]["diff_athena_waystone"] == 0.0


def test_maia_par_varies_at_constant_prices():
    # Maia holds half the US bond — the pass-2 gap must be exactly the
    # missing par at Waystone's price/FX, plus the whole per-unit and EUR
    # positions it lacks.
    maia = {"US0000000001": {"par": 500_000, "own_value_base": 500_000.0}}
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=maia, athena_par=None)
    rows = {r["key"]: r for r in p["rows"]}
    assert rows["clean_bond_mv"]["maia"] == 495_000.0     # 500k × 99 / 100
    assert rows["accrued_income"]["maia"] == 2_500.0      # 5,000 × 0.5
    cov = p["coverage"]
    assert cov["admin_isins_missing_from_side"]["maia"] == [
        "DE0000000002", "GB00BBQ33664"]
    assert cov["matched"]["maia"] == 1


def test_maia_only_positions_bucketed_at_own_value_not_totalled():
    maia = {
        "US0000000001": {"par": 1_000_000, "own_value_base": 995_000.0},
        "IT0000000009": {"par": 200_000, "own_value_base": 210_000.0},  # govvie switch
    }
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=maia, athena_par=None)
    um = p["unmatched_positions"]
    assert [b["isin"] for b in um["maia"]] == ["IT0000000009"]
    assert um["maia_value_at_own_marks"] == 210_000.0
    # Not folded into the repriced clean total (only the matched US bond is).
    rows = {r["key"]: r for r in p["rows"]}
    assert rows["clean_bond_mv"]["maia"] == 990_000.0


def test_per_unit_detected_and_athena_units_normalised():
    # Athena's transactions book stores units×100 for the per-unit security.
    ath = {"US0000000001": 1_000_000, "DE0000000002": 500_000,
           "GB00BBQ33664": 170_000}
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=None, athena_par=ath)
    assert p["per_unit_isins"] == ["GB00BBQ33664"]
    rows = {r["key"]: r for r in p["rows"]}
    # 170,000/100 = 1,700 units × factor-embedded unit price = 170,000 —
    # NOT the +39.2m-style explosion a naive par ratio produces.
    assert rows["clean_bond_mv"]["athena"] == rows["clean_bond_mv"]["waystone"]
    assert any("divided by 100" in w for w in p["warnings"])


def test_null_never_zero_for_non_repriceable_components():
    p = aum_passes.admin_priced_pass(_parsed(), _sides(),
                                     maia_bonds=None, athena_par=None)
    rows = {r["key"]: r for r in p["rows"]}
    assert rows["other_net"]["maia"] is None
    assert rows["fx_forward_pnl_share_class"]["maia"] is None
    assert rows["clean_bond_mv"]["maia"] is None  # no bridge → null, not 0
    assert "clean_bond_mv" in p["total"]["components_missing"]["maia"]
    assert any("null, not zero" in w for w in p["warnings"])


def test_athena_txn_to_side_maps_tiered_totals():
    txn = {
        "date": "2026-07-31", "source": "transactions",
        "holdings": [{"isin": "US0000000001", "par": 1_000_000}],
        "unpriced": ["XS111"],
        "coverage": {"priced_isins": 27, "total_isins": 28},
        "totals": {
            "holdings": {"clean_bond_mv": 10_290_498.94,
                         "accrued_income": 159_094.28,
                         "dirty_bond_mv": 10_449_593.22},
            "valuation": {"cash": 50_000.0, "fx_forward_pnl_fund": 16_748.35,
                          "other_net": None, "total": 10_516_341.57},
            "nav": {"fx_forward_pnl_share_class": -25_884.92,
                    "total": 10_490_456.65},
        },
        "caveats": ["7 corps unpriced"],
    }
    side = aum_passes.athena_txn_to_side(txn)
    assert side["source"] == "transactions"
    assert side["clean_bond_mv"] == 10_290_498.94
    assert side["accrued_income"] == 159_094.28
    assert side["cash"] == 50_000.0
    assert side["fx_forward_pnl_fund"] == 16_748.35
    assert side["fx_forward_pnl_share_class"] == -25_884.92
    assert side["other_net"] is None
    assert side["total"] == 10_490_456.65
    assert side["coverage"]["priced_isins"] == 27
