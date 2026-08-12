"""Main-fund AUM on GA10 accrual — the three traps and the cash check.

Figures mirror the GDBF 2026-07-31 shape so the arithmetic is hand-checkable:
a declared-but-unpaid income gap, partial GA10 coverage, and a per-unit bond
whose FX/unit scaling must cancel in the ratio.
"""
import main_fund_breakdown as mfb


def _parsed():
    return {
        "base_currency": "USD",
        "holdings": [
            {"isin": "A", "description": "USD bond", "currency": "USD",
             "par_amount": 1_000_000, "accrued_income": 10_000.0},
            {"isin": "B", "description": "EUR bond", "currency": "EUR",
             "par_amount": 500_000, "accrued_income": 2_200.0},
            # GA10 has no mark for this one — must be RETAINED, not dropped.
            {"isin": "C", "description": "unenrolled", "currency": "GBP",
             "par_amount": 200_000, "accrued_income": 3_000.0},
        ],
    }


def _waystone():
    # Balance-sheet accrued sits ABOVE the per-bond sum (10,000+2,200+3,000
    # = 15,200) — the 1,800 gap is declared-but-unpaid income.
    return {
        "clean_bond_mv": 1_000_000.0,
        "accrued_income": 17_000.0,
        "cash": 50_000.0,
        "fx_forward_pnl_fund": -500.0,
        "fx_forward_pnl_share_class": 2_000.0,
        "other_net": -100.0,
        "total": 1_068_400.0,
    }


def _rows():
    # waystone_per100 / ga10_c1_per100 — GA10 accrues 20% more on A, 10% on B.
    return [
        {"isin": "A", "waystone_per100": 1.0, "ga10_c1_per100": 1.2},
        {"isin": "B", "waystone_per100": 0.44, "ga10_c1_per100": 0.484},
        {"isin": "C", "waystone_per100": 1.5, "ga10_c1_per100": None},
    ]


def _marks():
    return {"A": {"accrued_per_100_c1": 1.2}, "B": {"accrued_per_100_c1": 0.484}}


def test_declared_unpaid_income_survives_the_swap():
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    a = r["accrued"]
    assert a["waystone_per_bond_sum"] == 15_200.00
    assert a["waystone_balance_sheet"] == 17_000.00
    assert a["declared_unpaid_income"] == 1_800.00
    # GA10: A 12,000 + B 2,420 + C retained 3,000 = 17,420
    assert a["ga10_c1_per_bond"] == 17_420.00
    # Restated accrued keeps the declared-but-unpaid balance.
    assert a["restated_total"] == 19_220.00


def test_uncovered_bond_is_retained_not_dropped():
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    a = r["accrued"]
    assert a["coverage"] == "2/3"
    assert a["bonds_retained_at_waystone"] == 1
    assert a["value_retained_at_waystone"] == 3_000.00
    c = next(x for x in a["rows"] if x["isin"] == "C")
    assert c["source"] == "waystone_retained"
    assert c["ga10_accrued_base"] == c["waystone_accrued_base"] == 3_000.00
    assert any("must not be read as a full one" in x for x in r["caveats"])


def test_ratio_scaling_cancels_fx_and_unit_basis():
    # B is EUR: its base-currency accrued is scaled by the per-100 RATIO, so
    # no FX rate is ever applied and per-unit scaling would cancel identically.
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    b = next(x for x in r["accrued"]["rows"] if x["isin"] == "B")
    assert b["ga10_accrued_base"] == 2_420.00      # 2,200 x (0.484/0.44)
    assert b["diff_base"] == 220.00


def test_main_fund_strips_share_class_but_keeps_fund_hedge():
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    # accrued delta = restated 19,220 - balance sheet 17,000 = +2,220
    assert r["accrued"]["delta_vs_waystone"] == 2_220.00
    assert r["restated_nav"] == 1_070_620.00        # 1,068,400 + 2,220
    assert r["main_fund_aum"] == 1_068_620.00       # less 2,000 share-class
    # Fund-level hedge is still a component, deliberately retained.
    keys = [b["key"] for b in r["breakdown"]]
    assert "fx_forward_pnl_fund" in keys
    assert "fx_forward_pnl_share_class" not in keys


def test_breakdown_percentages_sum_to_one_hundred():
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    assert abs(r["breakdown_pct_total"] - 100.0) < 0.01
    clean = next(b for b in r["breakdown"] if b["key"] == "clean_bond_mv")
    assert clean["pct_of_main_fund"] == round(
        1_000_000.0 / r["main_fund_aum"] * 100, 3)


def test_cash_check_states_the_difference_and_refuses_to_tick():
    r = mfb.build(_parsed(), _marks(), _waystone(),
                  {"cash": 50_135.19}, _rows())
    c = r["cash_check"]
    assert c["maia"] == 50_135.19 and c["waystone"] == 50_000.0
    assert c["difference"] == 135.19
    # Never asserts a tally — the two measures are not like-for-like.
    assert c["like_for_like"] is False
    assert "unsettled FX forward legs" in c["note"]


def test_absent_maia_leaves_cash_check_null_not_zero():
    r = mfb.build(_parsed(), _marks(), _waystone(), None, _rows())
    assert r["cash_check"]["maia"] is None
    assert r["cash_check"]["difference"] is None
