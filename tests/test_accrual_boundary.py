"""The month-end accrual boundary rule.

Fixture figures are the QuantLib 1.37 measurements documented in
accrual_boundary's docstring: a 6.5% semi-annual bond, full period 3.25 per
100, valued 31-Jul with the coupon paying at the accrual point.
"""
import accrual_boundary as ab


def test_boundary_fires_when_c1_resets_to_nil():
    # Business-day 1st: coupon pays exactly at C+1, accrued zeroed.
    assert ab.boundary(3.25, 0.0) is True
    assert ab.valn_per100(3.25, 0.0) == 3.25


def test_weekend_case_is_not_a_boundary():
    # Payment lag preserves the entitlement — C+1 still carries the coupon,
    # so there is nothing to correct and the C+1 figure stands.
    assert ab.boundary(3.25, 3.25) is False
    assert ab.valn_per100(3.25, 3.25) == 3.25


def test_ordinary_mid_period_bond_untouched():
    # C+1 one day ahead of T+0 — the normal case. Must NOT be treated as a
    # boundary, or every bond in the book gets corrupted to fix a rare one.
    assert ab.boundary(1.7260, 1.8000) is False
    assert ab.valn_per100(1.7260, 1.8000) == 1.8000


def test_smaller_but_nonzero_c1_is_not_a_boundary():
    # A C+1 merely LOWER than T+0 is a day-count/convention difference, not a
    # coupon payment. Guessing here would be worse than the bug.
    assert ab.boundary(1.2541, 1.0833) is False
    assert ab.valn_per100(1.2541, 1.0833) == 1.0833


def test_genuinely_nil_accrued_is_not_a_boundary():
    # A bond that just reset on the valuation date itself carries ~nil on
    # both legs — no coupon is being lost.
    assert ab.boundary(0.0, 0.0) is False
    assert ab.valn_per100(0.0, 0.0) == 0.0


def test_missing_ga10_marks_return_none_not_zero():
    assert ab.boundary(None, 0.0) is False
    assert ab.valn_per100(None, None) is None
    # C+1 absent → nothing to state; never substitute T+0 silently.
    assert ab.valn_per100(3.25, None) is None


def test_apply_flags_and_quantifies_without_touching_existing_fields():
    rows = [
        {"isin": "XS_BOUNDARY", "description": "6.5% 2035", "currency": "GBP",
         "par": 1_000_000, "waystone_per100": 3.25,
         "ga10_t0_per100": 3.25, "ga10_c1_per100": 0.0},
        {"isin": "XS_NORMAL", "description": "1.8% 2040", "currency": "EUR",
         "par": 870_000, "waystone_per100": 1.5050,
         "ga10_t0_per100": 1.7260, "ga10_c1_per100": 1.8000},
    ]
    summary = ab.apply(rows)

    assert summary["boundary_count"] == 1
    b = summary["boundary_bonds"][0]
    assert b["isin"] == "XS_BOUNDARY"
    assert b["coupon_per100"] == 3.25
    assert b["coupon_local"] == 32_500.00   # 3.25 per 100 on 1m par

    assert rows[0]["valn_boundary"] is True
    assert rows[0]["ga10_valn_per100"] == 3.25
    assert rows[1]["valn_boundary"] is False
    assert rows[1]["ga10_valn_per100"] == 1.8000
    # Pre-existing fields untouched — the rule is additive.
    assert rows[0]["waystone_per100"] == 3.25
    assert rows[1]["ga10_c1_per100"] == 1.8000


def test_apply_says_so_plainly_when_nothing_fires():
    rows = [{"isin": "X", "par": 1000,
             "ga10_t0_per100": 1.0, "ga10_c1_per100": 1.1}]
    summary = ab.apply(rows)
    assert summary["boundary_count"] == 0
    assert summary["evaluated"] == 1 and summary["tested_of"] == 1
    assert "No bond pays a coupon at the accrual point" in summary["message"]


def test_unmarked_bonds_are_reported_not_counted_as_clean():
    # A bond GA10 cannot mark at both settle dates is a BLIND SPOT. The
    # summary must say so — "no boundary bonds" over an untested book is a
    # claim the data does not support.
    rows = [
        {"isin": "MARKED", "par": 1000,
         "ga10_t0_per100": 1.0, "ga10_c1_per100": 1.1},
        {"isin": "UNENROLLED", "par": 1000,
         "ga10_t0_per100": None, "ga10_c1_per100": None},
    ]
    summary = ab.apply(rows)
    assert summary["boundary_count"] == 0
    assert summary["evaluated"] == 1
    assert summary["tested_of"] == 2
    assert summary["unevaluable_isins"] == ["UNENROLLED"]
    assert "could NOT be tested" in summary["message"]
    assert "UNENROLLED" in summary["message"]
    # The untestable bond still gets an honest null, never a fabricated value.
    assert rows[1]["ga10_valn_per100"] is None
    assert rows[1]["valn_boundary"] is False
