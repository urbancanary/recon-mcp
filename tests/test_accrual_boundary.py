"""The coupon-at-C+1 receivable under the C+1 valuation convention.

Fixture figures are the QuantLib 1.37 measurements documented in
accrual_boundary's docstring: a 6.5% semi-annual bond, full period 3.25 per
100, valued 31-Jul with the coupon paying at the accrual point.
"""
import accrual_boundary as ab


def test_boundary_fires_when_coupon_pays_inside_the_window():
    # Business-day 1st: coupon pays exactly at C+1, accrued zeroed.
    assert ab.boundary(3.25, 0.0) is True
    assert ab.receivable_per100(3.25, 0.0) == 3.25


def test_weekend_case_is_not_a_boundary():
    # Payment lag puts the coupon outside the window — C+1 still carries it,
    # so there is nothing owed off-book and no receivable.
    assert ab.boundary(3.25, 3.25) is False
    assert ab.receivable_per100(3.25, 3.25) == 0.0


def test_ordinary_mid_period_bond_raises_no_receivable():
    # C+1 one day ahead of T+0 — the normal case. Must NOT be treated as a
    # boundary, or every bond in the book gets a phantom receivable.
    assert ab.boundary(1.7260, 1.8000) is False
    assert ab.receivable_per100(1.7260, 1.8000) == 0.0


def test_smaller_but_nonzero_c1_is_not_a_boundary():
    # A C+1 merely LOWER than T+0 is a day-count/convention difference, not a
    # coupon payment. Guessing here would be worse than the bug.
    assert ab.boundary(1.2541, 1.0833) is False
    assert ab.receivable_per100(1.2541, 1.0833) == 0.0


def test_genuinely_nil_accrued_is_not_a_boundary():
    assert ab.boundary(0.0, 0.0) is False
    assert ab.receivable_per100(0.0, 0.0) == 0.0


def test_missing_ga10_marks_raise_no_receivable():
    assert ab.boundary(None, 0.0) is False
    assert ab.receivable_per100(3.25, None) == 0.0
    assert ab.receivable_per100(None, None) == 0.0


def test_apply_books_receivable_and_leaves_c1_accrued_untouched():
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
    assert summary["receivable_total_local"] == 32_500.00   # 3.25/100 on 1m
    assert summary["amounts_are_lower_bounds"] is True
    b = summary["boundary_bonds"][0]
    assert b["isin"] == "XS_BOUNDARY" and b["is_lower_bound"] is True

    assert rows[0]["valn_boundary"] is True
    assert rows[0]["coupon_receivable_per100"] == 3.25
    assert rows[1]["coupon_receivable_per100"] == 0.0
    # THE ACCRUED COLUMN STAYS C+1 — the receivable is a separate line, so
    # accrued still reconciles to Bloomberg and the administrator.
    assert rows[0]["ga10_c1_per100"] == 0.0
    assert rows[1]["ga10_c1_per100"] == 1.8000
    assert rows[0]["waystone_per100"] == 3.25


def test_apply_says_so_plainly_when_nothing_fires():
    rows = [{"isin": "X", "par": 1000,
             "ga10_t0_per100": 1.0, "ga10_c1_per100": 1.1}]
    summary = ab.apply(rows)
    assert summary["boundary_count"] == 0
    assert summary["receivable_total_local"] == 0.0
    assert summary["amounts_are_lower_bounds"] is False
    assert summary["evaluated"] == 1 and summary["tested_of"] == 1
    assert "No bond pays a coupon inside" in summary["message"]


def test_unmarked_bonds_are_reported_not_counted_as_clean():
    rows = [
        {"isin": "MARKED", "par": 1000,
         "ga10_t0_per100": 1.0, "ga10_c1_per100": 1.1},
        {"isin": "UNENROLLED", "par": 1000,
         "ga10_t0_per100": None, "ga10_c1_per100": None},
    ]
    summary = ab.apply(rows)
    assert summary["evaluated"] == 1 and summary["tested_of"] == 2
    assert summary["unevaluable_isins"] == ["UNENROLLED"]
    assert "could NOT be tested" in summary["message"]
    assert "UNENROLLED" in summary["message"]
