"""The coupon-at-C+1 receivable — the one gap the C+1 valuation basis leaves.

CONVENTION (Andy, 2026-08-11): valuation always accrues to C+1 (valuation date
+ 1 calendar day); trades accrue to their own market settlement, T+n. That is
ONE rule, not two: at a calendar month end C+1 already IS the first calendar
day of the next month (31-Mar->1-Apr), which is the Bloomberg index basis, so
there is no month-end special case anywhere in this module.

THE GAP. A coupon whose PAYMENT date falls in the window (valuation date, C+1]
is deemed paid by the accrual, so accrued at C+1 is nil — while cash is struck
at the valuation date and has not received it. The fund is owed a full coupon
that appears in no line of the valuation. Measured with QuantLib 1.37 on a
6.5% semi-annual bond:

    coupon pays 1-Aug-2025 (Fri, = C+1)   accrued T+0 3.250000 -> C+1 0.000000
    coupon schedule 1-Aug-2026 (Sat),
    pays Mon 3-Aug (outside the window)   accrued T+0 3.250000 -> C+1 3.250000

Note the calendar runs opposite to intuition: QuantLib resets on the PAYMENT
date, so a weekend 1st is protected by the payment lag and a WORKING-day 1st
is the exposure — the more common month end.

THE TREATMENT. Book a coupon RECEIVABLE for the window; do NOT substitute the
T+0 accrued figure into the accrued column. Two reasons: the accrued line stays
honestly C+1 (so it still reconciles against Bloomberg and the administrator's
own per-bond lines), and a receivable is what the money actually is — cash owed,
not interest accruing. It is also how Waystone books it, in its income bucket,
which is why the totals tie.

    valuation = clean + accrued(C+1) + cash(valuation date) + coupon receivable

PRECISION — READ BEFORE RELYING ON THE AMOUNT. The receivable should be the
ACTUAL coupon for the period just ended. The best figure available from the two
settle dates we already fetch is the T+0 accrued, which is a LOWER BOUND: exact
under 30/360-style counts (where the 31st collapses to the 30th, so T+0 and the
period end coincide) but up to one day short under ACT counts. Rows carry
`receivable_is_lower_bound` so a consumer can refuse to book an approximate
figure; the exact amount needs the coupon from QuantLib, which is a targeted
per-bond call and cheap because this fires on very few bonds.

REVERT: stop calling `apply()` (one call site in aum_orchestrator._accrued_recon)
— every pre-existing field is computed independently of this module.
"""

from __future__ import annotations

# A reset is only credible when the pre-reset figure is a real accrual. Below
# this (per 100 of par) we are looking at a bond that genuinely carries almost
# no accrued, not a coupon that has just been paid away.
_MATERIAL_PER100 = 1e-6


def boundary(t0_per100: float | None,
             c1_per100: float | None) -> bool:
    """True when a coupon paid in (valuation date, C+1] has zeroed the accrual.

    Deliberately narrow: it fires only on an exact nil at C+1 against a
    positive T+0. A merely SMALLER C+1 is not a boundary — it is either a
    day-count difference or a bond mid-period, and guessing there would
    corrupt ordinary bonds to fix a rare one.
    """
    if t0_per100 is None or c1_per100 is None:
        return False
    return c1_per100 == 0.0 and t0_per100 > _MATERIAL_PER100


def receivable_per100(t0_per100: float | None,
                      c1_per100: float | None) -> float:
    """Coupon owed but present in neither accrued nor cash, per 100 of par.

    Zero for every ordinary bond. At the boundary this is a LOWER BOUND on the
    coupon — see the precision note in the module docstring.
    """
    if boundary(t0_per100, c1_per100):
        return t0_per100
    return 0.0


def apply(rows: list[dict]) -> dict:
    """Add `coupon_receivable_per100` + `valn_boundary` to accrued-recon rows.

    Returns a summary the payload can surface. Rows are mutated in place and
    every field they already carry — the C+1 accrued included — is left
    exactly as it was.
    """
    flagged, unevaluable = [], []
    total_local = 0.0
    for r in rows:
        t0, c1 = r.get("ga10_t0_per100"), r.get("ga10_c1_per100")
        # A bond GA10 could not mark at BOTH settle dates cannot be tested for
        # the boundary at all. Silence there would read as "no coupon at
        # risk", which is a claim the data does not support — so it is
        # counted and named, not folded into a clean result.
        if t0 is None or c1 is None:
            unevaluable.append(r.get("isin"))
        is_b = boundary(t0, c1)
        rec = receivable_per100(t0, c1)
        r["coupon_receivable_per100"] = round(rec, 4) if is_b else 0.0
        r["valn_boundary"] = is_b
        r["receivable_is_lower_bound"] = is_b
        if is_b:
            par = r.get("par") or 0.0
            local = rec * par / 100.0
            total_local += local
            flagged.append({
                "isin": r.get("isin"),
                "description": r.get("description"),
                "currency": r.get("currency"),
                "coupon_per100": round(rec, 4),
                # Local currency: the caller knows the FX, and stating it in
                # local keeps this figure independent of any rate.
                "coupon_local": round(local, 2),
                "is_lower_bound": True,
            })
    return {
        "basis": "Valuation accrues to C+1 (valuation date + 1 calendar day) "
                 "on every date, month end included — at a calendar month end "
                 "C+1 is already the first day of the next month. A coupon "
                 "paying inside (valuation date, C+1] is booked as a "
                 "RECEIVABLE here rather than adjusting the accrued column, "
                 "which stays C+1 and still reconciles to Bloomberg and to "
                 "the administrator's per-bond lines.",
        "boundary_bonds": flagged,
        "boundary_count": len(flagged),
        "receivable_total_local": round(total_local, 2),
        "amounts_are_lower_bounds": bool(flagged),
        # Coverage is part of the finding, not a footnote: the rule can only
        # speak for bonds marked at both settle dates.
        "evaluated": len(rows) - len(unevaluable),
        "tested_of": len(rows),
        "unevaluable_isins": unevaluable,
        "message": (
            "{} bond(s) pay a coupon inside (valuation date, C+1]. Their C+1 "
            "accrued is nil and the cash has not been received, so the coupon "
            "belongs in no line of the valuation unless booked as a "
            "receivable. Amounts shown are LOWER BOUNDS (T+0 accrued) — take "
            "the exact coupon from QuantLib before booking."
            .format(len(flagged))
            if flagged else
            "No bond pays a coupon inside (valuation date, C+1] among the {} "
            "of {} bonds that could be tested.".format(
                len(rows) - len(unevaluable), len(rows)))
        + ("" if not unevaluable else
           " {} bond(s) are not marked by GA10 at both settlement dates and "
           "could NOT be tested — if one of them pays in the window it would "
           "go unnoticed. Enrol them to close the gap: {}.".format(
               len(unevaluable), ", ".join(str(i) for i in unevaluable))),
    }
