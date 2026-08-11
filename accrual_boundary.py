"""Month-end accrual boundary rule — stop a full coupon vanishing from NAV.

THE TRAP, measured with QuantLib 1.37 (2026-08-11).

Accrued is struck at C+1 (the calendar day after valuation) because that is
the Bloomberg index convention, and matching BBG is what makes the fund's
analytics comparable. QuantLib resets accrued at the coupon's PAYMENT date,
not its schedule date — which usually protects us:

    coupon schedule 1-Aug-2026 (Sat), pays Mon 3-Aug
        accrued 31-Jul (T+0) = 3.250000
        accrued  1-Aug (C+1) = 3.250000   <- entitlement preserved, no hole

But when the coupon's payment date lands EXACTLY on the accrual point, the
coupon is deemed paid and accrued resets to nil:

    coupon schedule 1-Aug-2025 (Fri), pays 1-Aug
        accrued 31-Jul (T+0) = 3.250000
        accrued  1-Aug (C+1) = 0.000000   <- a full coupon disappears

At that instant the fund is owed the coupon, has not received the cash (our
cash is struck at T+0), and accrued says nil. The value is in no line of the
valuation. That is a basis mismatch — accrued measured at C+1 against cash
measured at T+0 — not a QuantLib defect.

NOTE THE CALENDAR IS THE OPPOSITE WAY ROUND to intuition: a weekend/holiday
1st is SAFE (payment lag preserves the entitlement); a working-day 1st is
the exposure. So this bites on the more common month-end, not the rarer one.

THE RULE. `accrued_valn` is the entitlement basis: normally the C+1 figure
(unchanged, BBG-comparable), but at the boundary the T+0 figure, which is
exactly the full coupon the fund is owed. Both inputs come from GA10's own
QuantLib calls at two settlement dates — nothing is re-derived here, no
formula substitutes for the engine (see mcp_central/CLAUDE.md).

WHAT THIS IS NOT. It does not touch `accrued_income` on any settled trade,
and it does not change the C+1 figures used to reconcile against Waystone's
per-bond lines. It adds a separate, labelled measure for valuation and
factsheet weighting, leaving every existing number untouched.

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
    """True when C+1 has reset to nil while T+0 still carries the coupon.

    Deliberately narrow: it fires only on an exact nil at C+1 against a
    positive T+0. A merely SMALLER C+1 is not a boundary — it is either a
    day-count difference or a bond mid-period, and guessing there would
    corrupt ordinary bonds to fix a rare one.
    """
    if t0_per100 is None or c1_per100 is None:
        return False
    return c1_per100 == 0.0 and t0_per100 > _MATERIAL_PER100


def valn_per100(t0_per100: float | None,
                c1_per100: float | None) -> float | None:
    """Accrued on the ENTITLEMENT basis: C+1 normally, T+0 at the boundary."""
    if boundary(t0_per100, c1_per100):
        return t0_per100
    return c1_per100


def apply(rows: list[dict]) -> dict:
    """Add `ga10_valn_per100` + `valn_boundary` to accrued-recon rows.

    Returns a summary the payload can surface: which bonds hit the boundary
    and how much coupon would otherwise have gone missing. Rows are mutated
    in place; every field they already carry is left exactly as it was.
    """
    flagged = []
    for r in rows:
        t0, c1 = r.get("ga10_t0_per100"), r.get("ga10_c1_per100")
        is_b = boundary(t0, c1)
        v = valn_per100(t0, c1)
        r["ga10_valn_per100"] = None if v is None else round(v, 4)
        r["valn_boundary"] = is_b
        if is_b:
            par = r.get("par") or 0.0
            flagged.append({
                "isin": r.get("isin"),
                "description": r.get("description"),
                "currency": r.get("currency"),
                "coupon_per100": round(t0, 4),
                # Local currency: the caller knows the FX, and stating it in
                # local keeps this figure independent of any rate.
                "coupon_local": round(t0 * par / 100.0, 2),
            })
    return {
        "basis": "Accrued on the entitlement basis: the C+1 (Bloomberg index) "
                 "figure normally, and the T+0 figure where a coupon pays "
                 "exactly at the accrual point and C+1 has reset to nil. Use "
                 "this for NAV and factsheet weighting; use ga10_c1_per100 to "
                 "reconcile against the administrator's per-bond lines.",
        "boundary_bonds": flagged,
        "boundary_count": len(flagged),
        "message": (
            "{} bond(s) pay a coupon exactly at the accrual point this "
            "valuation. Their C+1 accrued is nil while the cash has not been "
            "received, so the coupon sits in no line of the valuation unless "
            "the entitlement basis is used.".format(len(flagged))
            if flagged else
            "No bond pays a coupon at the accrual point this valuation — the "
            "entitlement and C+1 bases agree on every line."),
    }
