"""Recover a bond's prior (last) coupon date — and its day-count basis — from
accrued interest alone.

Why this lives in recon-mcp: custodian/admin files rarely carry prior-coupon
dates, but the accrued they settle at ENCODES the date:

    accrued = Custodian_MV - round(nominal * clean_price / 100, 2)   # pure subtraction
    accrued = (coupon/freq) * daycount_fraction(last_coupon -> settlement)

Within a coupon period accrued is strictly increasing in days-since-last-coupon,
so given (coupon, freq, basis) the last-coupon date has exactly one solution.
Because our QuantLib engine is penny-exact, use EXACT-TIE elimination, not
tolerance fitting: /360 never exactly equals /365 (days/365 = d30/360 needs
72*days = 73*d30 — effectively never on real dates), so on a well-placed
observation the true basis is the only one that ties to the cent and every
other basis is *disproved*, not merely "worse".

Operating rules (from the 2026-07 recon work, memory id=2367):
- Prefer observations 2-5 months into a period; all bases collide near period
  start/end where accrued ~ 0 or ~ full coupon.
- Confirm FREQUENCY across >=2 consecutive month-ends: BBG-style sources accrue
  past 6 months and post the reset at month-end, so a semi bond can look annual
  on one month-end; the next month-end confirms or denies (gotcha id=623).
- No exact tie on a clean observation is a STRUCTURE SIGNAL (odd first/last
  stub, ex-dividend, pool factor != 1, wrong coupon) — classify, never widen
  the tolerance.
- The recovered anniversary day-of-month also snaps par-call solver candidate
  grids (structure inference).

Not for: odd stubs, ex-div (negative accrued), step/FRN coupons, sinkers with
factor != 1 — those are the cases the failure mode is designed to flag.
"""
from __future__ import annotations

import datetime as dt
from calendar import monthrange

BASES = ("ACT/ACT", "ACT/365", "30/360", "30E/360")


def addm(d: dt.date, n: int) -> dt.date:
    """Add n months, clamping day-of-month (handles month-end)."""
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return dt.date(y, m, min(d.day, monthrange(y, m)[1]))


def _d30(a: dt.date, b: dt.date, european: bool) -> int:
    """30/360 day count. european=True -> 30E (both ends capped at 30);
    False -> US/SIA (D2 capped only when D1 is 30/31 — the day-31 settlement
    divergence that separates the two on month-end recons)."""
    D1 = min(a.day, 30)
    D2 = min(b.day, 30) if european or (b.day == 31 and a.day >= 30) else b.day
    if not european:
        D2 = min(b.day, 30) if (b.day == 31 and D1 == 30) else b.day
    return 360 * (b.year - a.year) + 30 * (b.month - a.month) + (D2 - D1)


def accrued_from(basis: str, coupon: float, freq: int,
                 last: dt.date, settle: dt.date) -> float:
    """Accrued per 100 face for the given basis and last-coupon date."""
    nxt = addm(last, 12 // freq)
    if basis == "ACT/ACT":
        return coupon / freq * (settle - last).days / (nxt - last).days
    if basis == "ACT/365":
        return coupon / freq * (settle - last).days / (365 / freq)
    return coupon / freq * _d30(last, settle, basis == "30E/360") / (360 / freq)


def embedded_accrued(mv: float, clean_price: float, nominal: float) -> float:
    """Extract accrued (currency) from a custodian market value by subtraction."""
    return round(mv - round(nominal * clean_price / 100.0, 2), 2)


def recover_last_coupon(coupon: float, nominal: float, accrued: float,
                        settle: dt.date, freq: int = 2, basis: str = "ACT/ACT",
                        tol: float = 0.01, lookback: int = 370):
    """Return (last_coupon_date, gap_in_currency). gap<=tol means an exact tie
    at cent-rounding; anything larger is a disproof of this (basis,freq), not a
    near-miss."""
    best = None
    for k in range(lookback + 1):
        L = settle - dt.timedelta(days=k)
        cand = accrued_from(basis, coupon, freq, L, settle) / 100.0 * nominal
        gap = abs(cand - accrued)
        if best is None or gap < best[1]:
            best = (L, gap)
        if gap <= tol:
            return L, gap
    return best


def solve_basis_and_anchor(coupon: float, nominal: float,
                           observations: list[tuple[dt.date, float]],
                           freq: int = 2, tol: float = 0.01) -> list[dict]:
    """Joint solve across multiple (settle_date, accrued_currency) observations.
    Returns the candidate (basis, anchor day-of-month) pairs that tie EVERY
    observation exactly. One survivor = identified; none = structure signal;
    several = add an observation (prefer mid-period dates)."""
    out = []
    for basis in BASES:
        anchors = set()
        ok = True
        for settle, acc in observations:
            L, gap = recover_last_coupon(coupon, nominal, acc, settle, freq, basis, tol)
            if gap > tol:
                ok = False
                break
            anchors.add((L.month % (12 // freq), L.day))  # anniversary grid id
        if ok and len(anchors) == 1:
            out.append({"basis": basis, "freq": freq,
                        "anchor_month_mod": next(iter(anchors))[0],
                        "anchor_day": next(iter(anchors))[1]})
    return out
