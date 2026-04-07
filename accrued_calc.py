"""
accrued_calc.py — Compute accrued interest from bond static data.

No QuantLib dependency. Uses coupon, maturity, day_count, frequency
from bond_reference to compute accrued at multiple settlement dates.

This replaces GA10 for accrued calculations. GA10 is still used for
yield/duration/spread (which genuinely need QuantLib).
"""

import logging
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional

logger = logging.getLogger(__name__)


def compute_accrued(
    coupon: float,
    maturity_date: date,
    par: float,
    settlement_date: date,
    frequency: str = "Semiannual",
    day_count: str = "30/360",
    accrual_date: Optional[date] = None,
) -> float:
    """Compute accrued interest for a bond at a given settlement date.

    Args:
        coupon: Annual coupon rate as percentage (e.g. 2.98 for 2.98%)
        maturity_date: Bond maturity date
        par: Face value / par amount
        settlement_date: Date to compute accrued for
        frequency: "Annual", "Semiannual", "Quarterly"
        day_count: "30/360", "ACT/365", "ACT/ACT", "ACT/360"
        accrual_date: First accrual start date (if known)

    Returns:
        Accrued interest in absolute currency amount
    """
    if coupon is None or coupon == 0 or par is None or par == 0:
        return 0.0

    # Determine coupon period in months
    freq_months = {"Annual": 12, "Semiannual": 6, "Quarterly": 3}.get(frequency, 6)

    # Build coupon schedule backwards from maturity
    last_coupon = _find_last_coupon_date(maturity_date, settlement_date, freq_months, accrual_date)

    if last_coupon is None or last_coupon >= settlement_date:
        return 0.0

    # Count days based on day count convention
    days = _day_count(last_coupon, settlement_date, day_count)
    basis = _day_count_basis(day_count)

    # Accrued = par × (coupon% / 100) × (days / basis)
    accrued = par * (coupon / 100.0) * (days / basis)

    return round(accrued, 6)


def compute_accrued_multi(
    coupon: float,
    maturity_date: date,
    par: float,
    valuation_date: date,
    frequency: str = "Semiannual",
    day_count: str = "30/360",
    accrual_date: Optional[date] = None,
) -> dict:
    """Compute accrued at multiple settlement dates.

    Returns dict with keys: t0, c1, t1, c2, c3
    - t0: valuation date
    - c1: next calendar day (C+1)
    - t1: next business day (T+1, skips weekends)
    - c2, c3: calendar +2, +3
    """
    t0 = valuation_date
    c1 = t0 + timedelta(days=1)
    c2 = t0 + timedelta(days=2)
    c3 = t0 + timedelta(days=3)

    # T+1 = next business day (skip weekends)
    t1 = t0 + timedelta(days=1)
    while t1.weekday() >= 5:  # 5=Sat, 6=Sun
        t1 += timedelta(days=1)

    kwargs = dict(coupon=coupon, maturity_date=maturity_date, par=par,
                  frequency=frequency, day_count=day_count, accrual_date=accrual_date)

    return {
        "t0": compute_accrued(settlement_date=t0, **kwargs),
        "c1": compute_accrued(settlement_date=c1, **kwargs),
        "t1": compute_accrued(settlement_date=t1, **kwargs),
        "c2": compute_accrued(settlement_date=c2, **kwargs),
        "c3": compute_accrued(settlement_date=c3, **kwargs),
    }


def _find_last_coupon_date(
    maturity: date, settlement: date, freq_months: int, accrual_start: Optional[date]
) -> Optional[date]:
    """Walk backwards from maturity to find the most recent coupon date before settlement."""
    # Start from maturity and walk back
    d = maturity
    while d > settlement:
        d -= relativedelta(months=freq_months)

    # d is now the last coupon date on or before settlement
    # But if we have an accrual_start and d is before it, use accrual_start
    if accrual_start and d < accrual_start:
        return accrual_start

    return d


def _day_count(start: date, end: date, convention: str) -> float:
    """Count days between two dates using the specified convention."""
    conv = (convention or "30/360").upper().replace(" ", "")

    if conv in ("30/360", "30E/360", "BOND"):
        # 30/360 European/Bond convention
        d1, m1, y1 = start.day, start.month, start.year
        d2, m2, y2 = end.day, end.month, end.year
        d1 = min(d1, 30)
        if d1 == 30:
            d2 = min(d2, 30)
        return 360 * (y2 - y1) + 30 * (m2 - m1) + (d2 - d1)

    elif conv in ("ACT/365", "ACTUAL/365", "ACT/365FIXED"):
        return (end - start).days

    elif conv in ("ACT/360", "ACTUAL/360"):
        return (end - start).days

    elif conv in ("ACT/ACT", "ACTUAL/ACTUAL", "ACT/ACTISDA"):
        return (end - start).days

    else:
        # Default to actual days
        logger.warning(f"Unknown day count convention: {convention}, using actual days")
        return (end - start).days


def _day_count_basis(convention: str) -> float:
    """Return the year basis for a day count convention."""
    conv = (convention or "30/360").upper().replace(" ", "")

    if conv in ("30/360", "30E/360", "BOND"):
        return 360.0
    elif conv in ("ACT/365", "ACTUAL/365", "ACT/365FIXED"):
        return 365.0
    elif conv in ("ACT/360", "ACTUAL/360"):
        return 360.0
    elif conv in ("ACT/ACT", "ACTUAL/ACTUAL", "ACT/ACTISDA"):
        return 365.0  # simplified — proper Act/Act uses actual period days
    else:
        return 365.0
