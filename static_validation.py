"""Validate bond static against settled-trade confirms. Trades are gospel.

WHY THIS IS THE ONLY NON-CIRCULAR CHECK WE HAVE. Comparing our accrued to the
administrator's is two calculations disagreeing — neither is authority. A
SETTLED TRADE'S ACCRUED IS MONEY THAT ACTUALLY CHANGED HANDS on a counterparty
confirm: per bond, on a known date, produced by someone else's system. So when
QuantLib disagrees with a confirm, the static is wrong — not the confirm.
(mcp_central/CLAUDE.md: when QuantLib output looks wrong the error is almost
certainly in the static data. This is how you TEST that, instead of guessing.)

PROVEN ON FIRST RUN, GDBF 2026-07-15 settlements — every confirm reconciled
exactly to ACT/365 annual to six decimal places, and GA10 did not:

    Vodafone   XS2630493570  trade 6.991781  GA10 3.044444   (8.000 x 319/365)
    SW Finance XS2731297235  trade 4.344177  GA10 0.676042   (7.375 x 215/365)
    PIC        XS2819228664  trade 4.558219  GA10 1.145833   (6.875 x 242/365)
    Bund       DE0001030757  trade 1.647123  GA10 1.650000   (1.800 x 334/365)

Three GBP bonds modelled SEMI-ANNUAL that pay ANNUAL, plus a 30/360 basis
where the trades use ACT/365. The Bund carries only the basis defect, which is
why its miss is 0.003 per 100 rather than 3.7.

THE SHAPE OF THE MISMATCH LOCALISES THE FIELD, which is what makes this a
diagnostic rather than a pass/fail:

    ratio ~ 0.5 or ~2.0   frequency        (period halved/doubled)
    ratio within ~1.5%    day-count basis  (360 vs 365 is 1.0139)
    otherwise             schedule / last-coupon date, or the coupon itself

FROZEN-CALC RULE. A mismatch corrects static GOING FORWARD (Tier D valuation).
It must NEVER re-derive the settled accrued: Tier C settled values are frozen,
and silently restating them would retroactively move booked P&L and NAV.

"GOSPEL UNLESS WE FLAG OTHERWISE." A confirm can occasionally be wrong (a
mis-booked trade, a broker error). EXCEPTIONS carries those, keyed by ISIN or
by (ISIN, settlement_date), each with a REASON. An excepted trade is still
listed in the output with its reason — suppressed from the findings, never
from the page. Silence would let a real defect hide behind a stale exception.

COVERAGE IS A FINDING. This tests only bonds we have actually traded, and only
pins the static as at each trade date. A clean result means "no contradiction
found among the trades we hold" — never "the static is correct".
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

# Trades whose confirm we do NOT treat as authority, with the reason. Keys are
# either "ISIN" (all trades in that bond) or "ISIN@YYYY-MM-DD" (one settlement).
# Every entry MUST carry a reason — an unexplained exception is a hidden defect.
EXCEPTIONS: dict[str, str] = {}

# Per 100 of par. Below this a difference is rounding on the confirm, not a
# static defect: the smallest real defect we have seen (a 360-vs-365 basis on a
# 1.8% coupon) is 0.003, an order of magnitude above this.
_TOL_PER100 = 0.0005

_TRADE_TYPES = ("BUY", "SELL")


def _classify(trade: float, ga10: float) -> tuple[str, str]:
    """(defect, explanation) from the shape of the mismatch.

    DELIBERATELY NOT CLAIMING MORE THAN THE NUMBERS SUPPORT. A value ratio
    cannot separate a frequency error from a wrong last-coupon date, because
    real defects COMPOUND: on the measured GDBF book the three annual bonds
    are modelled semi-annual AND on 30/360, so their ratios came out 0.435,
    0.156 and 0.251 — nowhere near the clean 0.5 a pure frequency halving
    would give. An earlier version banded around 0.5 and mis-scored all three.

    So only two verdicts are offered: a small proportional miss is a BASIS
    difference (dates agree, denominator does not), and anything larger is a
    PERIOD ANCHOR difference — the accrual start and/or frequency is wrong,
    and which of the two it is comes from the day counts, not the ratio.
    """
    if not trade:
        return ("indeterminate", "trade accrued is zero — nothing to scale against")
    ratio = ga10 / trade
    if 0.985 <= ratio <= 1.015:
        return ("day_count",
                "a basis difference, not a date one — the accrual dates agree "
                "but the denominator does not (30/360 against ACT/365 is a "
                "factor of 1.0139)")
    return ("period_anchor",
            "our accrual period differs materially from the trade's, so the "
            "assumed last-coupon date and/or the coupon frequency is wrong. "
            "Compare our accrued_days against the trade-implied days to see "
            "which: a whole-period offset points at frequency, a partial one "
            "at the schedule anchoring")


async def _orca_trades(pid: str) -> list[dict]:
    """Settled bond trades from orca. NOTE the portfolio id: the transactions
    book is keyed 'gdbft', and asking for 'gdbf' returns count=0 SILENTLY —
    an empty result must never be read as "no trades to check"."""
    from auth_client import get_api_key, get_service_url
    url = (get_service_url("ORCA_MCP_URL") or "").rstrip("/")
    key = get_api_key("ORCA_API_KEY") or ""
    if not url:
        raise RuntimeError("ORCA_MCP_URL unavailable from auth-mcp")
    async with httpx.AsyncClient(timeout=45.0) as c:
        r = await c.get(f"{url}/api/transactions",
                        params={"portfolio_id": pid},
                        headers={"X-API-Key": key} if key else {})
    r.raise_for_status()
    rows = (r.json() or {}).get("transactions") or []
    return [t for t in rows
            if t.get("transaction_type") in _TRADE_TYPES
            and t.get("isin") and t.get("settlement_date")
            and t.get("accrued_interest") is not None]


async def _ga10_accrued_batch(prices: dict, settle: str,
                              api_key: str, ga10_url: str) -> dict:
    """{isin: analytics} for a whole settlement date, via portfolio/analysis.

    USE THIS ENDPOINT, NOT /api/v1/bond/analysis/flexible. Measured
    2026-08-12: `flexible` returns day_count "Thirty360", frequency 2 and
    coupon None for every bond — unconditionally, even when the correct
    conventions are passed explicitly — so it silently ignores stored static
    and manufactures disagreements that do not exist. An earlier version of
    this module called it and produced ~51 findings that were artifacts of
    its own engine call, not defects in the book.

    portfolio/analysis, same host and same bonds, reproduces the settled
    trade confirms to six decimal places:

        Vodafone   XS2630493570   6.991781 vs confirm 6.991781
        SW Finance XS2731297235   4.344178 vs confirm 4.344177
        PIC        XS2819228664   4.558219 vs confirm 4.558219
        Bund       DE0001030757   1.647123 vs confirm 1.647123

    Batching by settlement date is also what makes this cheap: one call per
    date instead of one per trade.
    """
    if not prices:
        return {}
    payload = {"format": "FLDS", "data": [
        {"BOND_CD": i, "CLOSING PRICE": float(p), "WEIGHTING": 1.0,
         "Inventory Date": settle.replace("-", "/")}
        for i, p in prices.items()]}
    try:
        async with httpx.AsyncClient(timeout=120.0) as c:
            r = await c.post(f"{ga10_url.rstrip('/')}/api/v1/portfolio/analysis",
                             json=payload,
                             headers={"X-API-Key": api_key,
                                      "Content-Type": "application/json"})
        if r.status_code == 200:
            return {b["isin"]: b for b in (r.json().get("bond_data") or [])
                    if b.get("isin")}
    except Exception as e:
        logger.warning("static-validation: GA10 failed @ %s: %s", settle, e)
        if len(prices) == 1:
            return {}
        r = None

    # POISON BOND: the batch dies wholesale when ANY bond in it lacks
    # reference data, so one bad ISIN blanks the whole settlement date —
    # measured here as 31 of 60 trades going unchecked on two 500s. Split
    # and recurse so every priceable bond still gets a mark and only the
    # genuinely un-enrolled drop out. Same fix as aum_orchestrator._ga10_batch.
    if len(prices) == 1:
        logger.info("static-validation: GA10 cannot price %s @ %s",
                    next(iter(prices)), settle)
        return {}
    items = list(prices.items())
    mid = len(items) // 2
    left = await _ga10_accrued_batch(dict(items[:mid]), settle, api_key, ga10_url)
    right = await _ga10_accrued_batch(dict(items[mid:]), settle, api_key, ga10_url)
    return {**left, **right}


def _exception_for(isin: str, settle: str) -> str | None:
    return EXCEPTIONS.get(f"{isin}@{settle}") or EXCEPTIONS.get(isin)


async def validate(pid: str = "gdbft") -> dict:
    """Check every settled trade's accrued against static. Trades are gospel."""
    from auth_client import get_api_key, get_service_url
    try:
        trades = await _orca_trades(pid)
    except Exception as e:
        return {"status": "error", "error": f"could not fetch trades: {e}",
                "portfolio_id": pid}
    api_key = get_api_key("GA10_API_KEY", requester="recon-static-validation")
    if not api_key:
        return {"status": "error", "error": "GA10_API_KEY unavailable",
                "portfolio_id": pid}
    ga10_url = get_service_url("GA10_PRICING_URL", requester="recon-static-validation")
    if not ga10_url:
        return {"status": "error", "error": "GA10_PRICING_URL unavailable",
                "portfolio_id": pid}

    # One GA10 call per settlement date, not per trade.
    by_settle: dict[str, dict] = {}
    for t in trades:
        by_settle.setdefault(str(t["settlement_date"])[:10], {})[t["isin"]] = \
            float(t.get("price") or 100.0)
    marks: dict[str, dict] = {}
    for settle, prices in by_settle.items():
        got = await _ga10_accrued_batch(prices, settle, api_key, ga10_url)
        for isin, b in got.items():
            marks[f"{isin}@{settle}"] = b

    findings, excepted, unchecked, ok = [], [], [], []
    for t in trades:
        isin, settle = t["isin"], str(t["settlement_date"])[:10]
        trade_acc = float(t["accrued_interest"])
        a = marks.get(f"{isin}@{settle}")
        g = (a or {}).get("accrued_interest")
        row = {
            "isin": isin, "description": t.get("description"),
            "ticker": t.get("ticker"),
            "trade_date": t.get("transaction_date"),
            "settlement_date": settle,
            "par": t.get("par_amount"),
            "trade_accrued_per100": round(trade_acc, 6),
            "our_accrued_per100": None if g is None else round(g, 6),
            "our_accrued_days": (a or {}).get("accrued_days"),
            "coupon": (a or {}).get("coupon"),
        }
        if g is None:
            row["reason"] = "no GA10 mark at this settlement date"
            unchecked.append(row)
            continue
        diff = g - trade_acc
        row["diff_per100"] = round(diff, 6)
        # Cash impact on THIS trade's par, so the finding has a size.
        if t.get("par_amount"):
            row["diff_local"] = round(diff * float(t["par_amount"]) / 100.0, 2)
        exc = _exception_for(isin, settle)
        if abs(diff) <= _TOL_PER100:
            ok.append(row)
        elif exc:
            row["exception_reason"] = exc
            excepted.append(row)
        else:
            defect, why = _classify(trade_acc, g)
            row["defect"] = defect
            row["explanation"] = why
            row["ratio"] = round(g / trade_acc, 4) if trade_acc else None
            findings.append(row)

    findings.sort(key=lambda r: -abs(r.get("diff_per100") or 0))
    by_bond: dict[str, int] = {}
    for f in findings:
        by_bond[f["isin"]] = by_bond.get(f["isin"], 0) + 1

    return {
        "status": "ok",
        "portfolio_id": pid,
        "basis": "Settled-trade confirms are the authority: a difference here "
                 "is a defect in OUR static, not in the trade. Findings "
                 "correct static going forward only — settled values are "
                 "frozen and are never re-derived.",
        "trades_checked": len(ok) + len(findings) + len(excepted),
        "trades_unchecked": len(unchecked),
        "agree": len(ok),
        "findings": findings,
        "finding_count": len(findings),
        "bonds_affected": sorted(by_bond),
        "excepted": excepted,
        "unchecked": unchecked,
        "coverage_note": "Only bonds we have actually traded are tested, and "
                         "only as at each trade's settlement date. A clean "
                         "result means no contradiction was found among the "
                         "trades we hold — not that the static is correct.",
    }


def probe(result: dict) -> dict:
    """/ops/probes entry. Amber on any finding — a static defect silently
    misprices every valuation until fixed — red when it fails to run at all,
    because an unrun check is indistinguishable from a passing one."""
    if result.get("status") != "ok":
        return {"id": "static_vs_trades", "status": "red", "value": None,
                "expected": "check runs", "detail": result.get("error", "did not run")}
    n = result.get("finding_count", 0)
    bonds = result.get("bonds_affected") or []
    return {
        "id": "static_vs_trades",
        "status": "green" if n == 0 else "amber",
        "value": n,
        "expected": "0 trades disagreeing with static",
        "detail": ("{} trade(s) disagree with our static across {} bond(s): {}. "
                   "Trades are the authority — the static is wrong."
                   .format(n, len(bonds), ", ".join(bonds))
                   if n else
                   "All {} checked trade(s) reconcile to our static."
                   .format(result.get("agree", 0))),
    }
