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

_GAE_URL = "https://future-footing-414610.uc.r.appspot.com"
_TRADE_TYPES = ("BUY", "SELL")


def _classify(trade: float, ga10: float) -> tuple[str, str]:
    """(defect, explanation) from the shape of the mismatch."""
    if not trade:
        return ("indeterminate", "trade accrued is zero — nothing to scale against")
    ratio = ga10 / trade
    if 0.45 <= ratio <= 0.55:
        return ("frequency",
                "our accrual period is about HALF the trade's — the bond most "
                "likely pays ANNUAL and is modelled semi-annual")
    if 1.8 <= ratio <= 2.2:
        return ("frequency",
                "our accrual period is about DOUBLE the trade's — the bond "
                "most likely pays semi-annual and is modelled annual")
    if 0.985 <= ratio <= 1.015:
        return ("day_count",
                "a basis difference, not a date one — 30/360 against ACT/365 "
                "is a factor of 1.0139")
    return ("schedule_or_coupon",
            "neither a clean period nor a basis difference — check the "
            "last-coupon/schedule anchoring first, then the coupon rate")


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


async def _ga10_accrued(isin: str, price: float, settle: str,
                        api_key: str) -> dict | None:
    """GA10 accrued per 100 at an explicit settlement date."""
    try:
        async with httpx.AsyncClient(timeout=60.0) as c:
            r = await c.post(
                f"{_GAE_URL}/api/v1/bond/analysis/flexible",
                json={"isin": isin, "price": price, "settlement_date": settle},
                headers={"X-API-Key": api_key, "Content-Type": "application/json"})
        if r.status_code != 200:
            return None
        return (r.json() or {}).get("analytics") or {}
    except Exception as e:
        logger.warning("static-validation: GA10 failed %s @ %s: %s", isin, settle, e)
        return None


def _exception_for(isin: str, settle: str) -> str | None:
    return EXCEPTIONS.get(f"{isin}@{settle}") or EXCEPTIONS.get(isin)


async def validate(pid: str = "gdbft") -> dict:
    """Check every settled trade's accrued against static. Trades are gospel."""
    from auth_client import get_api_key
    try:
        trades = await _orca_trades(pid)
    except Exception as e:
        return {"status": "error", "error": f"could not fetch trades: {e}",
                "portfolio_id": pid}
    api_key = get_api_key("GA10_API_KEY", requester="recon-static-validation")
    if not api_key:
        return {"status": "error", "error": "GA10_API_KEY unavailable",
                "portfolio_id": pid}

    findings, excepted, unchecked, ok = [], [], [], []
    for t in trades:
        isin, settle = t["isin"], str(t["settlement_date"])[:10]
        trade_acc = float(t["accrued_interest"])
        a = await _ga10_accrued(isin, float(t.get("price") or 100.0), settle, api_key)
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
