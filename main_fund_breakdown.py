"""Main-fund AUM on GA10 accrual, with every component as a % of it.

WHAT THIS ANSWERS. The administrator's NAV is the fund's total net assets and
includes the share-class hedging overlay. The question this section answers is
different: what is the MAIN fund worth on its own investment activity, valued
on our own accrual rather than the administrator's booking?

    main fund AUM = Waystone NAV
                  - share-class hedge P&L        (overlay belongs to classes)
                  with the per-bond accrued swapped from Waystone's to GA10's

Every component is then expressed as a percentage of that main-fund figure, so
the reader sees the shape of the fund rather than a column of absolutes.

FUND-LEVEL HEDGING IS DELIBERATELY RETAINED. It is a portfolio currency
decision hedging the fund's own non-base bond exposure, so it belongs in the
investment result like any other position. Only the SHARE-CLASS overlay is
stripped — it is run for particular classes and its result belongs to them.

THREE TRAPS THIS MODULE EXISTS TO AVOID
---------------------------------------

1. THE DECLARED-BUT-UNPAID INCOME MUST SURVIVE THE SWAP. Waystone's
   balance-sheet Accrued Income is NOT the sum of its per-bond accrued:

       balance sheet 191,912.05  =  per-bond sum 159,094.28
                                  + declared-but-unpaid 32,817.77   (GDBF 31-Jul)

   The gap is income declared on holdings but not yet paid, which belongs to no
   bond's accrual. GA10 computes BOND accrual only. Replacing the whole
   balance-sheet figure with a GA10 total silently deletes that 32,817.77 from
   NAV. So only the per-bond component is swapped and the declared-but-unpaid
   balance is carried through untouched.

2. COVERAGE. GA10 marks 22 of 28 GDBF bonds. Substituting a 22-bond GA10 total
   for a 28-bond Waystone total understates accrued by the six it never
   priced. Each bond is therefore swapped INDIVIDUALLY where GA10 has a mark
   and RETAINED at Waystone's figure where it does not, with the retained
   value stated so a partial restatement can never read as a full one.

3. FX AND PER-UNIT SECURITIES. GA10's accrued is per 100 of par in LOCAL
   currency; Waystone's is in base. Rather than re-deriving an FX rate (and
   tripping over per-unit securities like the CCDS, whose "par" is units), each
   bond's base-currency accrued is scaled by the RATIO of the two per-100
   figures:

       ga10_accrued_base = waystone_accrued_base x (ga10_per100 / waystone_per100)

   FX and unit scaling are identical in numerator and denominator, so both drop
   out exactly. No rate is ever inferred from prices.

CASH IS CHECKED, NOT ASSUMED TO TALLY. Maia's cash includes unsettled FX
forward legs; the administrator's Cash line does not (its forwards sit in
receivables/payables). The two are therefore NOT like-for-like by construction,
and this module states the difference and says so rather than reporting a tick.
"""

from __future__ import annotations

# Accrual basis for the swap. C+1 is the valuation convention (see
# accrual_convention_policy): at a calendar month end C+1 already IS the first
# day of the next month, which is the Bloomberg index basis.
_GA10_BASIS = "accrued_per_100_c1"


def _pct(v, base):
    if v is None or not base:
        return None
    return round(v / base * 100.0, 3)


def build(parsed: dict, athena_marks: dict, waystone: dict,
          maia: dict | None, accrued_rows: list[dict] | None = None) -> dict:
    """Main-fund breakdown on GA10 accrual.

    parsed        administrator pack (holdings carry par/accrued_income).
    athena_marks  {isin: {accrued_per_100, accrued_per_100_c1, ...}} from GA10.
    waystone      nav_comparison.waystone_side(parsed).
    maia          nav_comparison.maia_side(...) or None.
    accrued_rows  _accrued_recon rows, for the per-100 figures already computed.
    """
    holdings = [h for h in (parsed.get("holdings") or []) if h.get("isin")]
    per100 = {r["isin"]: r for r in (accrued_rows or []) if r.get("isin")}

    rows = []
    ga10_total = way_per_bond_total = 0.0
    retained_value = 0.0
    swapped = retained = 0
    for h in holdings:
        isin = h["isin"]
        w_base = h.get("accrued_income")
        if w_base is None:
            continue
        way_per_bond_total += w_base
        r = per100.get(isin) or {}
        w100 = r.get("waystone_per100")
        g100 = (athena_marks.get(isin) or {}).get(_GA10_BASIS)
        if g100 is None:
            g100 = r.get("ga10_c1_per100")

        if g100 is not None and w100:
            # Ratio scaling: FX and per-unit scaling cancel exactly.
            g_base = w_base * (g100 / w100)
            ga10_total += g_base
            swapped += 1
            src = "ga10_c1"
        else:
            # No GA10 mark (or no Waystone per-100 to scale from) — retain the
            # administrator's figure rather than drop the bond.
            g_base = w_base
            ga10_total += g_base
            retained_value += w_base
            retained += 1
            src = "waystone_retained"
        rows.append({
            "isin": isin,
            "description": h.get("description"),
            "currency": h.get("currency"),
            "par": h.get("par_amount") or h.get("face_value"),
            "waystone_per100": w100,
            "ga10_c1_per100": g100,
            "waystone_accrued_base": round(w_base, 2),
            "ga10_accrued_base": round(g_base, 2),
            "diff_base": round(g_base - w_base, 2),
            "source": src,
        })
    rows.sort(key=lambda x: -abs(x["diff_base"]))

    # ── The declared-but-unpaid balance must survive the swap ──────────────
    bs_accrued = waystone.get("accrued_income")
    declared_unpaid = (None if bs_accrued is None
                       else round(bs_accrued - way_per_bond_total, 2))

    restated_accrued = (None if declared_unpaid is None
                        else round(ga10_total + declared_unpaid, 2))

    total_nav = waystone.get("total")
    sc_hedge = waystone.get("fx_forward_pnl_share_class")
    accrued_delta = (None if bs_accrued is None or restated_accrued is None
                     else round(restated_accrued - bs_accrued, 2))

    restated_nav = (None if (total_nav is None or accrued_delta is None)
                    else round(total_nav + accrued_delta, 2))
    main_fund = (None if (restated_nav is None or sc_hedge is None)
                 else round(restated_nav - sc_hedge, 2))

    components = [
        ("clean_bond_mv", "Clean bond MV", waystone.get("clean_bond_mv")),
        ("accrued_income", "Accrued income (GA10 C+1, per bond)",
         round(ga10_total, 2)),
        ("declared_unpaid_income", "Declared but unpaid income", declared_unpaid),
        ("cash", "Cash", waystone.get("cash")),
        ("fx_forward_pnl_fund", "FX forward P&L — fund hedges",
         waystone.get("fx_forward_pnl_fund")),
        ("other_net", "Other (net receivables/payables)", waystone.get("other_net")),
    ]
    breakdown = [{"key": k, "label": lbl, "value": v,
                  "pct_of_main_fund": _pct(v, main_fund)}
                 for k, lbl, v in components]
    total_pct = sum(b["pct_of_main_fund"] or 0.0 for b in breakdown)

    # ── Cash check against Maia. NOT like-for-like — stated, never ticked ──
    m_cash = (maia or {}).get("cash")
    w_cash = waystone.get("cash")
    cash_check = {
        "maia": m_cash,
        "waystone": w_cash,
        "difference": (None if (m_cash is None or w_cash is None)
                       else round(m_cash - w_cash, 2)),
        "like_for_like": False,
        "note": "Maia's cash includes unsettled FX forward legs; the "
                "administrator's Cash line does not (its forwards sit in "
                "receivables/payables). A non-zero difference is expected by "
                "construction — read it against the forward P&L rows, not as "
                "a break.",
    }
    if cash_check["difference"] is not None and w_cash:
        cash_check["difference_pct_of_cash"] = round(
            cash_check["difference"] / w_cash * 100.0, 3)

    return {
        "basis": "Administrator NAV with the share-class hedging overlay "
                 "removed and the PER-BOND accrued restated from the "
                 "administrator's booking to GA10's C+1 calculation. "
                 "Declared-but-unpaid income is carried through unchanged — "
                 "GA10 computes bond accrual only. Fund-level hedging is "
                 "retained: it is a portfolio decision, not a class overlay.",
        "waystone_total_nav": total_nav,
        "share_class_hedge_pnl": sc_hedge,
        "accrued": {
            "waystone_balance_sheet": bs_accrued,
            "waystone_per_bond_sum": round(way_per_bond_total, 2),
            "declared_unpaid_income": declared_unpaid,
            "ga10_c1_per_bond": round(ga10_total, 2),
            "restated_total": restated_accrued,
            "delta_vs_waystone": accrued_delta,
            "coverage": f"{swapped}/{swapped + retained}",
            "bonds_swapped": swapped,
            "bonds_retained_at_waystone": retained,
            "value_retained_at_waystone": round(retained_value, 2),
            "rows": rows,
        },
        "restated_nav": restated_nav,
        "main_fund_aum": main_fund,
        "breakdown": breakdown,
        "breakdown_pct_total": round(total_pct, 3),
        "cash_check": cash_check,
        "caveats": [
            "Main fund AUM is a MEMO measure for judging investment "
            "performance — it is NOT the fund's NAV, which necessarily "
            "includes the share-class forwards.",
        ] + ([] if retained == 0 else [
            "{} bond(s) worth {:,.2f} of accrued are NOT restated — GA10 has "
            "no mark for them, so the administrator's figure is retained. The "
            "restatement is partial and must not be read as a full one."
            .format(retained, retained_value)]),
    }
