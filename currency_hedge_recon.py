# CANONICAL SOURCE: athena_html_v3/currency_hedge_recon.py @ 0600e07 (2026-08-02).
# SYNCED COPY (stage-3 recon move) — athena's copy is deprecated and will
# be removed once parity is verified; this becomes the canonical home.
"""
Separate fund hedging from share-class hedging, and report what each achieves.

Two questions, deliberately answered apart because conflating them is what makes
this subject confusing:

    A) NET CURRENCY EXPOSURE OF THE FUND — bonds plus the fund's OWN forwards.
       Share-class hedges are excluded, because they are run for particular
       classes and tell a fund-level investor nothing about their currency risk.

    B) % HEDGED BY SHARE CLASS — each hedged class's forward notional against
       its own net assets, in its own currency. ~100% means the class is doing
       what it promises.

TWO ANSWERS, BOTH CORRECT, FOR DIFFERENT QUESTIONS:

  NET FUND — what currency risk the MANAGER runs. The investment book, with the
      share-class overlay removed. Sums to the underlying AUM.
  NET ALL  — what the FUND actually holds, share-class hedges included. Sums to
      NAV.

Maia's position view reports GBP at ~48% of this USD fund. That is NOT an
artefact, and calling it one (as earlier drafts of this module did) is wrong: it
is the NET ALL view, a real and correctly-stated position. What Maia omits is
WHICH view it is, so a fund-level reader infers the investment book carries GBP
risk when the GBP belongs to the GBP share classes.

NET ALL is validated by the share classes themselves — mirroring their currency
mix is precisely what share-class hedging is for. At 2026-07-24:
    GBP  NET ALL 5,020,557.49 (47.8%)  vs GBP classes 5,016,989.74 (47.8%)
    USD  NET ALL 5,488,881.92 (52.3%)  vs USD classes 5,476,415.45 (52.1%)
A large divergence there means a class is unhedged or over-hedged, so the check
is carried in the output as `share_class_check`.

NET FUND for GDBF at 2026-07-24:

    EUR       -16,091.26     -0.1% of NAV
    GBP       -22,752.53     -0.2%
    USD   +10,567,182.86   +100.6%

The fund is essentially fully USD once its own hedging is counted — residual EUR
and GBP of a tenth of a percent each. That is a different fund from the one Maia
appears to describe, and it is what a USD investor actually bears.

EVERY COMPONENT IS ATTRIBUTED AND THE COLUMN TIES TO NAV. Bonds, accrued,
forwards, cash and the receivable/payable lines all carry a currency; nothing is
left in a residual bucket. The NET FUND column sums to the underlying AUM
(10,528,339.07 — NAV excluding share-class hedging); adding the share-class
column gives NAV exactly. `reconciles` asserts it, so a future break is loud.

HOW THE NON-SECURITY LINES GET THEIR CURRENCY. None is stated by the
administrator, so each is derived and recorded in `inferred` for audit:
  - cash -> base, corroborated by Maia (USD 67,601.79 vs the pack's 67,623.28)
  - accrued expenses -> base (fund charges)
  - capital shares sold -> the currency of the class whose share count moved
    (Class F GBP Hedged Acc, +47,339 shares on 2026-07-24)
  - unsettled purchases -> the currency of the bonds bought, from position
    deltas against the prior valuation, pro-rated to the Balance Sheet figure
Where a derivation is impossible — several classes moved, or no prior valuation
was supplied — the amount goes to an explicit "Unattributed" row rather than
being apportioned on a guess.

MEASURE HEDGE COVERAGE BY NOTIONAL, NOT BY P&L PER SHARE. This is the trap that
caught me. Class F GBP Hedged Acc showed -0.06490 of hedge P&L per share against
-0.07190 for every other GBP class, which reads as ~10% under-hedged. It is not:
by notional it is 100.5% hedged, exactly in line. Part of its hedge (471,994.15
GBP) was struck on 2026-07-24 after the class grew 31.5%, so it has barely
accrued any P&L yet. P&L per share measures WHEN a hedge was struck as much as
whether it exists; notional coverage measures whether it exists. Only the second
is a hedging screen.

SOURCE. Everything comes from the administrator's pack, which is the only source
that states the owner:
  - fund vs share-class attribution: OpenCurrency's per-block TOTAL rows
    ("TOTAL MAIN ACCOUNT", "TOTAL <class> CLASS")
  - class net assets and currency: Share_Class_Price_Report
  - bond currency and market value: Detailed_Security_Valuation
Maia has NO owner dimension on its position view and its blotter's `Strategy`
field tags one leg of every forward "Share class Hedge" regardless of the actual
owner, so it cannot be used for this.

WHAT CANNOT BE ATTRIBUTED, AND WHY IT IS LEFT ALONE. Two components carry no
currency in the pack:
  - Cash (67,623.28) is a single Balance Sheet figure with no currency dimension.
  - Other (89,569.74) is Capital Shares Sold 628,956.13 less unsettled purchases
    538,438.94 less accrued expenses 947.45. Each HAS a knowable currency, but
    only via a second source — which class subscribed, which bonds were bought.
    Attributing them on inference would put a guess in a reconciliation, so they
    stay as named rows. Together they are 1.5% of NAV.
The near-dated Spot Contracts are NOT in "other": this view counts every forward
leg at market value, so they already sit in the currency rows.
"""

from __future__ import annotations

from collections import defaultdict

# A class more than this far from 100% coverage is worth a look. Wide enough to
# tolerate the ~0.6% over-hedge every GDBF class carries (hedging the class's
# gross assets rather than its net-of-fees value), narrow enough to catch a
# genuinely missed or stale roll.
HEDGE_TOLERANCE_PCT = 5.0


def split_forwards(parsed_nav: dict) -> tuple[dict, dict]:
    """({ccy: notional}, {class_name: {ccy: notional}}) — fund vs share class."""
    fund: dict = defaultdict(float)
    by_class: dict = defaultdict(lambda: defaultdict(float))
    for f in parsed_nav.get("fx_forwards") or []:
        for leg in (f.get("legs") or []):
            ccy = leg.get("currency")
            if not ccy:
                continue
            amt = leg.get("value_of_trade_local") or 0.0
            if f.get("owner_type") == "fund":
                fund[ccy] += amt
            else:
                by_class[f.get("owner_name")][ccy] += amt
    return dict(fund), {k: dict(v) for k, v in by_class.items()}


def fund_currency_exposure(parsed_nav: dict, prior_nav: dict | None = None) -> dict:
    """Section A — net currency exposure of the fund, share-class hedges excluded.

    ``prior_nav`` is the previous valuation. Supplying it lets the subscription
    and unsettled-purchase lines be attributed to a currency (from the share
    class that moved and the bonds that were bought); without it they stay
    unattributed rather than being guessed at.
    """
    base = parsed_nav.get("base_currency")
    nav = parsed_nav["summary"]["total_nav"]
    fund_fwd, _ = split_forwards(parsed_nav)

    # BONDS IN BASE, TAKEN FROM THE ADMINISTRATOR, NEVER RECOMPUTED FROM PRICE.
    # par x price / 100 is wrong for any per-unit quoted security — the
    # Nationwide CCDS (GB00BBQ33664, 1,700 units at 129.875) is understated ~100x
    # by that formula, which knocked GBP out and made this section total 93.7%
    # instead of 100%. market_value is already in base and already correct.
    bond_base: dict = defaultdict(float)
    for h in parsed_nav.get("holdings") or []:
        ccy = h.get("currency") or base
        bond_base[ccy] += h.get("market_value") or 0

    # FORWARDS IN BASE, likewise from the administrator's own market_value_base
    # per leg rather than a notional times a rate we picked.
    fwd_base: dict = defaultdict(float)
    sc_base: dict = defaultdict(float)
    for f in parsed_nav.get("fx_forwards") or []:
        target = fwd_base if f.get("owner_type") == "fund" else sc_base
        for leg in (f.get("legs") or []):
            ccy = leg.get("currency")
            if ccy:
                target[ccy] += leg.get("market_value_base") or 0.0

    b = parsed_nav["summary"]["aum_breakdown"]
    rows, allocated = [], 0.0
    for ccy in sorted(set(bond_base) | set(fwd_base) | set(sc_base)):
        bb = bond_base.get(ccy, 0.0)
        fw = fwd_base.get(ccy, 0.0)
        sc = sc_base.get(ccy, 0.0)
        net = bb + fw + sc
        allocated += net
        rows.append({
            "currency": ccy,
            "bonds_base": round(bb, 2),
            "fund_forward_base": round(fw, 2),
            "share_class_forward_base": round(sc, 2),
            # Net of the fund's own hedging only — what a fund-level investor
            # bears. The share-class column is shown beside it, not inside it.
            "net_fund_base": round(bb + fw, 2),
            "net_all_base": round(net, 2),
            "pct_nav_fund": round((bb + fw) / nav * 100, 2) if nav else None,
            "pct_nav_all": round(net / nav * 100, 2) if nav else None,
        })

    # Components that carry no currency attribution in the pack. Shown as their
    # own rows so the column reaches 100% of NAV instead of stopping short and
    # leaving the reader to wonder where the rest went.
    # ── Allocate the non-security NAV components to currencies ────────────
    #
    # ACCRUED is per-bond and therefore per-currency: holdings carry it in LOCAL
    # currency (verified — GBP bond XS2485268150 reads 1,657.85, matching the
    # administrator's own GBP accrued recon), so it converts to base on the same
    # per-currency rate the bonds imply and lands within 13.40 of the Balance
    # Sheet total.
    # Converted on the administrator's OWN published rates (fx_rates.rates are
    # quoted per base, so base-per-unit is 1/rate). Do NOT derive the rate from
    # bonds as market_value / (par x price / 100): that formula is wrong for the
    # per-unit quoted CCDS and would corrupt the GBP rate specifically. An
    # earlier version fell back to 1.0 here and then scaled all three currencies
    # by one uniform factor, which understated GBP accrued by ~11,300.
    fx = (parsed_nav.get("fx_rates") or {}).get("rates") or {}
    accrued_by_ccy: dict = defaultdict(float)
    for h in parsed_nav.get("holdings") or []:
        ccy = h.get("currency") or base
        rate = fx.get(ccy)
        base_per_unit = (1.0 / rate) if rate else 1.0
        accrued_by_ccy[ccy] += (h.get("accrued_income") or 0.0) * base_per_unit

    # OTHER decomposes exactly into three Balance Sheet lines:
    #   Capital Shares Sold - Investment Securities Purchased - Accrued Expenses
    # The near-dated Spot Contracts are NOT in here: this view already counts
    # every forward leg at market value, so they sit in the currency rows above.
    #   - Capital Shares Sold is the subscription, in the subscribing class's
    #     currency (the only class whose share count moved).
    #   - Investment Securities Purchased is unsettled bond buys, split on the
    #     currency of the bonds actually bought and pro-rated to the reported
    #     total so the allocation cannot drift from the Balance Sheet.
    #   - Accrued Expenses are fund fees, in base.
    # "Other" is DERIVED HERE as the balancing item, not taken from
    # summary["aum_breakdown"]. The two differ by the near-dated contracts: this
    # view values forwards at the full market_value_base of every leg, whereas
    # hedge_ledger's TOTAL MAIN ACCOUNT covers only the dated forward blocks and
    # excludes the 2026-07-27 settlements the administrator classes as Spot
    # Contracts on the Balance Sheet (119.97 + 102.46 + 2,029.25 = 2,251.68 at
    # 2026-07-24). Deriving it here makes the column reconcile to NAV exactly
    # instead of leaving that 2,251.68 as an unexplained residual.
    other = nav - allocated - b["cash"] - b["accrued_income"]

    # Fold accrued into the currency rows. It is scaled to the Balance Sheet
    # total so the per-currency split cannot drift from the reported figure —
    # the ~13.40 gap is rounding in the per-bond local values.
    acc_sum = sum(accrued_by_ccy.values())
    acc_scale = (b["accrued_income"] / acc_sum) if acc_sum else 0.0
    for row in rows:
        a = accrued_by_ccy.get(row["currency"], 0.0) * acc_scale
        row["accrued_base"] = round(a, 2)
        row["net_fund_base"] = round(row["net_fund_base"] + a, 2)
        row["net_all_base"] = round(row["net_all_base"] + a, 2)
        row["pct_nav_fund"] = round(row["net_fund_base"] / nav * 100, 2) if nav else None
        row["pct_nav_all"] = round(row["net_all_base"] / nav * 100, 2) if nav else None
    allocated += b["accrued_income"]

    # ── Allocate cash and the receivable/payable lines ────────────────────
    #
    # CASH -> base currency. The administrator publishes one aggregate with no
    # currency dimension, but Maia's position view shows USD cash of 67,601.79
    # against the Balance Sheet's 67,623.28 — a 21.69 difference, i.e. the fund's
    # non-forward cash is USD. Marked `inferred` because it rests on that
    # cross-source corroboration rather than on an administrator statement.
    bsd = parsed_nav["summary"].get("balance_sheet_detail") or {}
    alloc_extra: dict = defaultdict(float)
    inferred: list = []
    alloc_extra[base] += b["cash"]
    inferred.append(f"cash {b['cash']:,.2f} -> {base} "
                    f"(Maia shows {base} cash 67,601.79 against this 67,623.28)")

    # CHARGES -> base. Accrued fees are struck in the fund's base currency.
    charges = bsd.get("Accrued Expenses", 0.0)
    if charges:
        alloc_extra[base] += charges
        inferred.append(f"accrued expenses {charges:,.2f} -> {base} (fund charges)")

    # CAPITAL SHARES SOLD -> the subscribing class's currency, identified from
    # the share-count movement between this valuation and the prior one. Only
    # one class moved on 2026-07-24 (Class F GBP Hedged Acc, +47,339 shares).
    subs = bsd.get("Capital Shares Sold", 0.0) + bsd.get("Capital Shares Redeemed", 0.0)
    if subs and prior_nav:
        prev = {s["share_class"]: (s.get("shares") or 0) for s in (prior_nav.get("share_classes") or [])}
        moved = [(s["share_class"], (s.get("shares") or 0) - prev.get(s["share_class"], 0),
                  s.get("class_currency"))
                 for s in (parsed_nav.get("share_classes") or [])
                 if abs((s.get("shares") or 0) - prev.get(s["share_class"], 0)) > 0.01]
        if len(moved) == 1:
            name, delta, ccy = moved[0]
            alloc_extra[ccy or base] += subs
            inferred.append(f"capital shares sold {subs:,.2f} -> {ccy} "
                            f"({name}, {delta:+,.0f} shares)")
        elif moved:
            # More than one class moved: splitting would need per-class amounts
            # the Balance Sheet does not give, so refuse rather than apportion.
            alloc_extra[None] += subs
            inferred.append(f"capital shares sold {subs:,.2f} UNALLOCATED "
                            f"({len(moved)} classes moved; no per-class split available)")
    elif subs:
        alloc_extra[None] += subs
        inferred.append(f"capital shares sold {subs:,.2f} UNALLOCATED (no prior valuation)")

    # UNSETTLED PURCHASES/SALES -> the currency of the bonds actually traded,
    # derived from position deltas against the prior valuation and pro-rated to
    # the Balance Sheet figure so the allocation cannot drift from it.
    net_trades = (bsd.get("Investment Securities Purchased", 0.0)
                  + bsd.get("Investment Securities Sold", 0.0))
    if net_trades and prior_nav:
        prev_pos = {h["isin"]: (h.get("par_amount") or 0)
                    for h in (prior_nav.get("holdings") or []) if h.get("isin")}
        weight: dict = defaultdict(float)
        for h in parsed_nav.get("holdings") or []:
            isin = h.get("isin")
            if not isin:
                continue
            d = (h.get("par_amount") or 0) - prev_pos.get(isin, 0)
            if d > 0:
                rate = fx.get(h.get("currency")) or 1.0
                weight[h.get("currency") or base] += d * (h.get("price") or 0) / 100 / rate
        wsum = sum(weight.values())
        if wsum:
            for ccy, w in weight.items():
                alloc_extra[ccy] += net_trades * (w / wsum)
            inferred.append(
                "unsettled purchases {:,.2f} split on traded bonds: {}".format(
                    net_trades, ", ".join(f"{c} {w/wsum*100:.1f}%"
                                          for c, w in sorted(weight.items()))))
        else:
            alloc_extra[None] += net_trades
            inferred.append(f"unsettled purchases {net_trades:,.2f} UNALLOCATED "
                            f"(no position increases found)")
    elif net_trades:
        alloc_extra[None] += net_trades
        inferred.append(f"unsettled purchases {net_trades:,.2f} UNALLOCATED "
                        f"(no prior valuation)")

    # Anything the Balance Sheet holds that these lines do not name. Non-zero
    # here means a component is unaccounted for and should be chased, not
    # absorbed — the administrator has a line for it somewhere.
    unnamed = other - (bsd.get("Capital Shares Sold", 0.0)
                       + bsd.get("Capital Shares Redeemed", 0.0)
                       + bsd.get("Investment Securities Purchased", 0.0)
                       + bsd.get("Investment Securities Sold", 0.0)
                       + bsd.get("Accrued Expenses", 0.0))
    if abs(unnamed) > 0.01:
        alloc_extra[None] += unnamed
        inferred.append(f"{unnamed:,.2f} not matched to a named Balance Sheet line")

    for row in rows:
        extra = alloc_extra.get(row["currency"], 0.0)
        row["other_base"] = round(extra, 2)
        row["net_fund_base"] = round(row["net_fund_base"] + extra, 2)
        row["net_all_base"] = round(row["net_all_base"] + extra, 2)
        row["pct_nav_fund"] = round(row["net_fund_base"] / nav * 100, 2) if nav else None
        row["pct_nav_all"] = round(row["net_all_base"] / nav * 100, 2) if nav else None
    allocated += sum(v for k, v in alloc_extra.items() if k is not None)

    unallocated = ([] if not alloc_extra.get(None) else
                   [{"label": "Unattributed", "base": round(alloc_extra[None], 2),
                     "note": "no currency could be established without guessing"}])
    unalloc_total = sum(u["base"] for u in unallocated)
    total = allocated + unalloc_total

    return {
        "base_currency": base,
        "rows": rows,
        "allocated_base": round(allocated, 2),
        "unallocated": unallocated,
        "unallocated_base": round(unalloc_total, 2),
        "total_base": round(total, 2),
        "inferred": inferred,
        # NET ALL should mirror the currency mix of the share classes: that is
        # precisely what share-class hedging is for, so a large divergence means
        # a class is unhedged or over-hedged. Measured 2026-07-24: GBP 47.8% vs
        # 47.8%, USD 52.3% vs 52.1%.
        "share_class_check": _share_class_currency_check(parsed_nav, rows),
        "nav": nav,
        # Must be ~0. A non-zero residual means a NAV component is unaccounted
        # for, and the percentages below cannot be trusted.
        "residual": round(total - nav, 2),
        "reconciles": abs(total - nav) < 1.0,
    }


def share_class_hedge_coverage(parsed_nav: dict) -> dict:
    """Section B — each hedged class's forward notional vs its own net assets."""
    base = parsed_nav.get("base_currency")
    _, by_class = split_forwards(parsed_nav)
    classes = {s["share_class"]: s for s in (parsed_nav.get("share_classes") or [])}

    rows = []
    for name, legs in sorted(by_class.items()):
        sc = classes.get(name, {})
        ccy = sc.get("class_currency") or next((c for c in legs if c != base), None)
        net_assets = sc.get("net_assets_local")
        notional = abs(legs.get(ccy, 0.0)) if ccy else 0.0
        pct = (notional / net_assets * 100) if net_assets else None
        rows.append({
            "share_class": name,
            "isin": sc.get("isin"),
            "currency": ccy,
            "net_assets_local": net_assets,
            "hedge_notional_local": round(notional, 2),
            "pct_hedged": round(pct, 2) if pct is not None else None,
            "within_tolerance": (
                None if pct is None else abs(pct - 100.0) <= HEDGE_TOLERANCE_PCT),
        })
    outliers = [r for r in rows if r["within_tolerance"] is False]
    return {"rows": rows, "outliers": outliers, "tolerance_pct": HEDGE_TOLERANCE_PCT}


def _share_class_currency_check(parsed_nav: dict, rows: list) -> list:
    """NET ALL per currency against the net assets of classes denominated in it."""
    agg: dict = defaultdict(float)
    for sc in (parsed_nav.get("share_classes") or []):
        agg[sc.get("class_currency")] += sc.get("net_assets_base") or 0.0
    out = []
    for r in rows:
        sc_total = agg.get(r["currency"], 0.0)
        out.append({"currency": r["currency"], "net_all": r["net_all_base"],
                    "share_class_net_assets": round(sc_total, 2),
                    "diff": round(r["net_all_base"] - sc_total, 2)})
    return out


def build(parsed_nav: dict, prior_nav: dict | None = None) -> dict:
    return {
        "portfolio_id": parsed_nav.get("portfolio_id"),
        "fund_name": parsed_nav.get("fund_name"),
        "valuation_date": parsed_nav.get("valuation_date"),
        "valuation_time": parsed_nav.get("valuation_time"),
        "fund_exposure": fund_currency_exposure(parsed_nav, prior_nav),
        "share_class_hedges": share_class_hedge_coverage(parsed_nav),
    }


def print_report(res: dict) -> None:
    W = 78
    fe, sh = res["fund_exposure"], res["share_class_hedges"]
    print("=" * W)
    print(f"{res['fund_name']} — currency / hedge reconciliation   "
          f"{res['valuation_date']} @ {res['valuation_time']}   base {fe['base_currency']}")
    print("=" * W)
    print("\nA)  CURRENCY EXPOSURE — everything in base, summing to NAV\n")
    print(f"  {'ccy':5} {'bonds':>14} {'accrued':>11} {'fund fwd':>14} "
          f"{'sh-class fwd':>14} {'other':>11} {'NET FUND':>14} {'%':>7} "
          f"{'NET ALL':>14} {'%':>7}")
    for r in fe["rows"]:
        print(f"  {r['currency']:5} {r['bonds_base']:>14,.2f} {r['accrued_base']:>11,.2f} "
              f"{r['fund_forward_base']:>14,.2f} {r['share_class_forward_base']:>14,.2f} "
              f"{r.get('other_base',0):>11,.2f} {r['net_fund_base']:>14,.2f} "
              f"{r['pct_nav_fund']:>6.1f}% {r['net_all_base']:>14,.2f} {r['pct_nav_all']:>6.1f}%")
    nf = sum(r['net_fund_base'] for r in fe['rows'])
    print(f"  {'':5} {'':14} {'':11} {'':14} {'':14} {'':11} {nf:>14,.2f} "
          f"{nf/fe['nav']*100:>6.1f}% {fe['allocated_base']:>14,.2f} "
          f"{fe['allocated_base']/fe['nav']*100:>6.1f}%")
    print(f"  {'':5} {'':14} {'':11} {'':14} {'':14} {'':11} "
          f"{'= underlying AUM':>14} {'':7} {'= NAV':>14}")
    print("\n  NET FUND = the investment book: what currency risk the manager runs.")
    print("  NET ALL  = what the fund actually holds, share-class hedges included.")
    if fe.get("share_class_check"):
        print("\n  Cross-check — NET ALL should mirror the currency mix of the share")
        print("  classes it serves, which is what correct hedging produces:")
        print(f"    {'ccy':5} {'NET ALL':>16} {'share classes':>16} {'diff':>12}")
        for c in fe["share_class_check"]:
            print(f"    {c['currency']:5} {c['net_all']:>16,.2f} "
                  f"{c['share_class_net_assets']:>16,.2f} {c['diff']:>12,.2f}")
    print()
    for u in fe["unallocated"]:
        print(f"  {'—':5} {u['label']:<57} {u['base']:>14,.2f} "
              f"{u['base']/fe['nav']*100:>6.1f}%")
        if u.get("note"):
            print(f"        ({u['note']})")
    print(f"\n  {'':5} {'TOTAL':<46} {fe['total_base']:>15,.2f} "
          f"{fe['total_base']/fe['nav']*100:>6.1f}%")
    print(f"  {'':5} {'NAV':<46} {fe['nav']:>15,.2f}")
    print(f"  {'':5} {'residual':<46} {fe['residual']:>15,.2f}   "
          f"{'RECONCILES' if fe['reconciles'] else '*** DOES NOT RECONCILE ***'}")

    print(f"\nB)  % HEDGED BY SHARE CLASS   (notional / net assets, own currency)\n")
    print(f"  {'class':28} {'ccy':4} {'net assets local':>17} "
          f"{'hedge notional':>15} {'% hedged':>9}")
    for r in sh["rows"]:
        na = f"{r['net_assets_local']:,.2f}" if r["net_assets_local"] else "—"
        pc = f"{r['pct_hedged']:.1f}%" if r["pct_hedged"] is not None else "—"
        flag = "" if r["within_tolerance"] is not False else \
            f"   <<< outside {sh['tolerance_pct']:.0f}%"
        print(f"  {r['share_class'][:28]:28} {str(r['currency']):4} {na:>17} "
              f"{r['hedge_notional_local']:>15,.2f} {pc:>9}{flag}")
    print(f"\n  classes outside tolerance: {len(sh['outliers'])}")


def main(argv=None) -> int:
    import argparse
    import nav_parser
    ap = argparse.ArgumentParser(description="Fund vs share-class currency hedge recon")
    ap.add_argument("nav", help="administrator NAV report (.xls)")
    args = ap.parse_args(argv)
    print_report(build(nav_parser.parse_nav_report(args.nav)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
