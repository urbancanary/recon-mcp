"""Two-pass AUM comparison — separating POSITION mistakes from MARKS.

Pass 1 ("own")           each provider on its own prices and FX — the
                         comparison the fund has always shown.
Pass 2 ("admin_priced")  every side's POSITIONS (its own par per ISIN)
                         revalued at the administrator's per-bond price and
                         the administrator's FX. Prices are held constant and
                         par varies — the mirror of nav_comparison.
                         price_comparison, which holds par constant and
                         varies price. Any pass-2 difference is therefore a
                         position/completeness difference, never a mark.

FX RULE. Each bond's conversion to base uses the administrator's own embedded
factor, market_value / (par × price / 100) — that ratio IS Waystone's FX as
applied to that bond, so revaluing Waystone's own par reproduces its stated
clean bond MV to the cent (the identity check below asserts exactly that).
Where the factor cannot be formed (zero par/price) the pack's stated fx_rates
are the fallback. FX is NEVER derived by comparing prices across sources.

PER-UNIT SECURITIES. The same embedded factor detects them: a factor > 10
cannot be an FX rate, so the pack's "par" is UNITS and the factor bundles
price-per-unit scaling (GB00BBQ33664, the CCDS — naive par-ratio scaling
exploded it to +39.2m on 31-Jul). Maia exports units natively, so its par
ratio is already right; Athena's transactions book stores units×100, so an
injected Athena par is divided by 100 on flagged ISINs. Flagged ISINs and
any normalisation applied are carried in the payload, never silent.

ACCRUED — TWO ADMIN FIGURES EXIST. Waystone's Σ per-bond accrued does NOT
equal its balance-sheet Accrued Income line (31-Jul GDBF: 159,094.28 vs
191,912.05 — the gap is declared-but-unpaid income, which belongs to no
holding row). Pass 2 scales the PER-BOND accrued (Waystone's base-ccy accrued
× side par / Waystone par), so its identity target is the per-bond sum; the
balance-sheet figure and the declared-but-unpaid gap are stated alongside.

NULL, NEVER ZERO. Cash, forward P&L and receivables/payables are not
par-times-price positions, so they cannot be "revalued at admin marks" — each
side carries its own figure, and a side with no figure shows null. A zero
would claim the side says nil, which no file states.

COVERAGE IS VISIBLE, NOT FOLDED IN. A side that prices only part of the book
produces a smaller pass-2 total. That total is still shown, but the
shortfall is stated alongside in `coverage` — and side positions the
administrator has no mark for (the 31-Jul afternoon govvie switch: four
bonds, ~2.85m, in Maia's 14:51 export but not the 12:00 pack) go into an
explicit `unmatched_positions` bucket valued at the side's OWN figure —
never dropped, never guessed at admin marks it does not have.
"""

from __future__ import annotations

from nav_comparison import COMPONENTS

# The embedded mv/(par×px/100) factor is FX for a normal bond (order of 1);
# above this it cannot be an FX rate and the security is per-unit.
_PER_UNIT_FACTOR = 10.0


def athena_txn_to_side(txn: dict) -> dict:
    """nav_comparison side dict from Athena's transactions valuation payload
    (GET /api/internal/txn-valuation/{fund}, confirmed 2026-08-10):

        {date, source: "transactions",
         holdings: [{isin, par, currency, clean_price, accrued_per100,
                     clean_mv_base, accrued_base, fx_applied}],
         unpriced: [...], coverage: {...},
         totals: {holdings:  {clean_bond_mv, accrued_income, dirty_bond_mv},
                  valuation: {cash, fx_forward_pnl_fund, other_net, total},
                  nav:       {fx_forward_pnl_share_class, total}},
         caveats: [...]}

    Tier names follow the estate-wide definitions (backlog 2738): holdings =
    bonds only; valuation = + cash + fund hedges (underlying AUM); nav =
    + share-class hedge P&L. Coverage and caveats travel with the side so a
    shortfall is visible rather than folded into a smaller total.
    """
    totals = txn.get("totals") or {}
    t_hold = totals.get("holdings") or {}
    t_val = totals.get("valuation") or {}
    t_nav = totals.get("nav") or {}
    return {
        "clean_bond_mv": t_hold.get("clean_bond_mv"),
        "accrued_income": t_hold.get("accrued_income"),
        "cash": t_val.get("cash"),
        "fx_forward_pnl_fund": t_val.get("fx_forward_pnl_fund"),
        "fx_forward_pnl_share_class": t_nav.get("fx_forward_pnl_share_class"),
        "other_net": t_val.get("other_net"),
        "total": t_nav.get("total"),
        "source": "transactions",
        "coverage": txn.get("coverage"),
        "unpriced": txn.get("unpriced") or [],
        "caveats": txn.get("caveats") or [],
    }


def _fx_fallback(parsed: dict) -> dict:
    """{ccy: base-per-unit} from the pack's stated rates (the parser stores
    units-per-base)."""
    base = parsed.get("base_currency") or "USD"
    out = {base: 1.0, "USD": 1.0}
    for c, r in ((parsed.get("fx_rates") or {}).get("rates") or {}).items():
        if r:
            out[c] = 1.0 / r
    return out


def _row(key: str, label: str, m, a, w, not_lfl: bool) -> dict:
    return {
        "key": key, "label": label,
        "maia": m, "athena": a, "waystone": w,
        "diff_maia_athena": None if (m is None or a is None) else round(m - a, 2),
        "diff_athena_waystone": None if (a is None or w is None) else round(a - w, 2),
        "diff_maia_waystone": None if (m is None or w is None) else round(m - w, 2),
        "not_like_for_like": not_lfl,
    }


def admin_priced_pass(parsed: dict, sides: dict,
                      maia_bonds: dict | None,
                      athena_par: dict | None,
                      bonds_meta: dict | None = None) -> dict:
    """The pass-2 table: every side's par at Waystone's price and FX.

    parsed      the administrator pack (holdings carry par/price/mv/accrued).
    sides       {"maia": side|{}, "athena": side, "waystone": side} — pass-1
                dicts, used only for the non-repriced components.
    maia_bonds  {isin: {"par": ..., "own_value_base": ...}} from the Maia
                snapshot joined on ISIN via the reference-view bridge
                (bond_recon), or None when no usable Maia view/bridge
                exists. ISIN-only join — six 220,000-GBP GDBF bonds
                collapse under any fuzzier key.
    athena_par  {isin: par} from the injected transactions valuation, or
                None while the Athena side is derived from the admin book
                (then Athena par IS admin par by construction).
    bonds_meta  bond_recon.compare output, for bridge warnings.
    """
    holdings = [h for h in (parsed.get("holdings") or []) if h.get("isin")]
    fx_fb = _fx_fallback(parsed)
    maia_avail = maia_bonds is not None
    maia_bonds = maia_bonds or {}

    clean = {"maia": 0.0, "athena": 0.0, "waystone": 0.0}
    accrued = {"maia": 0.0, "athena": 0.0, "waystone": 0.0}
    matched = {"maia": 0, "athena": 0, "waystone": 0}
    unmatched_admin = {"maia": [], "athena": []}
    per_unit, ath_normalised, unpriceable = [], [], []

    for h in holdings:
        isin = h["isin"]
        par = h.get("par_amount") or h.get("face_value")
        px = h.get("price")
        mv = h.get("market_value")
        acc = h.get("accrued_income")
        if not par or not px:
            unpriceable.append(isin)
            continue
        fx_factor = (mv / (par * px / 100.0)) if mv else \
            fx_fb.get(h.get("currency") or "", 1.0)
        is_per_unit = fx_factor > _PER_UNIT_FACTOR
        if is_per_unit:
            per_unit.append(isin)

        m_par = (maia_bonds.get(isin) or {}).get("par") if maia_avail else None
        if athena_par is None:
            a_par = par
        else:
            a_par = athena_par.get(isin)
            if a_par is not None and is_per_unit:
                # Athena's transactions book stores units×100 for per-unit
                # securities; the pack's par is UNITS. Normalise, loudly.
                a_par = a_par / 100.0
                ath_normalised.append(isin)

        for side, sp in (("maia", m_par), ("athena", a_par), ("waystone", par)):
            if sp is None:
                unmatched_admin[side].append(isin)
                continue
            matched[side] += 1
            clean[side] += sp * px / 100.0 * fx_factor
            if acc is not None:
                accrued[side] += acc * sp / par

    admin_isins = {h["isin"] for h in holdings}
    unmatched_admin = {s: v for s, v in unmatched_admin.items() if s != "waystone"}

    # Side positions the administrator has no mark for (e.g. the afternoon
    # govvie switch) — valued at the side's OWN figure, in their own bucket.
    maia_only = [{"isin": i, "par": (maia_bonds[i] or {}).get("par"),
                  "own_value_base": (maia_bonds[i] or {}).get("own_value_base")}
                 for i in sorted(set(maia_bonds) - admin_isins)]
    athena_only = sorted(set(athena_par or ()) - admin_isins)
    unmatched_positions = {
        "maia": maia_only,
        "maia_value_at_own_marks": round(
            sum(p.get("own_value_base") or 0.0 for p in maia_only), 2),
        "athena": athena_only,
        "note": "Positions with no administrator mark cannot be revalued in "
                "this pass. They are shown at the side's own figure and are "
                "NOT in the table totals — never dropped, never guessed.",
    }

    ath = sides.get("athena") or {}
    way = sides.get("waystone") or {}
    mai = sides.get("maia") or {}

    p2 = {}
    for side, own in (("maia", mai), ("athena", ath), ("waystone", way)):
        avail = side != "maia" or maia_avail
        p2[side] = {
            "clean_bond_mv": round(clean[side], 2) if avail else None,
            "accrued_income": round(accrued[side], 2) if avail else None,
            # Not par×price positions — the side's own figure or null.
            "cash": own.get("cash"),
            "fx_forward_pnl_fund": own.get("fx_forward_pnl_fund"),
            "fx_forward_pnl_share_class": own.get("fx_forward_pnl_share_class"),
            "other_net": own.get("other_net"),
        }

    labels = dict(COMPONENTS)
    labels["accrued_income"] = ("Accrued income (per-bond; excludes the "
                                "administrator's declared-but-unpaid income)")
    rows = [_row(k, labels[k], p2["maia"][k], p2["athena"][k], p2["waystone"][k],
                 not_lfl=k in ("cash", "other_net"))
            for k, _ in COMPONENTS]

    totals, missing = {}, {}
    for side in ("maia", "athena", "waystone"):
        vals = {k: p2[side][k] for k, _ in COMPONENTS}
        missing[side] = [k for k, v in vals.items() if v is None]
        present = [v for v in vals.values() if v is not None]
        totals[side] = round(sum(present), 2) if present else None
    total = {
        "label": "TOTAL (components summed — see components_missing)",
        "maia": totals["maia"], "athena": totals["athena"],
        "waystone": totals["waystone"],
        "diff_maia_athena": None if (totals["maia"] is None or totals["athena"] is None)
        else round(totals["maia"] - totals["athena"], 2),
        "diff_athena_waystone": None if (totals["athena"] is None or totals["waystone"] is None)
        else round(totals["athena"] - totals["waystone"], 2),
        "diff_maia_waystone": None if (totals["maia"] is None or totals["waystone"] is None)
        else round(totals["maia"] - totals["waystone"], 2),
        "components_missing": missing,
    }

    # Identity check: Waystone's own par at Waystone's marks must reproduce
    # its STATED clean bond MV to the cent, and the per-bond accrued sum
    # (see module docstring for why the balance-sheet accrued is a different,
    # larger number). A mismatch means the revaluation arithmetic (or the
    # pack parse) is wrong — surfaced, never assumed.
    b = (parsed.get("summary") or {}).get("aum_breakdown") or {}
    stated_clean = b.get("clean_bond_mv")
    per_bond_acc = round(sum(h.get("accrued_income") or 0.0 for h in holdings), 2)
    bs_acc = b.get("accrued_income")
    identity = {
        "stated_clean_bond_mv": stated_clean,
        "recomputed_clean_bond_mv": round(clean["waystone"], 2),
        "clean_diff": None if stated_clean is None
        else round(clean["waystone"] - stated_clean, 2),
        "per_bond_accrued_sum": per_bond_acc,
        "recomputed_accrued": round(accrued["waystone"], 2),
        "accrued_diff": round(accrued["waystone"] - per_bond_acc, 2),
        "balance_sheet_accrued": bs_acc,
        "declared_unpaid_income_gap": None if bs_acc is None
        else round(bs_acc - per_bond_acc, 2),
        "holds": (stated_clean is not None
                  and abs(clean["waystone"] - stated_clean) < 0.005 + 1e-9
                  and abs(accrued["waystone"] - per_bond_acc) < 0.005 + 1e-9),
    }

    warnings = list((bonds_meta or {}).get("warnings") or [])
    if not maia_avail:
        warnings.append("No Maia par per ISIN (no usable Maia view or no "
                        "reference-view bridge) — the Maia column is null, "
                        "not zero.")
    if unpriceable:
        warnings.append(f"{len(unpriceable)} admin holding(s) carry no "
                        f"price/par and were excluded from every side: "
                        f"{unpriceable}")
    if ath_normalised:
        warnings.append(f"Athena par divided by 100 on per-unit securities "
                        f"{ath_normalised} (transactions book stores "
                        f"units×100; the pack's par is units).")

    return {
        "basis": "Each side's OWN par per ISIN at the administrator's price "
                 "and the administrator's FX (constant prices, varying par). "
                 "Differences here are position/completeness, never marks. "
                 "Cash/forwards/other are not repriceable positions — own "
                 "figure or null.",
        "rows": rows,
        "total": total,
        "coverage": {
            "admin_holdings": len(admin_isins),
            "matched": matched,
            "admin_isins_missing_from_side": unmatched_admin,
            "unresolved_tickers": (bonds_meta or {}).get("unresolved_tickers") or [],
            "unresolved_value": (bonds_meta or {}).get("unresolved_value"),
            "bridge_stale": (bonds_meta or {}).get("bridge_stale"),
        },
        "unmatched_positions": unmatched_positions,
        "per_unit_isins": per_unit,
        "identity_check": identity,
        "warnings": warnings,
    }
