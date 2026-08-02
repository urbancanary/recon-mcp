# CANONICAL SOURCE: athena_html_v3/nav_comparison.py @ 0600e07 (2026-08-02).
# SYNCED COPY (stage-3 recon move) — athena's copy is deprecated and will
# be removed once parity is verified; this becomes the canonical home.
"""
Three-way NAV comparison: Maia | Athena | Waystone, with diffs on the edges.

Assembles one row per AUM component from all three sources so they can be read
across, with Athena in the middle because it is the view being judged and the
other two are the independent checks either side of it.

    diff        Maia        ATHENA        Waystone        diff
  (M - A)                                                (A - W)

Components, in NAV order:

    clean bond MV
    accrued income
    cash
    FX forward P&L — FUND hedges
    FX forward P&L — SHARE-CLASS hedges     <- broken out deliberately
    other (net receivables/payables)
    = total NAV

WHY SHARE-CLASS HEDGING GETS ITS OWN LINE. It is run for one class and its P&L
belongs to that class, so it must never be read as the fund's investment result.
Waystone does not present it separately — its Balance Sheet splits forwards by
SIGN (Unrealized Gain 16,966.13 / Unrealized Loss -26,102.70), not by owner. The
owner split exists further inside the pack, on OpenCurrency's per-block TOTAL
rows ("TOTAL MAIN ACCOUNT", "TOTAL <class> CLASS"), and surfacing it is the main
thing Athena adds over reading the admin report directly.

To be clear about what the split is NOT: it does not remove anything from NAV.
The forwards are contracts of the fund and their value is part of what the fund
owns, in every one of these three sources. The split governs ATTRIBUTION — which
number belongs in fund-level performance and the main cash account, and which
gets allocated to the hedged classes.

HOW EACH SIDE IS BUILT

  Waystone — nav_parser summary["aum_breakdown"], read from the Balance Sheet,
      with the fund/share-class split taken from hedge_ledger (OpenCurrency).
      Nothing here is computed; every figure is a stated cell.

  Athena — currently DERIVED FROM the Waystone valuation for GDBF, so the
      Athena/Waystone diff is structurally zero rather than a validated match.
      This is stated on the page rather than left to be inferred: a column of
      zeroes that looks like agreement but is really identity would be worse
      than no column. It stops being identity once Athena prices the book
      independently (GA10/QuantLib) instead of ingesting admin prices.

  Maia — aum_recon.maia_breakdown off the position view. Two things there are
      not like-for-like and are handled explicitly:
        * Maia's cash INCLUDES unsettled forward legs; Waystone's Cash line does
          not (its forwards sit in receivables/payables).
        * Maia has no "other" concept at all, so that row is admin-only.
      Maia's forwards are attributed to fund vs share class BY SETTLEMENT DATE,
      using the map derived from Waystone's own owner blocks (measured on
      2026-07-24: fund settles 07-27 and 08-07, share classes 07-28 and 08-28).
      The map is derived per-valuation rather than hardcoded so it tracks the
      actual book.

A MEASURED CAVEAT ON THE MAIA COLUMN. Maia's published percentages divide by a
denominator that is neither its own displayed total nor the fund NAV, and its
rows summed to 93.85% on the 2026-07-24 snapshot because a subscription hit NAV
before the positions caught up. Only absolute figures are used here, never Maia's
percentages. See codebase-mcp backlog 2422.
"""

from __future__ import annotations


COMPONENTS = [
    ("clean_bond_mv", "Clean bond MV"),
    ("accrued_income", "Accrued income"),
    ("cash", "Cash"),
    ("fx_forward_pnl_fund", "FX forward P&L — fund hedges"),
    ("fx_forward_pnl_share_class", "FX forward P&L — share-class hedges"),
    ("other_net", "Other (net receivables/payables)"),
]


def _norm_date(v) -> str | None:
    """Normalise a settlement date to ISO.

    The two sources disagree on format: the admin pack yields ISO
    ("2026-08-07"), Maia writes UK order ("07/08/2026"). Matching them without
    normalising silently attributes nothing, which shows up as every forward
    being unattributed rather than as an error.
    """
    if v is None or v == "":
        return None
    s = str(v).strip()[:10]
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        d, m, y = s.split("/")
        return f"{y}-{m}-{d}"
    return s


def _owner_by_settlement(fx_forwards: list[dict]) -> dict:
    """{settlement_date: 'fund'|'share_class'} from the admin's own owner blocks.

    Derived per valuation rather than hardcoded: the dates move every time the
    book rolls, and a stale hardcoded map would silently misattribute hedges.
    A date used by BOTH owners is returned as None so the caller can refuse to
    guess rather than assign it arbitrarily.
    """
    seen: dict[str, set] = {}
    for f in fx_forwards or []:
        owner = f.get("owner_type")
        for dt in {f.get("settlement_date")} | {
                l.get("settlement_date") for l in (f.get("legs") or [])}:
            nd = _norm_date(dt)
            if nd and owner:
                seen.setdefault(nd, set()).add(owner)
    return {dt: (next(iter(o)) if len(o) == 1 else None) for dt, o in seen.items()}


def price_comparison(parsed_nav: dict, maia_rows: list[dict] | None = None,
                     ga10_prices: dict | None = None) -> dict:
    """Per-bond prices from each source, on a CONSTANT quantity.

    The point of switching Athena between its own, Maia's and Waystone's prices
    is to isolate WHICH input causes a valuation difference. Holding par fixed
    (the administrator's, since that is the registered position) and varying only
    price means any change in the revalued total is attributable to price alone —
    not to quantity, FX or accrual convention, which are the other three ways two
    systems disagree about a bond.

    Returns per-ISIN prices plus a revalued clean total for each source, so the
    caller can show "clean bond MV, priced by X" and read the difference as a
    pure price effect.
    """
    maia_px = {}
    for r in (maia_rows or []):
        isin = r.get("isin")
        if isin and r.get("last_px") is not None:
            maia_px[isin] = r["last_px"]

    rows, totals = [], {"waystone": 0.0, "maia": 0.0, "athena": 0.0}
    covered = {"waystone": 0, "maia": 0, "athena": 0}
    for h in parsed_nav.get("holdings") or []:
        isin = h.get("isin")
        par = h.get("par_amount") or 0.0
        w_px = h.get("price")
        m_px = maia_px.get(isin)
        a_px = (ga10_prices or {}).get(isin)
        # Waystone's own market_value already embeds its FX; reuse that ratio so
        # a repriced total stays in base currency without re-deriving FX here.
        mv = h.get("market_value") or 0.0
        fx_factor = (mv / (par * w_px / 100.0)) if (par and w_px) else 1.0

        entry = {"isin": isin, "description": h.get("description"),
                 "par": par, "currency": h.get("currency"),
                 "price_waystone": w_px, "price_maia": m_px, "price_athena": a_px}
        for src, px in (("waystone", w_px), ("maia", m_px), ("athena", a_px)):
            if px is None or not par:
                entry[f"clean_{src}"] = None
                continue
            val = par * px / 100.0 * fx_factor
            entry[f"clean_{src}"] = round(val, 2)
            totals[src] += val
            covered[src] += 1
        entry["price_diff_maia_waystone"] = (
            None if (m_px is None or w_px is None) else round(m_px - w_px, 6))
        rows.append(entry)

    return {
        "rows": rows,
        "totals": {k: round(v, 2) for k, v in totals.items()},
        # Coverage matters: a source that prices only half the book produces a
        # smaller total that must NOT be read as a valuation difference.
        "coverage": covered,
        "holdings": len(parsed_nav.get("holdings") or []),
    }


def waystone_side(parsed_nav: dict) -> dict:
    """Waystone components from a parsed NAV report."""
    b = parsed_nav["summary"]["aum_breakdown"]
    return {
        "clean_bond_mv": b["clean_bond_mv"],
        "accrued_income": b["accrued_income"],
        "cash": b["cash"],
        "fx_forward_pnl_fund": b["fx_forward_pnl_fund"],
        "fx_forward_pnl_share_class": b["fx_forward_pnl_share_class"],
        "other_net": b["other_net"],
        "total": b["total_nav"],
    }


def athena_side(parsed_nav: dict) -> dict:
    """Athena's own view.

    For funds whose book Athena ingests from the administrator (GDBF today) this
    IS the admin valuation re-presented with the owner split broken out, so it
    agrees by construction. `derived_from_admin` says so, and the UI must show
    that rather than presenting a zero diff as corroboration.
    """
    side = waystone_side(parsed_nav)
    side["derived_from_admin"] = True
    return side


def maia_side(maia_breakdown: dict, owner_by_settlement: dict,
              forwards: list[dict] | None = None) -> dict:
    """Maia components, with forwards attributed to fund vs share class.

    `forwards` is a list of {settlement_date, pnl} read off the position view.
    Any forward whose settlement date is not in the map, or is used by both
    owners, is reported as unattributed instead of being forced into one side.
    """
    # When the export's forward figures are gross leg notionals rather than
    # P&L (see aum_recon's plausibility guard), attributing them to fund vs
    # share class would produce confident-looking nonsense — GDBF 2026-07-24
    # showed -20,125,362.01 fund / +7,144,078.25 share class against true
    # values of +16,748.35 / -25,884.92. Report None instead.
    # Also covers the export simply not HAVING a forward P&L column (the
    # allocation view does not). Summing an absent column gives 0.00, which
    # renders as "Maia says the hedges are worth nothing" — a claim the file
    # does not make.
    _caps = maia_breakdown.get("capabilities") or {}
    if maia_breakdown.get("forwards_unreliable") or not _caps.get("forward_pnl", True):
        forwards = []
        fx_unreliable = True
    else:
        fx_unreliable = False

    fund = cls = unattributed = 0.0
    unknown_dates = []
    for f in forwards or []:
        owner = owner_by_settlement.get(_norm_date(f.get("settlement_date")))
        pnl = f.get("pnl") or 0.0
        if owner == "fund":
            fund += pnl
        elif owner == "share_class":
            cls += pnl
        else:
            unattributed += pnl
            unknown_dates.append(f.get("settlement_date"))

    return {
        # clean/accrued are None when the export carries no accrued column (the
        # allocation view does not). Passed through as None rather than zeroed:
        # a zero would read as "Maia says nil accrued", which is a claim the file
        # does not support. `bonds_dirty` is always available and is what the UI
        # should fall back to.
        "clean_bond_mv": maia_breakdown["clean_bond_mv"],
        "accrued_income": maia_breakdown["accrued_income"],
        "bonds_dirty": maia_breakdown.get("bonds_dirty"),
        "capabilities": maia_breakdown.get("capabilities"),
        "unavailable": maia_breakdown.get("unavailable"),
        "cash": maia_breakdown["cash"],
        "fx_forward_pnl_fund": None if fx_unreliable else round(fund, 2),
        "fx_forward_pnl_share_class": None if fx_unreliable else round(cls, 2),
        "fx_forward_pnl_unreliable": fx_unreliable,
        # Maia has no receivables/payables concept — admin-only, so None rather
        # than 0.0: a zero would read as "Maia says nil", which is a claim we
        # cannot make on its behalf.
        "other_net": None,
        "total": maia_breakdown["total"],
        "unattributed_fx_pnl": None if fx_unreliable else round(unattributed, 2),
        "unattributed_settlement_dates": sorted({_norm_date(d) for d in unknown_dates if d}),
        # Not like-for-like with the Waystone cash line — see module docstring.
        "cash_includes_forward_legs": True,
    }


def build(maia: dict, athena: dict, waystone: dict, meta: dict | None = None) -> dict:
    """Assemble the five-column comparison.

    Column order left to right: diff(Maia-Athena), Maia, Athena, Waystone,
    diff(Athena-Waystone). The Maia-vs-Waystone diff is carried on each row too
    (as `diff_maia_waystone`) since that is the only pair with two genuinely
    independent sources behind it.
    """
    rows = []
    for key, label in COMPONENTS:
        m, a, w = maia.get(key), athena.get(key), waystone.get(key)
        rows.append({
            "key": key,
            "label": label,
            "maia": m,
            "athena": a,
            "waystone": w,
            "diff_maia_athena": None if (m is None or a is None) else round(m - a, 2),
            "diff_athena_waystone": None if (a is None or w is None) else round(a - w, 2),
            "diff_maia_waystone": None if (m is None or w is None) else round(m - w, 2),
            # Rows where the sources genuinely measure different things, so a
            # non-zero diff is expected and must not read as a break.
            "not_like_for_like": key in ("cash", "other_net"),
        })

    m_t, a_t, w_t = maia.get("total"), athena.get("total"), waystone.get("total")
    total = {
        "label": "TOTAL NAV",
        "maia": m_t, "athena": a_t, "waystone": w_t,
        "diff_maia_athena": None if (m_t is None or a_t is None) else round(m_t - a_t, 2),
        "diff_athena_waystone": None if (a_t is None or w_t is None) else round(a_t - w_t, 2),
        "diff_maia_waystone": None if (m_t is None or w_t is None) else round(m_t - w_t, 2),
    }

    # ── MEMO: underlying fund AUM, share-class overlay stripped ───────────
    #
    # NAV above is the fund's total net assets and INCLUDES share-class hedge
    # P&L. That is not negotiable — the forwards are contracts of the fund and
    # their value is part of what it owns. Nothing here changes NAV.
    #
    # What this line answers is a different question: what is the fund worth on
    # its own investment activity, with the share-class hedging overlay taken
    # out? That is the figure to judge the manager on, because the overlay is
    # run for particular classes and its result belongs to them.
    #
    #     underlying AUM = NAV - share-class hedge P&L
    #     GDBF 2026-07-24: 10,502,454.15 - (-25,884.92) = 10,528,339.07
    #
    # The FUND-level hedge is deliberately NOT stripped. It is a portfolio
    # currency decision hedging the fund's own GBP/EUR bond exposure back to
    # base, so it belongs in the investment result like any other position.
    #
    # LABELLING IS LOAD-BEARING. Presenting this as NAV would misstate the
    # fund's assets, which is precisely the mistake this comment exists to stop
    # anyone repeating. It is a memo measure and must be rendered as one,
    # visually separated from the NAV row above it.
    memo_rows = []
    sc_key = "fx_forward_pnl_share_class"
    if any(side.get(sc_key) is not None for side in (maia, athena, waystone)):
        def _strip(side):
            tot, sc = side.get("total"), side.get(sc_key)
            return None if (tot is None or sc is None) else round(tot - sc, 2)
        m_u, a_u, w_u = _strip(maia), _strip(athena), _strip(waystone)
        memo_rows.append({
            "key": "underlying_fund_aum",
            "label": "Underlying fund AUM (ex share-class hedging)",
            "memo": True,
            "maia": m_u, "athena": a_u, "waystone": w_u,
            "diff_maia_athena": None if (m_u is None or a_u is None) else round(m_u - a_u, 2),
            "diff_athena_waystone": None if (a_u is None or w_u is None) else round(a_u - w_u, 2),
            "diff_maia_waystone": None if (m_u is None or w_u is None) else round(m_u - w_u, 2),
            "note": "NAV with the share-class hedging overlay removed. A memo "
                    "measure for judging investment performance — NOT the fund's "
                    "NAV, which necessarily includes these forwards. Fund-level "
                    "hedging is retained, being a portfolio decision.",
        })

    # BONDS ON A DIRTY BASIS. When Maia's export carries no accrued column the
    # clean and accrued rows above are both blank, which leaves the largest
    # thing the fund owns absent from the Maia column altogether — the reader
    # sees empty cells where 10m of bonds should be and cannot tell whether
    # Maia holds nothing or the file says nothing. Dirty is always available on
    # both sides, and it is the comparison that does not depend on either
    # system's accrual convention.
    m_dirty = maia.get("bonds_dirty")
    if m_dirty is not None and maia.get("clean_bond_mv") is None:
        def _dirty(side):
            c, a_ = side.get("clean_bond_mv"), side.get("accrued_income")
            return None if (c is None or a_ is None) else round(c + a_, 2)
        a_d, w_d = _dirty(athena), _dirty(waystone)
        memo_rows.append({
            "key": "bonds_dirty",
            "label": "Bonds, dirty (clean + accrued)",
            "memo": True,
            "maia": m_dirty, "athena": a_d, "waystone": w_d,
            "diff_maia_athena": None if a_d is None else round(m_dirty - a_d, 2),
            "diff_athena_waystone": None if (a_d is None or w_d is None) else round(a_d - w_d, 2),
            "diff_maia_waystone": None if w_d is None else round(m_dirty - w_d, 2),
            "note": "Shown because this Maia export carries no accrued column, "
                    "so the clean/accrued split above is blank on the Maia side. "
                    "Dirty is directly comparable and is independent of either "
                    "system's accrual convention.",
        })

    notes = []
    if maia.get("unavailable"):
        notes.append(maia["unavailable"])
    if athena.get("derived_from_admin"):
        notes.append(
            "Athena's book for this fund is ingested from the administrator, so "
            "the Athena/Waystone column is identity, not corroboration. It "
            "becomes a real check once Athena prices independently.")
    if maia.get("cash_includes_forward_legs"):
        notes.append(
            "Maia's cash includes unsettled FX forward legs; Waystone's Cash "
            "line does not (its forwards sit in receivables/payables). The cash "
            "rows are not like-for-like.")
    if maia.get("other_net") is None:
        notes.append(
            "'Other' is receivables/payables off the Waystone Balance Sheet. "
            "Maia has no equivalent, so it is blank rather than zero.")
    if maia.get("unattributed_fx_pnl"):
        notes.append(
            f"{maia['unattributed_fx_pnl']:,.2f} of Maia FX P&L could not be "
            f"attributed to fund or share class (settlement dates "
            f"{', '.join(maia['unattributed_settlement_dates'])} not matched to "
            "an owner block in the admin pack).")

    return {"rows": rows, "total": total, "memo_rows": memo_rows,
            "notes": notes, "meta": meta or {}}
