# CANONICAL SOURCE: athena_html_v3/aum_recon.py @ 0600e07 (2026-08-02).
# SYNCED COPY (stage-3 recon move) — athena's copy is deprecated and will
# be removed once parity is verified; this becomes the canonical home.
"""
AUM reconciliation — Maia vs the administrator, component by component.

Breaks both sides into the same five components and compares them:

    clean bond MV + accrued + cash + FX forward P&L + other = total

The admin side comes from nav_parser's summary["aum_breakdown"] (parsed from the
Waystone Balance Sheet). The Maia side is read here, from the position-view
export whose Sheet0 carries Qty / Last Px / Dirty price / Accrued Total (Local) /
Exposure / Currency.

MEASURED on GDBF at 2026-07-24 (uploads/gdbfpos7.xlsx vs the stored Waystone
valuation), which is where every rule below comes from:

    component            Maia            Admin           diff
    clean bond MV   10,153,879.16   10,165,176.38     -11,297.22
    accrued            196,824.07      186,969.64       9,854.43
    cash               145,859.17       67,623.28      78,235.89
    FX forward P&L      -1,245.06       -9,136.57       7,891.51
    other                    0.00       91,821.42     -91,821.42
    TOTAL           10,496,562.40   10,502,454.15      -5,891.75   (0.056%)

FOUR THINGS THAT WILL BITE ANYONE REDOING THIS:

1. NEVER RECOMPUTE CLEAN FROM PRICE. Maia's Exposure is DIRTY (verified on both
   GDBF and GCRIF: exposure exceeds par x price / 100 x spot by a small positive
   margin on every bond — that margin is accrued). The obvious move is to
   recompute clean as qty x px / 100, and it breaks on the Nationwide CCDS
   (GB00BBQ33664), which is quoted PER UNIT, not per 100. Dividing it by 100
   understated that line by 290,163.04 — which was the entire unexplained gap in
   the first attempt. `clean = exposure - accrued` uses Maia's own numbers and is
   immune to the quotation convention. (The admin agrees the security is
   per-unit: 1,700 x 129.875 = 220,787.50 exactly.)

2. MAIA'S FORWARDS HAVE Exposure = 0. Their P&L is in "NAV Contri Fund CCY",
   never in the Exposure column — the same behaviour seen on GCRIF. Summing
   Exposure for forwards silently yields 0.00 and looks like "no forwards".

3. MAIA'S CASH INCLUDES UNSETTLED FORWARD LEGS; the admin's Cash line does not
   (its forwards sit in receivables/payables). So the cash rows are NOT
   like-for-like even when both are right, and most of the +78,235.89 cash
   difference is this rather than a real cash break.

4. NUMERIC COLUMNS ARE COMMA-FORMATTED STRINGS ("10,496,562.41"), so a naive
   sum raises or silently concatenates.

WHAT THE MEASURED DIFFERENCES ACTUALLY MEAN:
  - Bonds agree far better than the component rows suggest. On a DIRTY basis the
    two differ by only -1,442.79 on 10.35m (0.014%). The -11,297 clean / +9,854
    accrued pair is largely the same money classified differently: Maia's
    price-implied accrual runs ahead of the admin's booked accrual (Andy's own
    "Accrued Comparison" tab shows Maia implying more accrual days on every line,
    e.g. 347.0 vs 292.6 on the Bund).
  - FX forwards are NOT comparable yet: Maia lists 7 forwards, the admin 25 legs
    (8 fund + 17 share-class). Maia appears to carry fund-level hedges only, so
    -1,245.06 should be read against the admin's FUND figure (+16,748.35), not
    against the -9,136.57 combined total. Confirm with Will before treating the
    residual as a break.
  - "Other" (91,821.42) is an admin-only concept — net receivables/payables off
    the Balance Sheet with no Maia counterpart. It is most of the headline total
    difference and is expected, not a break.
"""

from __future__ import annotations

import openpyxl

# COLUMNS ARE RESOLVED BY NAME, NOT POSITION. Maia exports the same book in at
# least three shapes and they share no column order:
#
#   13 cols  allocation view  (gdbft310726pm1451) Grouping, Holding, Exp (Fund CCY)
#   34 cols  priced view      (gdbfpos7)          Qty, Exposure ($ USD), Accrued Total
#   629 cols full export      (gdbfpos240726am1145) everything, incl. ISIN
#
# Reading by fixed index meant the 34-column layout worked and the other two
# parsed to ZERO rows without error — which put -10,517,972.84 on the AUM Recon
# page (Maia read as nil against a full administrator book) until a
# "parsed 0 bonds" guard caught it. A guard is not a fix; this is.
#
# Each field lists its known headers in preference order. A field with no match
# is simply absent, and callers must handle that rather than assume — the
# allocation view genuinely has no accrued or dirty price, and inventing one
# would be worse than reporting the section unavailable.
_FIELDS = {
    "grouping":    ["Grouping"],
    # The 629-column export has NO Grouping column; it classifies by Asset
    # Class ("Corporate Bond" / "Government Bond" / "Convertible Bond" /
    # "Cash" / "Exchrate Forward"). Either one is enough to classify a row.
    "asset_class": ["Asset Class"],
    "ticker":      ["Ticker"],
    "description": ["Description", "Short Name"],
    "isin":        ["ISIN"],
    "currency":    ["Currency"],
    "qty":         ["Qty", "Holding", "Qty / Total Qty", "Total Qty"],
    "exposure":    ["Exposure ($ USD)", "Exp (Fund CCY)", "Exp Fund CCY"],
    "last_px":     ["Last Px"],
    "dirty_px":    ["Dirty price", "Dirty Px"],
    # Accrued comes in two flavours and they must NOT be treated alike:
    # "(Local)" is in the security's own currency and needs the FX rate applied;
    # "Fund CCY" is already in base and must NOT be multiplied again. Conflating
    # them would inflate every non-base bond's accrued by its FX rate.
    "accrued_local": ["Accrued Total (Local)", "Accrued Total"],
    "accrued_base":  ["Accrued Interest Fund CCY"],
    "nav_contri":  ["NAV Contri Fund CCY (Alt 1)", "NAV Contri Fund CCY",
                    "NAV Contri Reporting CCY"],
    "settlement":  ["Settlement Date"],
}

BOND_GROUPING = "Fixed Income"
CASH_GROUPING = "Currency"


def _classify(cols, row) -> str:
    """'bond' | 'cash' | 'forward' | '' — from whichever classifier the shape has.

    Grouping is checked first (the priced and allocation views have it); Asset
    Class is the fallback for the full export. Forwards are recognised by asset
    class where available and by the ticker convention otherwise, because in the
    Grouping-only shapes forwards sit under Currency alongside real cash.
    """
    ac = str(cols.get(row, "asset_class") or "")
    if ac and ac != "None":
        low = ac.lower()
        if "forward" in low:
            return "forward"
        if "bond" in low or "fixed income" in low:
            return "bond"
        if "cash" in low or "currency" in low:
            return "cash"
        return ""
    grouping = str(cols.get(row, "grouping") or "")
    if grouping == BOND_GROUPING:
        return "bond"
    if grouping == CASH_GROUPING:
        return "forward" if "Fwd" in str(cols.get(row, "ticker") or "") else "cash"
    return ""


def _norm_header(h) -> str:
    return " ".join(str(h or "").split()).strip().lower()


class _Cols:
    """Header-name -> column index for one Maia sheet."""

    def __init__(self, header_row):
        self._by_norm = {}
        for i, h in enumerate(header_row):
            key = _norm_header(h)
            if key and key not in self._by_norm:
                self._by_norm[key] = i
        self.index = {}
        for field, names in _FIELDS.items():
            for name in names:
                i = self._by_norm.get(_norm_header(name))
                if i is not None:
                    self.index[field] = i
                    break
        self.missing = [f for f in _FIELDS if f not in self.index]

    def get(self, row, field):
        i = self.index.get(field)
        return None if i is None or i >= len(row) else row[i]

    def has(self, *fields):
        return all(f in self.index for f in fields)


def _num(v):
    """Maia writes numbers as comma-formatted strings; None for blanks."""
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return None


def _sheet(path):
    """Maia always writes the position grid to Sheet0, headers on row 1."""
    ws = openpyxl.load_workbook(path, data_only=True)["Sheet0"]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError(f"{path}: Sheet0 is empty")
    return _Cols(rows[0]), rows[1:]


def _fx_rates(cols: _Cols, rows) -> dict:
    """FX to base, derived from each currency's own cash row (exposure / qty).

    Exact for cash, and taken from the file so the comparison uses the rates Maia
    used rather than rates we chose.
    """
    rates = {"USD": 1.0}
    if not cols.has("qty", "exposure", "currency"):
        return rates
    for row in rows:
        if _classify(cols, row) != "cash":
            continue
        ccy = cols.get(row, "currency")
        qty, exp = _num(cols.get(row, "qty")), _num(cols.get(row, "exposure"))
        if ccy and qty and exp is not None and ccy not in rates:
            rates[ccy] = exp / qty
    return rates


def maia_breakdown(path: str) -> dict:
    """AUM components from any Maia export shape.

    Requires Grouping, Qty and Exposure. Accrued, price and forward P&L are
    reported only when the export actually carries them — the allocation view
    has none of the three, and `capabilities` says so rather than returning
    zeros that would read as "no accrued" instead of "not in this file".
    """
    cols, rows = _sheet(path)
    if not (cols.has("grouping") or cols.has("asset_class")) \
            or not cols.has("qty", "exposure"):
        raise ValueError(
            f"{path}: not a Maia position export — needs Grouping or Asset "
            f"Class, plus Qty and Exposure (found {len(cols.index)} known "
            f"fields: {sorted(cols.index)})")

    has_accrued = cols.has("accrued_local") or cols.has("accrued_base")
    has_fwd_pnl = cols.has("nav_contri")
    rates = _fx_rates(cols, rows)

    dirty = accrued = cash = forward_pnl = 0.0
    n_bonds = n_cash = n_fwd = 0
    per_unit_suspects = []
    forwards = []      # {settlement_date, pnl} — for owner attribution
    bond_rows = []     # per-bond prices, for the price-source comparison

    for row in rows:
        kind = _classify(cols, row)
        ticker = str(cols.get(row, "ticker") or "")
        qty = _num(cols.get(row, "qty"))
        exposure = _num(cols.get(row, "exposure"))
        # Subtotal and grand-total rows carry an exposure but no quantity.
        if exposure is None or qty is None:
            continue

        if kind == "bond":
            fx = rates.get(cols.get(row, "currency"), 1.0)
            # Base-currency accrued is used as-is; local is converted. See the
            # note on the two accrued fields above.
            base_acc = _num(cols.get(row, "accrued_base"))
            acc_base = (base_acc if base_acc is not None
                        else (_num(cols.get(row, "accrued_local")) or 0.0) * fx)
            accrued += acc_base
            # Exposure is DIRTY. clean = dirty - accrued; see rule 1 in the
            # module docstring — do NOT recompute clean from price.
            dirty += exposure
            n_bonds += 1
            px = _num(cols.get(row, "last_px"))
            desc = cols.get(row, "description")
            bond_rows.append({
                # ISIN when the shape carries one (the 629-column export does),
                # None otherwise. NEVER fuzzy-matched from description: a
                # mis-joined price is a wrong valuation.
                "isin": cols.get(row, "isin"),
                "ticker": ticker, "description": desc,
                "last_px": px, "dirty_px": _num(cols.get(row, "dirty_px")),
                "qty": qty, "currency": cols.get(row, "currency"),
                "exposure": exposure,
                "accrued": acc_base if has_accrued else None,
            })
            # Keep the per-unit quotation issue (CCDS) visible rather than
            # silently absorbed by the subtraction above.
            if px:
                per_100 = qty * px / 100.0 * fx
                if per_100 > 0 and (exposure - acc_base) / per_100 > 50:
                    per_unit_suspects.append({
                        "ticker": ticker, "description": desc,
                        "qty": qty, "price": px, "exposure": exposure,
                    })
        elif kind == "forward":
            # Forwards carry Exposure = 0; their P&L is in NAV Contri.
            pnl = _num(cols.get(row, "nav_contri")) or 0.0
            forward_pnl += pnl
            forwards.append({"ticker": ticker, "pnl": pnl,
                             "settlement_date": cols.get(row, "settlement")})
            n_fwd += 1
        elif kind == "cash":
            cash += exposure
            n_cash += 1

    # PLAUSIBILITY GUARD ON FORWARD P&L. Maia's forwards always carry
    # Exposure = 0 and put their value in NAV Contri, but WHICH value differs by
    # export shape. Measured on GDBF 2026-07-24, the same leg (qty
    # -1,842,705.71) reads -1,287.42 in the 34-column priced view and
    # -15,117,488.07 in the 629-column full export — the latter is a gross leg
    # notional, not P&L. Bond figures agree exactly across both shapes, so this
    # is specific to forwards.
    #
    # Summing the notionals produced a total NAV of -2,485,989.76 against a real
    # 10,495,317.34. Rather than guess which column carries P&L in a shape we
    # have not verified, treat an implausible magnitude as "unknown" and drop it
    # from the total. Hedge P&L is a small fraction of a bond fund's NAV; a
    # figure worth more than a quarter of the bond book is not P&L.
    forwards_unreliable = n_fwd > 0 and dirty > 0 and abs(forward_pnl) > 0.25 * dirty
    if forwards_unreliable:
        forward_pnl = 0.0

    out = {
        "source": "maia",
        "shape": {"columns": len(cols._by_norm),
                  "fields": sorted(cols.index), "missing": sorted(cols.missing)},
        "capabilities": {"accrued": has_accrued, "forward_pnl": has_fwd_pnl,
                         "isin": cols.has("isin"), "price": cols.has("last_px")},
        "forwards_unreliable": forwards_unreliable,
        # Top-level so consumers that never look at capabilities still see it.
        "forwards_unreliable": forwards_unreliable,
        "bonds_dirty": round(dirty, 2),
        "clean_bond_mv": round(dirty - accrued, 2) if has_accrued else None,
        "accrued_income": round(accrued, 2) if has_accrued else None,
        "cash": round(cash, 2),
        "fx_forward_pnl": (round(forward_pnl, 2)
                           if has_fwd_pnl and not forwards_unreliable else None),
        "other_net": 0.0,          # no Maia counterpart — see docstring
        "total": round(dirty + cash + forward_pnl, 2),
        "counts": {"bonds": n_bonds, "cash_rows": n_cash, "forwards": n_fwd},
        "forwards": forwards,
        "bond_rows": bond_rows,
        "fx_rates": {k: round(v, 6) for k, v in rates.items()},
        "per_unit_suspects": per_unit_suspects,
    }
    notes = []
    if forwards_unreliable:
        out["capabilities"]["forward_pnl"] = False
        notes.append(
            f"Forward P&L is not usable from this export: the {n_fwd} forward "
            f"rows sum to a magnitude far larger than the bond book, which means "
            f"the NAV Contri column here carries gross leg notionals rather than "
            f"P&L. Excluded from the total. Use a priced (gdbfpos-style) export "
            f"for hedge P&L.")
    if notes:
        out["unavailable"] = " ".join(notes)
    if not has_accrued:
        out["unavailable"] = (out.get("unavailable", "") + " " if notes else "") + (
            "This export carries no accrued column, so the clean/accrued split "
            "and the accrued comparison cannot be produced. Bonds are compared "
            "on a dirty basis only. Supply a priced (gdbfpos-style) export for "
            "the full breakdown.")
    return out


def compare(maia: dict, admin: dict) -> dict:
    """Line-by-line comparison of the two breakdowns.

    A component the Maia export does not carry is reported as unavailable, NOT
    as zero. Zero would show as a difference equal to the whole admin figure and
    read as a break, when the truth is that the file simply lacks the column.
    """
    keys = ["clean_bond_mv", "accrued_income", "cash", "fx_forward_pnl", "other_net"]
    lines = []
    for k in keys:
        m, a_ = maia.get(k), admin.get(k, 0.0)
        if m is None:
            lines.append({"component": k, "maia": None, "admin": a_,
                          "diff": None, "unavailable": True})
        else:
            lines.append({"component": k, "maia": m, "admin": a_,
                          "diff": round(m - a_, 2)})
    m_total, a_total = maia["total"], admin["total_nav"]

    # Bonds compared on a DIRTY basis too. The clean/accrued split differs by
    # convention (Maia's price-implied accrual runs ahead of the admin's booked
    # accrual), so dirty is the honest measure of whether the two agree on what
    # the bonds are worth — and it is the ONLY bond measure available when the
    # export has no accrued column.
    m_dirty = maia.get("bonds_dirty")
    if m_dirty is None:
        m_dirty = (maia.get("clean_bond_mv") or 0.0) + (maia.get("accrued_income") or 0.0)
    a_dirty = admin["clean_bond_mv"] + admin["accrued_income"]

    return {
        "lines": lines,
        "total": {"maia": m_total, "admin": a_total, "diff": round(m_total - a_total, 2),
                  "diff_pct": round((m_total - a_total) / a_total * 100, 4) if a_total else None},
        "bonds_dirty": {"maia": round(m_dirty, 2), "admin": round(a_dirty, 2),
                        "diff": round(m_dirty - a_dirty, 2)},
        "unavailable": maia.get("unavailable"),
    }


def _fmt(v):
    return f"{v:>16,.2f}"


def print_comparison(maia: dict, admin: dict, label: str = "") -> None:
    c = compare(maia, admin)
    LABELS = {"clean_bond_mv": "Clean bond MV", "accrued_income": "Accrued income",
              "cash": "Cash", "fx_forward_pnl": "FX forward P&L",
              "other_net": "Other (net rec/pay)"}
    print(f"{label}\n")
    print(f"  {'COMPONENT':24} {'MAIA':>16} {'ADMIN':>16} {'DIFF':>14}")
    for line in c["lines"]:
        if line.get("unavailable"):
            print(f"  {LABELS[line['component']]:24} {'not in export':>16} "
                  f"{_fmt(line['admin'])} {'—':>14}")
        else:
            print(f"  {LABELS[line['component']]:24} {_fmt(line['maia'])} "
                  f"{_fmt(line['admin'])} {line['diff']:>14,.2f}")
    print(f"  {'-'*24} {'-'*16} {'-'*16} {'-'*14}")
    t = c["total"]
    print(f"  {'TOTAL':24} {_fmt(t['maia'])} {_fmt(t['admin'])} {t['diff']:>14,.2f}"
          f"   ({t['diff_pct']:+.4f}%)")
    b = c["bonds_dirty"]
    print(f"\n  memo — bonds on a DIRTY basis: Maia {b['maia']:,.2f} vs admin "
          f"{b['admin']:,.2f}, diff {b['diff']:,.2f}")
    if c.get("unavailable"):
        print(f"\n  ! {c['unavailable']}")
    if maia.get("per_unit_suspects"):
        print("  memo — per-unit quoted securities (not per 100):")
        for s in maia["per_unit_suspects"]:
            print(f"           {s['ticker']} — {s['description']}")


def main(argv=None) -> int:
    import argparse
    import nav_parser
    ap = argparse.ArgumentParser(description="Reconcile Maia AUM against the administrator")
    ap.add_argument("maia", help="Maia position-view export (.xlsx with Sheet0)")
    ap.add_argument("admin", help="Waystone NAV report (.xls)")
    args = ap.parse_args(argv)

    m = maia_breakdown(args.maia)
    parsed = nav_parser.parse_nav_report(args.admin)
    a = parsed["summary"]["aum_breakdown"]
    print_comparison(m, a, f"AUM RECONCILIATION — {parsed.get('fund_name') or ''} "
                           f"{parsed.get('valuation_date')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
