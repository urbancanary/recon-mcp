"""Parser for Waystone (WFS) NAV report workbooks — GDBF-format.

The Guinness Global Dynamic Bond Fund NAV workbook (daily email to
minerva@x-trillion.ai, filename "GUINNESS GLOBAL DYNAMIC BOND FUND - NAV
REPORTS - dd.mm.yyyy.xls[x]") differs from the GCRIF InvestOne format that
nav_parser.py handles. Sheets of interest:

  Share_Class_Price_Report      per-class NAV/price
  Detailed_Security_Valuation   position-level: sedol, isin, description, qty,
                                price, book/market values, type field
  WFAI_Accrued_Income_Recon     income accruals by category (incl. the rate
                                field the admin is accruing at — evidential)
  Earned_Income                 daily income movement per position

Handles both .xls (xlrd) and .xlsx (openpyxl). Returns plain dicts so the
recon engine / Athena can consume without depending on either reader.

Proven against the 15/21/24-Jul-2026 GDBF reports (the Nationwide CCDS case:
this parser is how the fabricated 10.25% accrual at GBP47.61/day was
extracted — see _internal_recon/reports/nationwide-ccds-no-accrual-evidence.md).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def _load_sheets(path: str) -> Dict[str, List[List[Any]]]:
    if path.lower().endswith(".xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            return {ws.title: [list(row) for row in ws.iter_rows(values_only=True)]
                    for ws in wb.worksheets}
        finally:
            wb.close()
    import xlrd
    wb = xlrd.open_workbook(path)
    return {sn: [[wb.sheet_by_name(sn).cell_value(r, c)
                  for c in range(wb.sheet_by_name(sn).ncols)]
                 for r in range(wb.sheet_by_name(sn).nrows)]
            for sn in wb.sheet_names()}


_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def positions(path: str) -> List[Dict[str, Any]]:
    """Position rows from Detailed_Security_Valuation. Columns are positional
    in the Waystone layout: sedol, isin, description, side, ccy, quantity,
    price, book_local, book_base, market_local, market_base, unreal_gl, type."""
    sheets = _load_sheets(path)
    rows = sheets.get("Detailed_Security_Valuation", [])
    out = []
    for r in rows:
        cells = [c for c in r if c not in (None, "")]
        isin_idx = next((i for i, c in enumerate(cells)
                         if isinstance(c, str) and _ISIN.match(c.strip())), None)
        if isin_idx is None:
            continue
        c = cells
        i = isin_idx
        try:
            out.append({
                "sedol": str(c[i - 1]).strip() if i >= 1 else None,
                "isin": str(c[i]).strip(),
                "description": str(c[i + 1]).strip(),
                "ccy": str(c[i + 3]).strip(),
                "quantity": float(c[i + 4]),
                "price": float(c[i + 5]),
                "book_local": float(c[i + 6]),
                "book_base": float(c[i + 7]),
                "market_local": float(c[i + 8]),
                "market_base": float(c[i + 9]),
                "type": next((str(x) for x in c[i + 10:i + 16]
                              if isinstance(x, str) and x.strip().isupper()
                              and not x.strip() in ("B", "S")), None),
            })
        except (ValueError, IndexError, TypeError):
            continue
    return out


def accrued_income(path: str) -> List[Dict[str, Any]]:
    """Accrual rows from WFAI_Accrued_Income_Recon: category, sedol, isin,
    name, rate_pct (as printed — evidential: this is the rate the admin is
    accruing at), quantity, accrued balance."""
    sheets = _load_sheets(path)
    rows = sheets.get("WFAI_Accrued_Income_Recon", [])
    out = []
    for r in rows:
        cells = [str(c) if c is not None else "" for c in r]
        joined = cells
        isin_idx = next((i for i, c in enumerate(joined)
                         if _ISIN.match(c.strip())), None)
        if isin_idx is None or "detail" not in [c.strip() for c in cells[:6]]:
            continue
        rate = next((c for c in cells if c.strip().endswith("%")), None)
        nums = []
        for c in r[isin_idx + 1:]:
            if isinstance(c, (int, float)):
                nums.append(float(c))
        out.append({
            "category": cells[4].strip() or cells[3].strip(),
            "isin": joined[isin_idx].strip(),
            "name": joined[isin_idx + 1].strip(),
            "rate_pct": rate,
            "quantity": nums[1] if len(nums) > 1 else None,
            "accrued_balance": nums[2] if len(nums) > 2 else None,
            "raw_numbers": nums[:8],
        })
    return out


def nav_summary(path: str) -> Optional[Dict[str, Any]]:
    """Fund-level figures from Share_Class_Price_Report (best-effort)."""
    sheets = _load_sheets(path)
    rows = sheets.get("Share_Class_Price_Report", [])
    for r in rows:
        cells = [c for c in r if c not in (None, "")]
        if any(isinstance(c, str) and "nav" in c.lower() for c in cells):
            return {"raw_header_row": cells}
    return None
