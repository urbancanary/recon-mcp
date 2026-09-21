"""Parse coverage — the parsers' own record of what they could NOT read.

WHY THIS EXISTS. nav_parser and bbg_parser both swallow an unreadable section
(missing header, unfamiliar sheet, a column the export does not carry) and log
a warning. The valuation still stores and nothing tells the reader that part of
the report went unread. These tests pin the two properties that make the new
`parse_coverage` useful:

  1. A file whose sections are all present reports NO coverage gaps — a
     mechanism that fires on everything is noise, not a signal.
  2. A file missing a section NAMES that section, and the note that reaches
     recon_uploads says so.

No parser is given a real client file: the fixtures are invented workbooks, as
in conftest. Redacted GCRIF/BBG samples are still outstanding (rebuild audit) —
these tests cover the mechanism, not the real reports.
"""
import sys
from pathlib import Path

import openpyxl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bbg_parser import parse_bbg_export  # noqa: E402
from nav_parser import parse_nav_report  # noqa: E402
from recon_db import coverage_note  # noqa: E402


# ── BBG PORT export ────────────────────────────────────────────────────────

# Row 1-3 metadata, row 4 header, then bonds — the shape parse_bbg_export
# expects. Only the columns a minimal export carries.
BBG_HEADER_FULL = [
    "Portfolio", "ISIN", "Security", "Position", "Market Value",
    "Accrued Int", "Price", "YTM", "YTW", "Mod Dur", "Cpn Rate",
    "Cpn Freq", "Day Count", "Issue Date", "Maturity Date",
]
BBG_HEADER_MINIMAL = ["Portfolio", "ISIN", "Security", "Accrued Int"]


def _bbg_workbook(path, header):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Bloomberg Portfolio Export", None, None])
    ws.append(["As of Date", "09/15/2026", None])
    ws.append(["Base Currency", "USD", None])
    ws.append(header)
    row = {
        "Portfolio": "GDBFT", "ISIN": "XS0000000001", "Security": "TEST 5% 2030",
        "Position": 1_000_000, "Market Value": 995_000.0, "Accrued Int": 4_500.0,
        "Price": 99.5, "YTM": 5.1, "YTW": 5.05, "Mod Dur": 3.5,
        "Cpn Rate": "5.0000%", "Cpn Freq": "S/A", "Day Count": "ACT/365",
        "Issue Date": "09/15/2020", "Maturity Date": "09/15/2030",
    }
    ws.append([row.get(h) for h in header])
    wb.save(path)
    return path


def test_a_complete_export_reports_no_coverage_gaps(tmp_path):
    # Anti-noise: a mechanism that flags every file tells you nothing.
    result = parse_bbg_export(_bbg_workbook(tmp_path / "full.xlsx",
                                            BBG_HEADER_FULL).read_bytes())
    gaps = {c["section"] for c in result["parse_coverage"]}
    assert "ytm" not in gaps
    assert "cpn_rate" not in gaps


def test_a_thin_export_names_every_column_it_does_not_carry(tmp_path):
    # The real defect: this file parses to one bond with no yield, no price,
    # no duration, no coupon and no maturity, and before this change said so
    # only in a logger.info line. The note must reach recon_uploads.
    result = parse_bbg_export(_bbg_workbook(tmp_path / "thin.xlsx",
                                            BBG_HEADER_MINIMAL).read_bytes())
    note = coverage_note(result["parse_coverage"])
    assert note, "a four-column export reported full coverage"
    for missing in ("ytm", "ytw", "mod_dur", "mv", "coupon", "maturity_date"):
        # Each gap is its own entry, keyed by the column it costs you.
        assert f"'{missing}'" in note, f"{missing} went unreported"
    # The gap is structural, and must say what it costs, not just what is absent.
    assert "None for every bond" in note


def test_coverage_note_is_none_when_the_parse_was_complete():
    # NULL on the upload row means "fully read". An empty string would read as
    # "something went wrong" on a row where nothing did.
    assert coverage_note(None) is None
    assert coverage_note([]) is None
    assert coverage_note([{"section": "holdings", "detail": "empty"}]) == "holdings: empty"


# ── InvestOne NAV report ───────────────────────────────────────────────────

def _nav_workbook(path, gross_header=True,
                  ai_sheet="WFAI_Accrued_Income_Recon"):
    """The minimum workbook parse_nav_report completes on: Balance_Sheet,
    an accrued-income recon sheet, a Detailed_Security_Valuation it keeps, and
    the two optional sheets it tolerates losing."""
    wb = openpyxl.Workbook()
    bs = wb.active
    bs.title = "Balance_Sheet"
    bs.append(["Fund Name", "TEST FUND", "23-Jan-26", "26-Jan-26"])
    bs.append(["Fund Base Ccy", "USD"])
    bs.append(["Cash", 1000.0])
    bs.append(["Net Assets", None, None, None, 2000.0])
    ws = wb.create_sheet(ai_sheet)
    ws.append(["Valuation Date", "26-Jan-2026"])
    # The header this parser scans for. GCRIF's Link-era column is 17; the
    # fallback to it is the silent-default this whole file exists to expose.
    ws.append(["Gross Income (Local)"] if gross_header else ["Something Else"])
    ws.append([None, None, None, "detail", None, "SEDOL1", 1.23])
    ds = wb.create_sheet("Detailed_Security_Valuation")
    ds.append(["h"] * 20)
    for _ in range(25):
        ds.append([None] * 20)
    sc = wb.create_sheet("Share_Class_Price_Report")
    sc.append(["Share Class"]); sc.append(["A", 1.0])
    oc = wb.create_sheet("OpenCurrency")
    oc.append(["x"]); oc.append(["y"])
    wb.save(path)
    return path


def _nav_gap_sections(path, **kw):
    return {c["section"] for c in
            parse_nav_report(_nav_workbook(path, **kw).read_bytes())["parse_coverage"]}


def test_a_report_whose_accrued_header_is_missing_says_so(tmp_path):
    # The fallback silently reads column 17 — right for GCRIF's layout, not
    # guaranteed for GDBF's. It is the single most consequential default in
    # this parser and it used to reach the log only.
    assert "accrued_income" in _nav_gap_sections(
        tmp_path / "noheader.xls", gross_header=False)


def test_a_report_carrying_the_header_reports_no_accrued_gap(tmp_path):
    assert "accrued_income" not in _nav_gap_sections(
        tmp_path / "withheader.xls", gross_header=True)


def test_the_wrong_accrued_sheet_is_named_in_the_gap(tmp_path):
    # _accrued_sheet() exists because this exact miss silently emptied
    # accrued_by_sedol and coupon_by_sedol for a whole fund.
    result = parse_nav_report(_nav_workbook(
        tmp_path / "wrongsheet.xls", ai_sheet="Accrued_Income").read_bytes())
    gap = next(c for c in result["parse_coverage"] if c["section"] == "accrued_income")
    assert "Accrued_Income" in gap["detail"]
