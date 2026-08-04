"""Content-based classification — synthetic workbooks in each real shape."""
import io

import openpyxl

import maia_classify
from conftest import write_allocation_view, write_priced_view, write_reference_view


def _wb(sheets):
    """{sheet_name: [header_row, *rows]} → xlsx bytes."""
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_admin_pack_by_sheet_names():
    b = _wb({"Share_Class_Price_Report": [["x"]], "Balance_Sheet": [["y"]],
             "OpenCurrency": [["z"]]})
    assert maia_classify.classify_xlsx(b)["type"] == "admin_pack"


def test_known_shapes(tmp_path):
    assert maia_classify.classify_xlsx(
        (tmp_path / "p.xlsx").read_bytes()
        if False else open(write_priced_view(tmp_path / "p.xlsx"), "rb").read()
    )["type"] == "maia_priced"
    assert maia_classify.classify_xlsx(
        open(write_allocation_view(tmp_path / "a.xlsx"), "rb").read()
    )["type"] == "maia_allocation"
    assert maia_classify.classify_xlsx(
        open(write_reference_view(tmp_path / "r.xlsx"), "rb").read()
    )["type"] == "maia_full"


def test_new_positions_shape():
    b = _wb({"Sheet0": [["Modified", "Ticker", "Qty", "Strategy",
                         "Instrument ID", "Instance Id", "Exp Fund CCY",
                         "Exp Inst CCY", "Date", "Source"]]})
    assert maia_classify.classify_xlsx(b)["type"] == "maia_positions"


def test_aum_summary_shape():
    b = _wb({"Sheet0": [["Instance Id", "Date", "Source", "Modified", "Date",
                         "AUM", "AUM ($)", "Calculated NAV", "Fund Type"]]})
    assert maia_classify.classify_xlsx(b)["type"] == "maia_aum"


def test_compliance_shape():
    b = _wb({"Sheet0": [["Date", "Rule ID", "Name", "Description", "Fail Msg",
                         "Active", "Rule Type"]]})
    assert maia_classify.classify_xlsx(b)["type"] == "maia_compliance"


def test_unknown_and_unreadable():
    assert maia_classify.classify_xlsx(b"not a workbook")["type"] == "unknown"
    b = _wb({"Sheet0": [["Colour", "Animal"]]})
    assert maia_classify.classify_xlsx(b)["type"] == "unknown"
