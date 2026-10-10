"""bbg_parser must capture Mod Dur and OAD into separate dicts and record
which measure the primary duration came from — never coalesce the source
column away (backlog 5634)."""
from io import BytesIO

import openpyxl
import pytest

import bbg_parser

ISIN = "XS1709535097"
ACC = 100.0
MOD_DUR = 12.771
OAD = 9.18


def _export_bytes(headers, rows):
    """Minimal BBG-shaped workbook: 3 metadata rows, then the header row."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet0"
    ws.append(["Bloomberg Portfolio Export"])
    ws.append(["As of", "03/31/2026"])
    ws.append(["Base Ccy", "USD"])
    ws.append(list(headers))
    for row in rows:
        ws.append(list(row))
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_both_columns_captured_separately():
    parsed = bbg_parser.parse_bbg_export(
        _export_bytes(
            ["ISIN", "Acc Int", "Mod Dur", "OAD"],
            [[ISIN, ACC, MOD_DUR, OAD]],
        )
    )
    # Both measures survive — neither is discarded.
    assert parsed["mod_dur_bonds"] == {ISIN: MOD_DUR}
    assert parsed["oad_bonds"] == {ISIN: OAD}
    # Primary duration prefers mod_dur (unchanged from the old coalescing),
    # and now carries provenance.
    assert parsed["duration_bonds"] == {ISIN: MOD_DUR}
    assert parsed["duration_measure_bonds"] == {ISIN: "mod_dur"}
    assert parsed["duration_measure"] == "mod_dur"


def test_mod_dur_only_layout():
    parsed = bbg_parser.parse_bbg_export(
        _export_bytes(["ISIN", "Acc Int", "Mod Dur"], [[ISIN, ACC, MOD_DUR]])
    )
    assert parsed["mod_dur_bonds"] == {ISIN: MOD_DUR}
    assert parsed["oad_bonds"] == {}
    assert parsed["duration_bonds"] == {ISIN: MOD_DUR}
    assert parsed["duration_measure_bonds"] == {ISIN: "mod_dur"}
    assert parsed["duration_measure"] == "mod_dur"


def test_oad_only_layout():
    parsed = bbg_parser.parse_bbg_export(
        _export_bytes(["ISIN", "Acc Int", "OAD"], [[ISIN, ACC, OAD]])
    )
    assert parsed["oad_bonds"] == {ISIN: OAD}
    assert parsed["mod_dur_bonds"] == {}
    assert parsed["duration_bonds"] == {ISIN: OAD}
    assert parsed["duration_measure_bonds"] == {ISIN: "oad"}
    assert parsed["duration_measure"] == "oad"
