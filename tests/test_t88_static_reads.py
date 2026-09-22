"""T-88 (Andy 2026-09-22): calc inputs come from v_bond_static, never from the
bond_identity / bond_reference base tables directly."""
import re
from pathlib import Path

from recon_db import (IDENTITY_CALC_FIELDS, REFERENCE_CALC_FIELDS,
                      overlay_canonical_static)

ROOT = Path(__file__).resolve().parent.parent


def test_overlay_takes_calc_fields_from_the_view():
    row = {"isin": "X1", "coupon": 6.63, "maturity_date": "2036-05-16",
           "day_count": "30/360", "frequency": 1, "accrual_date": None,
           "ticker_description": "ROMANIA 6.63% 2036"}
    view = {"X1": {"isin": "X1", "coupon": 6.625, "maturity_date": "2036-05-16",
                   "day_count": "30E/360", "frequency": 2, "accrual_date": "2025-07-16"}}
    out = overlay_canonical_static(row, view, REFERENCE_CALC_FIELDS)
    assert out["coupon"] == 6.625 and out["frequency"] == 2 and out["day_count"] == "30E/360"
    assert out["ticker_description"] == "ROMANIA 6.63% 2036"   # display stays mirrored
    assert row["coupon"] == 6.63                                 # input not mutated


def test_overlay_keeps_row_when_view_has_no_row():
    row = {"isin": "X2", "coupon": 5.0}
    assert overlay_canonical_static(row, {}, IDENTITY_CALC_FIELDS) == row


def test_recalc_single_bond_reads_conventions_from_the_view():
    src = (ROOT / "app.py").read_text()
    block = src[src.index("async def recalc_single_bond"):]
    block = block[:block.index("\nasync def ", 10)] if "\nasync def " in block[10:] else block
    assert "/rest/v1/v_bond_static" in block
    assert not re.search(r"/rest/v1/bond_(reference|identity)", block)
