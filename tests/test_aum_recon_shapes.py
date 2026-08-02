"""All three Maia export shapes must parse header-driven, with honest
capability flags — and a non-Maia file must be REJECTED, not read as zero."""
import openpyxl
import pytest

import aum_recon
from conftest import BOOK, bond_exposure


def _expected_dirty():
    return sum(bond_exposure(q, p, a, f) for _, _, _, _, q, p, a, f in BOOK["bonds"])


def test_priced_shape(priced_view):
    b = aum_recon.maia_breakdown(priced_view)
    assert b["counts"]["bonds"] == 2
    caps = b["capabilities"]
    assert caps["accrued"] and caps["forward_pnl"] and caps["price"]
    assert not caps["isin"]
    assert b["bonds_dirty"] == pytest.approx(_expected_dirty(), abs=0.01)
    # clean = dirty − accrued(base): 5,000×1.0 + 2,000×1.10
    assert b["clean_bond_mv"] == pytest.approx(_expected_dirty() - 7200.0, abs=0.01)
    assert b["fx_forward_pnl"] == pytest.approx(-3000.0)
    assert b["cash"] == pytest.approx(100_000 + 50_000 * 1.10, abs=0.01)


def test_allocation_shape_has_no_accrued(allocation_view):
    b = aum_recon.maia_breakdown(allocation_view)
    assert b["counts"]["bonds"] == 2
    assert not b["capabilities"]["accrued"]
    assert b["clean_bond_mv"] is None          # never invented
    assert b["bonds_dirty"] == pytest.approx(_expected_dirty(), abs=0.01)


def test_full_shape_carries_isin(reference_view):
    b = aum_recon.maia_breakdown(reference_view)
    assert b["capabilities"]["isin"]
    assert b["counts"]["bonds"] == 2
    isins = {r["isin"] for r in b["bond_rows"]}
    assert isins == {"US0000000001", "DE0000000002"}


def test_fx_rates_derived_from_cash_rows(priced_view):
    b = aum_recon.maia_breakdown(priced_view)
    assert b["fx_rates"]["USD"] == 1.0
    assert b["fx_rates"]["EUR"] == pytest.approx(1.10)


def test_wrong_shape_rejected_not_zero(tmp_path):
    """A spreadsheet with none of the Maia fields must raise — parsing
    'successfully' to zero rows once put -10.5m on the page."""
    p = tmp_path / "junk.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet0"
    ws.append(["Colour", "Animal"])
    ws.append(["red", "fox"])
    wb.save(p)
    with pytest.raises(ValueError):
        aum_recon.maia_breakdown(str(p))


def test_forward_plausibility_guard(tmp_path):
    """A 'forward P&L' bigger than 25% of the bond book is gross legs, not
    P&L — it must be zeroed and flagged, never reported as hedge P&L."""
    from conftest import write_priced_view
    import conftest
    saved = conftest.BOOK["forwards"]
    conftest.BOOK["forwards"] = [("EURUSD Fwd", -20_000_000.0, "15/09/2026")]
    try:
        p = write_priced_view(tmp_path / "gross.xlsx")
        b = aum_recon.maia_breakdown(str(p))
        assert b["forwards_unreliable"] is True
        assert b["fx_forward_pnl"] in (None, 0.0)
    finally:
        conftest.BOOK["forwards"] = saved
