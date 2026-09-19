"""The per-date candidate-file audit line: every stored Maia view for the
valuation date is reported with its own conclusion, not only the chosen one.
"""
import asyncio
import json
from pathlib import Path

import aum_orchestrator as ao
from conftest import write_priced_view, write_reference_view, admin_parsed


def test_candidate_views_reported_for_every_same_date_file(tmp_path, monkeypatch):
    priced_new = write_priced_view(tmp_path / "p_new.xlsx")
    priced_old = write_priced_view(tmp_path / "p_old.xlsx")
    ref = write_reference_view(tmp_path / "r.xlsx")
    payload = tmp_path / "2026-07-31.json"
    payload.write_text(json.dumps(admin_parsed()))

    reg = {
        ao.SRC_ADMIN: [{"file_path": "admin/x", "file_name": "nav.xls",
                        "date": "2026-07-31", "uploaded_at": "t"}],
        ao.SRC_ADMIN_PAYLOAD: [{"file_path": str(payload),
                                "file_name": "2026-07-31.json",
                                "date": "2026-07-31", "uploaded_at": "t"}],
        ao.SRC_MAIA: [{"file_path": str(priced_new), "file_name": "newer.xlsx",
                       "date": "2026-07-31", "uploaded_at": "2026-08-01"},
                      {"file_path": str(priced_old), "file_name": "older.xlsx",
                       "date": "2026-07-31", "uploaded_at": "2026-07-31"}],
        ao.SRC_MAIA_REF: [{"file_path": str(ref), "file_name": "r.xlsx",
                           "date": "2026-07-31", "uploaded_at": "t"}],
    }

    async def fake_registry(pid, source):
        return reg.get(source, [])

    async def fake_cached(row):
        p = Path(row["file_path"])
        return p if p.exists() else None

    async def no_marks(holdings, date):
        return {}

    async def no_txn(pid, date):
        return None

    monkeypatch.setattr(ao, "_registry", fake_registry)
    monkeypatch.setattr(ao, "_cached", fake_cached)
    monkeypatch.setattr(ao, "_ga10_marks", no_marks)
    monkeypatch.setattr(ao, "_athena_txn_valuation", no_txn)
    ao._result_cache.clear()

    res = asyncio.run(ao.build_aum_comparison("gdbf", "2026-07-31"))

    cv = res["candidate_views"]
    assert len(cv) == 2
    assert {c["file"] for c in cv} == {"newer.xlsx", "older.xlsx"}
    # Exactly one file is the one the verdict was built from, and it is the
    # newest upload for the date (newest-wins, not capability-ranked).
    selected = [c for c in cv if c["selected"]]
    assert len(selected) == 1 and selected[0]["file"] == "newer.xlsx"
    for c in cv:
        assert c["total"] is not None
        assert c["difference_pct"] is not None
        assert isinstance(c["material"], bool)
