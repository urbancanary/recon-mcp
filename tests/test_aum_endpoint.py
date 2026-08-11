"""End-to-end /aum/{fund} against the orchestrator with the storage layer
faked onto local fixture files, and the auth gate exercised."""
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app as app_module
import aum_orchestrator as ao
from conftest import write_priced_view, write_reference_view, admin_parsed


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("HETZNER_NOAUTH", "1")
    priced = write_priced_view(tmp_path / "p.xlsx")
    ref = write_reference_view(tmp_path / "r.xlsx")
    payload = tmp_path / "2026-07-31.json"
    import json
    payload.write_text(json.dumps(admin_parsed()))

    reg = {
        ao.SRC_ADMIN: [{"file_path": "admin/x", "file_name": "nav_2026-07-31.xls",
                        "date": "2026-07-31", "uploaded_at": "t"}],
        ao.SRC_ADMIN_PAYLOAD: [{"file_path": str(payload),
                                "file_name": "2026-07-31.json",
                                "date": "2026-07-31", "uploaded_at": "t"}],
        ao.SRC_MAIA: [{"file_path": str(priced), "file_name": "p.xlsx",
                       "date": "2026-07-31", "uploaded_at": "t"}],
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
        # Athena's txn-valuation endpoint is fetched live in prod; tests
        # must not reach for auth-mcp/network. None → derived_from_admin.
        return None

    monkeypatch.setattr(ao, "_registry", fake_registry)
    monkeypatch.setattr(ao, "_cached", fake_cached)
    monkeypatch.setattr(ao, "_ga10_marks", no_marks)
    monkeypatch.setattr(ao, "_athena_txn_valuation", no_txn)
    return TestClient(app_module.app)


def test_aum_endpoint_full_payload(client):
    r = client.get("/aum/gdbf")
    assert r.status_code == 200
    body = r.json()
    for key in ("rows", "total", "notes", "integrity", "attribution",
                "evidence_report", "prices", "currency", "meta",
                "assessment", "compliance_file", "fx_forward_alert",
                "passes", "athena"):
        assert key in body, key
    assert body["meta"]["dates_match"] is True

    # Two-pass section: pass 1 is the top-level table; pass 2 is the
    # admin-priced revaluation and its Waystone identity must hold.
    assert body["athena"]["source"] == "derived_from_admin"
    passes = body["passes"]
    assert passes["own"]["rows"] == body["rows"]
    ap = passes["admin_priced"]
    assert ap["identity_check"]["holds"] is True
    p2 = {r["key"]: r for r in ap["rows"]}
    # Fixture book: Maia par == admin par, so pass 2 collapses the sides.
    assert p2["clean_bond_mv"]["maia"] == p2["clean_bond_mv"]["waystone"]
    assert p2["other_net"]["maia"] is None  # null, never zero
    # Fixture book agrees to the cent → not material.
    assert body["integrity"]["material"] is False
    assert body["attribution"]["available"] is True
    # Assessment: causes must sum exactly to the stated difference.
    a = body["assessment"]
    assert a["available"] is True
    assert abs(sum(c["amount"] for c in a["causes"]) - a["stated_difference"]) < 0.01
    # No compliance file in the fixture registry → flagged, never silent.
    assert body["compliance_file"]["supplied_for_date"] is False


def test_alias_resolution(client):
    assert client.get("/aum/DYN").status_code == 200
    assert client.get("/aum/nonsense").status_code == 404


def test_auth_gate(client, monkeypatch):
    monkeypatch.delenv("HETZNER_NOAUTH")
    assert client.get("/aum/gdbf").status_code == 401
    assert client.get("/funds").status_code == 401
    assert client.get("/ui/recon.html").status_code == 401
    # machine endpoints stay reachable (no gate) — health as the canary
    assert client.get("/health").status_code == 200
