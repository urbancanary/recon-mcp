"""GET /aum/funds — the route that gates whether Athena shows recon pages.

It 500'd in production because the handler referenced `aum_orchestrator`
without importing it (every other /aum route imports it locally). Nothing
caught it: no test called the route, and its only consumer degraded to
hiding the nav entry rather than erroring visibly.
"""
import os

import pytest
from fastapi.testclient import TestClient

import app as app_module


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("HETZNER_NOAUTH", "1")
    return TestClient(app_module.app)


def test_aum_funds_returns_200_not_500(client):
    r = client.get("/aum/funds")
    assert r.status_code == 200, r.text


def test_aum_funds_lists_the_funds_with_an_admin_format(client):
    import funds as _funds
    body = client.get("/aum/funds").json()
    assert body["funds"] == _funds.aum_funds()
    # Must be non-empty, or Athena hides every recon page and the failure
    # again looks like "no recon available" rather than a bug.
    assert body["funds"], "empty fund list silently disables the recon nav"
    assert "gdbf" in body["funds"]


def test_route_is_declared_before_the_parameterised_one(client):
    # FastAPI matches in definition order; if /aum/{fund} were declared
    # first it would swallow "funds" as a fund id and return a 404 payload
    # with HTTP 200-ish shape rather than the fund list.
    body = client.get("/aum/funds").json()
    assert "funds" in body and "error" not in body
