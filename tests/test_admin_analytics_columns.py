"""The admin writer's column contract (backlog id=3604).

`_store_admin_prices_to_bond_data` posts straight to PostgREST, so a column
name it gets wrong is accepted by the writer and simply never arrives: the
row lands price-only, wins the v_holdings_enriched lateral on its price_date,
and silently zeroes that position's accrued. Nothing in the write path can
catch that — the only thing that can is a test that pins the mapping against
the columns the other writers fill.

Shape under test: GA10 /api/v1/portfolio/analysis with format=FLDS.
"""
import asyncio

import pytest

import recon_engine


FLDS_ROW = {
    "isin": "XS0000000001",
    "status": "success",
    "accrued_interest": 1.3760274,
    "ytm": 7.9290982,
    "ytw": 7.40,
    "ytal": 7.55,
    "duration": 12.4826249,
    "duration_worst": 11.9,
    "yield_convention": "YTW",
}


class _Resp:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _Client:
    """Everything _calc_admin_analytics reaches for, in one stub."""

    def __init__(self, flds_rows, schedule):
        self._flds = flds_rows
        self._schedule = schedule

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        return _Resp({"bond_data": self._flds})

    async def get(self, url, headers=None, params=None):
        return _Resp(self._schedule)


@pytest.fixture
def stub_ga10(monkeypatch):
    """GA10 returns one FLDS row; the cashflow schedule knows the period."""
    schedule = [{"isin": "XS0000000001",
                 "start_date": "2026-02-15", "date": "2026-08-15"}]

    def _factory(flds_rows=None, schedule_rows=None):
        monkeypatch.setattr(
            recon_engine.httpx, "AsyncClient",
            lambda *a, **k: _Client(flds_rows or [FLDS_ROW],
                                    schedule_rows if schedule_rows is not None else schedule),
        )
        monkeypatch.setattr(
            recon_engine, "get_api_key", lambda *a, **k: "test-key", raising=False)

    import auth_client
    monkeypatch.setattr(auth_client, "get_api_key", lambda *a, **k: "test-key")
    return _factory


def _calc():
    return asyncio.run(recon_engine._calc_admin_analytics(
        {"XS0000000001": 98.5}, "2026-08-10"))


def test_accrued_lands_in_the_column_the_other_writers_fill(stub_ga10):
    # Not accrued_interest_c1: the engine's boundary check replayed 2026/08/10
    # and matched the stored row, which carries its accrued in the plain column.
    stub_ga10()
    row = _calc()["XS0000000001"]
    assert row["accrued_interest"] == 1.3760274
    assert "accrued_interest_c1" not in row


def test_yield_without_duration_is_not_the_shape(stub_ga10):
    # The reported defect: a row carrying a yield but no duration. Both must
    # be present, and duration_worst must be populated too — it is what the
    # recon view compares against BBG's duration.
    stub_ga10()
    row = _calc()["XS0000000001"]
    assert row["yield_to_maturity"] == 7.9290982
    assert row["yield_to_worst"] == 7.40
    assert row["modified_duration"] == 12.4826249
    assert row["duration_worst"] == 11.9
    for col in ("yield_to_maturity", "yield_to_worst",
                "modified_duration", "duration_worst"):
        assert row[col] is not None


def test_duration_worst_falls_back_to_the_plain_duration(stub_ga10):
    # GA10 does not always emit duration_worst. A NULL there is what makes
    # has_null_analytics true on a row that has perfectly good analytics.
    stub_ga10(flds_rows=[{**FLDS_ROW, "duration_worst": None}])
    row = _calc()["XS0000000001"]
    assert row["duration_worst"] == 12.4826249


def test_yield_to_worst_falls_back_to_ytm(stub_ga10):
    stub_ga10(flds_rows=[{**FLDS_ROW, "ytw": None}])
    assert _calc()["XS0000000001"]["yield_to_worst"] == 7.9290982


def test_accrual_date_is_resolved_from_the_coupon_schedule(stub_ga10):
    # A NULL accrual_period_start drops the row from the engine's
    # BETWEEN accrual_period_start AND accrual_period_end query no matter how
    # good the accrued is — so it is filled here, not left to a later recompute.
    stub_ga10()
    assert _calc()["XS0000000001"]["accrual_date"] == "2026-02-15"


def test_accrual_date_prefers_the_engine_when_it_returns_one(stub_ga10):
    stub_ga10(flds_rows=[{**FLDS_ROW, "accrual_date": "2026-03-01"}])
    assert _calc()["XS0000000001"]["accrual_date"] == "2026-03-01"


def test_a_missing_schedule_does_not_lose_the_analytics(stub_ga10):
    # The schedule is a nice-to-have. Losing analytics because the lookup
    # failed would be worse than a NULL accrual_date — that is the 2026-08-11
    # GB00BBQ33664 incident.
    stub_ga10(schedule_rows=[])
    row = _calc()["XS0000000001"]
    assert row["accrual_date"] is None
    assert row["accrued_interest"] == 1.3760274
    assert row["modified_duration"] == 12.4826249


def test_non_success_rows_are_still_skipped(stub_ga10):
    # GA10 emits populated-looking numbers off synthetic bond terms on these
    # (HK0000895927 comes back yield -0.77%, accrued 0.0). Storing them would
    # be worse than storing nothing.
    stub_ga10(flds_rows=[{**FLDS_ROW, "status": "error"}])
    assert _calc() == {}
