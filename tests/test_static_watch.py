"""The daily static-vs-trades drift watch: alert on CHANGE, not on repeat."""
import app as app_module


def _res(*findings):
    return {"status": "ok", "finding_count": len(findings),
            "findings": list(findings)}


def _f(isin, settle, diff):
    return {"isin": isin, "settlement_date": settle, "diff_per100": diff}


def test_identical_finding_sets_share_a_signature():
    a = _res(_f("XS_A", "2026-07-15", -3.947), _f("XS_B", "2026-07-15", -3.668))
    # Order must not matter — the same defect reported in either order is
    # the same defect, and would otherwise re-alert every day.
    b = _res(_f("XS_B", "2026-07-15", -3.668), _f("XS_A", "2026-07-15", -3.947))
    assert app_module._finding_signature(a) == app_module._finding_signature(b)


def test_a_new_disagreeing_trade_changes_the_signature():
    a = _res(_f("XS_A", "2026-07-15", -3.947))
    b = _res(_f("XS_A", "2026-07-15", -3.947), _f("XS_C", "2026-08-01", -1.2))
    assert app_module._finding_signature(a) != app_module._finding_signature(b)


def test_a_changed_magnitude_on_the_same_trade_re_alerts():
    # Same bond and settlement, different size — the defect moved, so it is
    # news even though the bond list is unchanged.
    a = _res(_f("XS_A", "2026-07-15", -3.947))
    b = _res(_f("XS_A", "2026-07-15", -1.001))
    assert app_module._finding_signature(a) != app_module._finding_signature(b)


def test_clean_result_has_a_stable_empty_signature():
    assert app_module._finding_signature(_res()) == ""
    assert app_module._finding_signature({"status": "ok"}) == ""


def test_interval_is_hardcoded_not_env_driven():
    # Portability rule: a daily cadence is identical on Railway, the Guinness
    # box and a laptop, so it must not depend on an env var being present.
    assert app_module._STATIC_WATCH_INTERVAL == 24 * 3600
    assert app_module._STATIC_WATCH_FUNDS == ("gdbft",)
