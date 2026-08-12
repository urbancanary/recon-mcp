"""Static-vs-trades validation: defect classification, exceptions, probe.

Figures are the measured GDBF 2026-07-15 settlements — every confirm
reconciles to ACT/365 annual, and our static did not.
"""
import static_validation as sv


def test_frequency_defect_detected_from_halved_period():
    # Vodafone: trade 6.991781 (8.000 x 319/365), ours 3.044444 (8 x 137/360).
    defect, why = sv._classify(6.991781, 3.044444)
    assert defect == "frequency"
    assert "ANNUAL" in why


def test_doubled_period_also_reads_as_frequency():
    defect, _ = sv._classify(3.044444, 6.991781)
    assert defect == "frequency"


def test_day_count_defect_detected_from_small_proportional_miss():
    # Bund: 1.647123 vs 1.650000 — a 360-vs-365 basis, not a date error.
    defect, why = sv._classify(1.647123, 1.650000)
    assert defect == "day_count"
    assert "1.0139" in why


def test_odd_ratio_falls_through_to_schedule_or_coupon():
    defect, _ = sv._classify(4.0, 5.6)
    assert defect == "schedule_or_coupon"


def test_zero_trade_accrued_is_indeterminate_not_a_finding():
    defect, _ = sv._classify(0.0, 1.23)
    assert defect == "indeterminate"


def test_exception_lookup_matches_isin_and_isin_at_date():
    sv.EXCEPTIONS.clear()
    sv.EXCEPTIONS["XS_A"] = "mis-booked, broker confirmed"
    sv.EXCEPTIONS["XS_B@2026-07-15"] = "one bad confirm only"
    try:
        assert sv._exception_for("XS_A", "2026-01-01") is not None
        assert sv._exception_for("XS_B", "2026-07-15") is not None
        # A dated exception must NOT suppress other settlements of that bond.
        assert sv._exception_for("XS_B", "2026-09-01") is None
        assert sv._exception_for("XS_C", "2026-07-15") is None
    finally:
        sv.EXCEPTIONS.clear()


def test_probe_is_green_only_when_nothing_disagrees():
    p = sv.probe({"status": "ok", "finding_count": 0, "agree": 12,
                  "bonds_affected": []})
    assert p["status"] == "green" and p["value"] == 0


def test_probe_is_amber_and_names_the_bonds():
    p = sv.probe({"status": "ok", "finding_count": 3, "agree": 9,
                  "bonds_affected": ["XS2630493570", "XS2731297235"]})
    assert p["status"] == "amber" and p["value"] == 3
    assert "XS2630493570" in p["detail"]
    assert "the static is wrong" in p["detail"]


def test_probe_is_red_when_the_check_could_not_run():
    # An unrun check must never look like a passing one.
    p = sv.probe({"status": "error", "error": "orca unreachable"})
    assert p["status"] == "red"
    assert "orca unreachable" in p["detail"]
