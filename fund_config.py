"""
Per-fund static configuration — currently the VALUATION POINT.

Every fund strikes its NAV at a declared point in the day. GDBF values at
midday; WNBF and GCRIF value at close of play. Athena has to say which, because
"the price for 2026-07-24" means a different thing for a midday fund than for a
close fund, and the admin recon has to compare like with like.

WHY THIS IS HARDCODED (deliberately, not laziness): a fund's valuation point is
the same on Railway, on the Guinness Hetzner box, and on a laptop. It is fund
reference data, not environment configuration. Per the portability rule, an
env var here would be pure liability — Railway wipes them on redeploy and the
box never had them, so the app would silently lose a fund's valuation point.
Change a valuation point by editing this file.

SCOPE — this module DECLARES and LABELS the valuation point. It does not (yet)
select a different intraday price, because there is no SECOND intraday price to
select between: every price column in bond-data is `price_date date`, one price
per bond per day (verified 2026-07-25). That is not the same as "no time is known" —
Waystone's own report header states a VALUATION TIME per fund (nav_parser.py
parses it into `valuation_time` and cross-checks it against the declaration
below on every ingest), it is just a single fund-level stamp, not a per-bond
choice of snapshot. Actually strikes-two-different-times pricing needs
ga10-pricing to capture and tag more than one snapshot per day first — see
codebase-mcp backlog 2391.
"""

# Valuation points. Times are LOCAL to the stated timezone.
VALUATION_POINTS = {
    "midday": {
        "key": "midday",
        "label": "Midday",
        "short": "MIDDAY",
        "local_time": "12:00",
        "timezone": "Europe/Dublin",
    },
    "close": {
        "key": "close",
        "label": "Close of play",
        "short": "CLOSE",
        "local_time": "23:00",
        "timezone": "Europe/Dublin",
    },
}

# portfolio_id -> valuation point key.
#
# VERIFIED 2026-07-25 against the administrator's own reports, not just asserted.
# Waystone/InvestOne stamps a `VALUATION TIME:` cell on the
# Share_Class_Price_Report / Detailed_Security_Valuation / OpenCurrency sheets:
#   GDBF  -> 12:00:00   (data/nav_uploads/gdbf/latest.xls, fund no. 10234)
#   GCRIF -> 23:00:00   (fund no. 10194; identical across 18 sampled reports
#                        spanning 2025-06 to 2026-07, zero variance)
# Every one of the 18 other Guinness funds in the 21.07.2026 NAV zip also reads
# 23:00:00, so 23:00 is the house default and GDBF's midday is the genuine
# exception. nav_parser.py now extracts this cell and cross-checks it against the
# values below on every ingest, logging "VALUATION POINT MISMATCH" if they drift.
#
# TIMEZONE is Europe/DUBLIN, per the fund's own constitutional document. The GDBF
# supplement (15 June 2026, SharePoint GAM-Hub/.../Fund documents/current/) defines
#   '"Valuation Point" means 12.00 p.m. (Irish time) on each Dealing Day'
# and a Dealing Deadline of 11.00 a.m. Irish time "no later than the Valuation
# Point". That independently confirms GDBF's midday 12:00 alongside the
# administrator's VALUATION TIME cell — two sources agreeing.
# Ireland and the UK share the same offsets year-round (GMT / +1), so no computed
# value changes; the label was previously Europe/London, which was wrong for audit.
# Prior reasoning retained below, since it still evidences the CLOCK the admin uses:
# each workbook carries
# RUN DATE on two clocks exactly 5h apart (GDBF 08:50 vs 13:51 on 2026-07-22;
# GCRIF 06:46 vs 11:47 on 2026-01-26) — an EST/EDT report-generation stamp and a
# London one. GDBF's report is generated 13:51 London against a 12:00 valuation
# point, i.e. ~1h50m after it; were that 12:00 New York the report would predate
# its own valuation point, which is impossible. Nothing anywhere suggests an
# Asian time base, so GCRIF is London-23:00 despite being CNH-denominated.
#
# WNBF is the one value still only user-stated: it has no Waystone report (it is
# absent from the fund zip and FUND_MAP; only Bloomberg/PRTU uploads exist), so
# its 23:00 is inferred from the house default.
FUND_VALUATION_POINT = {
    "wnbf": "close",
    "gcrif": "close",
    "gdbf": "midday",
    "emwnbf": "close",
}

# Funds not in the map above fall back to this. Close is the house default:
# midday is the exception, and defaulting an unknown fund to the exception would
# silently mislabel it.
DEFAULT_VALUATION_POINT = "close"


def valuation_point(portfolio_id: str) -> dict:
    """Valuation point for a fund, as a dict (never None).

    Includes `is_default` so a caller can tell a declared point from a fallback
    — an unrecognised fund should be visibly assumed, not quietly asserted.
    """
    pid = (portfolio_id or "").strip().lower()
    key = FUND_VALUATION_POINT.get(pid)
    point = dict(VALUATION_POINTS[key or DEFAULT_VALUATION_POINT])
    point["is_default"] = key is None
    return point


def all_valuation_points() -> dict:
    """{portfolio_id: valuation_point} for every fund we know about."""
    return {pid: valuation_point(pid) for pid in FUND_VALUATION_POINT}
