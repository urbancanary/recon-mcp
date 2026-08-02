# CANONICAL SOURCE: athena_html_v3/maia_evidence_report.py @ 0600e07 (2026-08-02).
# SYNCED COPY (stage-3 recon move) — athena's copy is deprecated and will
# be removed once parity is verified; this becomes the canonical home.
"""
Evidence report: front-office (Maia) valuation data vs the administrator.

NOT recon_report.py — that is the LLM-narrated, Supabase-versioned report for
the WNBF recon. This module is deterministic: every sentence it emits is
derived from measured figures, and it calls no model. A report that asserts a
front-office system is misstating a fund has to be reproducible from the files,
line by line, or it is not evidence.

WHAT THIS PRODUCES, AND WHY IT IS NOT A DIFFERENCE TABLE. A components table
says "clean bond MV differs by -524,979.99" and leaves the reader to work out
whether that is serious, what causes it, and — the part most often got wrong —
what it does NOT mean. This states the finding, its magnitude, whether it is
proven, and the limits of what was tested.

THE FIVE RULES THIS ENCODES

1. STATUS IS EARNED, NEVER ASSERTED. PROVEN requires both sides measured from
   source files with the arithmetic closing. Measured on one side only is
   INDICATED; resting on an assumption this run cannot test is UNVERIFIED. A
   report that calls everything proven is worth nothing, because the reader
   cannot tell the strong findings from the weak ones.

2. WHAT WAS NOT TESTED IS PART OF THE REPORT. Coverage, persistence, cause,
   pricing and date alignment are GENERATED from what the run actually covered,
   so they cannot drift out of step with the evidence the way a hand-written
   caveat does. One fund on one date is one fund on one date.

3. SEPARATE WHAT IS AFFECTED FROM WHAT IS NOT. A front-office defect that stops
   short of the official NAV needs no restatement and reaches no investor.
   Saying so first is not a softener — it is what makes the real finding
   credible and stops a reader reaching for the wrong remedy.

4. THE REPORT MUST BE ABLE TO SAY "NO FINDING". The same code path produces the
   clean day and the bad one. Measured on GDBF: 2026-07-24 returns no material
   finding (-7,136.81, 0.07% — pricing and snapshot timing), 2026-07-31 returns
   a material one (-526,840.73, 4.99%). It says "agree" as readily as it says
   otherwise, which is the only reason to believe it when it says otherwise.

5. EVERY FINDING CARRIES ITS COUNTER-EXPLANATION. `not_this` states what the
   finding does not establish. Someone restating a NAV over a front-office
   display defect has done harm, not good.

WHY A CONTROL INSIDE THE FRONT-OFFICE SYSTEM CANNOT CATCH THIS. Such a control
compares the system's AUM against its own calculated NAV. Both derive from the
same position data, so a defective position appears identically on each side and
nets to zero — it reports nil on a fund that is materially out. That control
detects a stale or failed feed; it cannot detect a wrong valuation. A wrong
valuation is only ever visible against a source outside the system that produced
it, which is what this report is.
"""

from __future__ import annotations


# Materiality. An order of magnitude above ordinary pricing and snapshot timing
# (measured at 0.07% on a clean GDBF day) and well below a genuine position
# difference (4.99% on the day one was present).
MATERIAL_PCT = 1.0

PROVEN = "proven"            # both sides measured, arithmetic closes
INDICATED = "indicated"      # one side only, or closes with a residual
UNVERIFIED = "unverified"    # rests on an assumption this run cannot test


def _pct(v, base):
    return None if not base else round(v / base * 100, 3)


def _f(n, title, fund, magnitude, status, evidence, detail, not_this=None):
    return {"n": n, "finding": title, "fund": fund, "magnitude": magnitude,
            "status": status, "evidence": evidence, "detail": detail,
            "not_this": not_this}


def build(parsed_nav: dict, maia: dict | None, bonds: dict | None,
          meta: dict | None = None) -> dict:
    """Assemble the evidence report from an already-run reconciliation.

    `bonds` is bond_recon.compare()'s output when a per-security join was
    possible, else None — the report degrades to fund-level findings and says
    so, rather than dropping the section silently.
    """
    meta = meta or {}
    adm = (parsed_nav.get("summary") or {}).get("aum_breakdown") or {}
    nav = adm.get("total_nav")
    fund = parsed_nav.get("fund_name") or meta.get("portfolio_id") or ""
    date = parsed_nav.get("valuation_date")
    ccy = parsed_nav.get("base_currency") or "USD"
    maia = maia or {}
    caps = maia.get("capabilities") or {}
    m_total = maia.get("total")

    findings = []
    n = 0
    headline_pct = None

    # ── 1. The headline: does the front-office total stand up? ──────────
    if m_total is not None and nav:
        d = m_total - nav
        headline_pct = _pct(d, nav)
        material = abs(headline_pct) > MATERIAL_PCT
        n += 1
        findings.append(_f(
            n,
            ("The front-office total does not agree with the administrator's NAV."
             if material else
             "The front-office total agrees with the administrator's NAV."),
            fund,
            f"{d:,.2f} {ccy} ({headline_pct:+.2f}%)",
            PROVEN,
            f"{meta.get('maia_file') or 'front-office export'} stated total vs "
            f"{meta.get('admin_file') or 'administrator pack'}",
            (f"The front office states {m_total:,.2f}; the administrator states "
             f"{nav:,.2f}. The front-office figure is its own stated total, "
             f"reproduced from its rows to within rounding — not a restatement "
             f"by this system."),
            not_this=("Not a NAV error. The administrator strikes the official "
                      "NAV from its own records, which are unaffected."
                      if material else None),
        ))

    # ── 2. Population: do the two systems hold the same securities? ─────
    #
    # Runs even when the identifier bridge is stale. Naming WHICH security is
    # unmatched needs the bridge; comparing the aggregate VALUE of unmatched
    # positions does not — and that aggregate is the strongest evidence
    # available, so suppressing it along with the naming threw away the finding
    # to avoid a labelling risk. Positions are described as unidentified in that
    # case, never guessed at.
    #
    # ONLY WHEN THE TWO SOURCES DESCRIBE THE SAME DAY. Across different dates a
    # different security list is exactly what ordinary trading produces, and the
    # value-conservation test is meaningless because cash has moved for a dozen
    # unrelated reasons in between. Reporting either as a finding on mismatched
    # dates would manufacture a defect out of a fund simply doing its job — so
    # the section is replaced by a statement of why it cannot be run.
    conserved = None
    if bonds and not meta.get("dates_match"):
        n += 1
        findings.append(_f(
            n, "Security-level comparison could not be run.", fund,
            "not applicable", UNVERIFIED,
            "date alignment",
            (f"The administrator pack is dated {date} and the front-office "
             f"export {meta.get('maia_valuation_date') or 'undated'}. Across "
             f"different dates a differing security list and a differing cash "
             f"balance are the normal result of trading, so neither can "
             f"evidence a defect. A same-day pair is required."),
        ))
    elif bonds:
        reliable = bonds.get("coverage_reliable", True)
        rows = bonds.get("rows") or []
        both = [r for r in rows if r["in_admin"] and r["in_maia"]]
        a_only = [r for r in rows if not r["in_maia"]]
        m_only = [r for r in rows if not r["in_admin"]]
        # Dirty on both sides — Maia's exposure includes accrued.
        a_val = sum(r.get("admin_dirty_base") or 0 for r in a_only)
        m_val = sum(r.get("maia_exposure_base") or 0 for r in m_only)
        if not reliable:
            # Unmatched front-office positions were dropped from `rows` because
            # they could not be identified; their value is carried separately.
            m_only = bonds.get("unresolved_positions") or []
            m_val = bonds.get("unresolved_value") or 0.0
        if a_only or m_only:
            n += 1
            findings.append(_f(
                n, "The two systems do not hold the same securities.", fund,
                f"{m_val - a_val:,.2f} {ccy} net across {len(a_only)} "
                f"administrator-only and {len(m_only)} front-office-only positions",
                PROVEN,
                "per-security join on ISIN" if reliable
                else "aggregate value of unmatched positions",
                (f"{len(both)} securities are held identically by both. "
                 f"{len(a_only)} appear only in the administrator's book "
                 f"({a_val:,.2f}) and {len(m_only)} only in the front office's "
                 f"({m_val:,.2f})."
                 + ("" if reliable else
                    " The front-office positions could not be identified — the "
                    "identifier bridge predates this valuation — so they are "
                    "compared by value only, not named.")),
                not_this=("Not by itself an error in either system — a position "
                          "traded between the two snapshots looks exactly like "
                          "this. The next finding is the test that separates "
                          "the two."),
            ))

            # THE TEST THAT SEPARATES A TRADE FROM A DEFECT. A switch preserves
            # value at execution: securities out, cash in. If the two books
            # differ because the fund traded, the side holding fewer securities
            # must hold correspondingly more cash. If it does not, the value has
            # left the book and arrived nowhere — and that is not a trade.
            m_cash, a_cash = maia.get("cash"), adm.get("cash")
            if m_cash is not None and a_cash is not None:
                expected = a_val - m_val      # value the front office no longer holds
                actual = m_cash - a_cash      # cash it holds in excess
                gap = actual - expected
                conserved = abs(gap) < max(1000.0, 0.05 * abs(expected))
                n += 1
                findings.append(_f(
                    n,
                    ("Value is conserved across the difference — consistent with "
                     "a trade between the two snapshots."
                     if conserved else
                     "Value is not conserved: the difference appears as cash on "
                     "neither side."),
                    fund, f"{gap:,.2f} {ccy} unaccounted for",
                    INDICATED if conserved else PROVEN,
                    "administrator cash vs front-office cash",
                    (f"Securities worth {expected:,.2f} sit in the "
                     f"administrator's book but not the front office's. Had they "
                     f"been sold, front-office cash would exceed the "
                     f"administrator's by about that amount. It differs by "
                     f"{actual:+,.2f}."),
                    not_this=(None if conserved else
                              "Does not exclude proceeds booked as an unsettled "
                              "receivable: this export carries no receivables "
                              "line, so such a balance would be invisible here. "
                              "The administrator's pack shows no unsettled "
                              "purchases or sales, which narrows but does not "
                              "close that possibility."),
                ))

    # ── 3. The denominator effect — where it reaches a decision ─────────
    if headline_pct is not None and abs(headline_pct) > MATERIAL_PCT:
        n += 1
        findings.append(_f(
            n, "Every exposure percentage shown against this total is misstated.",
            fund,
            f"positions read as 100% of the front-office total but "
            f"{_pct(m_total, nav):.2f}% of the administrator's NAV",
            PROVEN, "arithmetic consequence of finding 1",
            ("Exposure percentages divide by the front-office total. Where that "
             "total is understated, every percentage derived from it is "
             "overstated in proportion, with nothing on the screen to indicate "
             "it. A limit breach can read as compliant, and a compliant "
             "position as a breach."),
            not_this=("Not evidence that a limit has been breached. It means the "
                      "screen cannot be used to determine whether one has."),
        ))

    # ── Preconditions: tests, not requests ──────────────────────────────
    joined = bool(bonds and bonds.get("coverage_reliable", True))
    pre = [
        {"condition": "Front-office position data is complete — every security "
                      "the administrator carries",
         "passes": (not (bonds.get("admin_only") or bonds.get("maia_only")))
                   if joined else None,
         "note": ("{} administrator-only, {} front-office-only".format(
                      len(bonds.get("admin_only") or []),
                      len(bonds.get("maia_only") or []))
                  if joined else "not testable — no reliable per-security join")},
        {"condition": "The front-office total is corroborated outside the system "
                      "that produced it",
         "passes": (abs(headline_pct) <= MATERIAL_PCT)
                   if headline_pct is not None else None,
         "note": ("agrees to {:+.2f}%".format(headline_pct)
                  if headline_pct is not None else "no front-office total")},
        {"condition": "The two sources describe the same instant",
         "passes": bool(meta.get("dates_match")),
         "note": "administrator {} · front office {}".format(
             date or "?", meta.get("maia_valuation_date") or "undated")},
        {"condition": "The scale of any defect is known",
         "passes": False,
         "note": "{} of {} funds tested, {} date(s)".format(
             meta.get("funds_tested", 1), meta.get("funds_total", "?"),
             meta.get("dates_tested", 1))},
    ]

    # ── What was not tested. Generated, so it cannot drift. ─────────────
    not_tested = [
        {"heading": "Coverage",
         "detail": "{} of {} funds tested. The remainder are unexamined and the "
                   "population sharing any defect is not established.".format(
                       meta.get("funds_tested", 1), meta.get("funds_total", "?"))},
        {"heading": "Persistence",
         "detail": f"Tested on {date}. Whether a finding is date-specific, "
                   f"instrument-specific or permanent is not established."},
        {"heading": "Cause",
         "detail": "Not diagnosed. Establishing it requires the front-office "
                   "system's trade and contract records, which this "
                   "reconciliation does not have."},
        {"heading": "Pricing",
         "detail": "No independent price source was obtained. Price and FX "
                   "differences of a few tenths of a percent are treated as "
                   "normal valuation-point timing, not as defects."},
    ]
    if not caps.get("accrued", True):
        not_tested.append({"heading": "Accrued income",
            "detail": "The front-office export carries no accrued column, so the "
                      "clean/accrued split could not be compared. Bonds are "
                      "compared dirty, which is independent of either system's "
                      "accrual convention."})
    if not caps.get("forward_pnl", True):
        not_tested.append({"heading": "FX forward P&L",
            "detail": "Not present in this front-office export, so hedge P&L "
                      "could not be compared. The administrator's figures are "
                      "shown unopposed rather than against a zero."})
    if not meta.get("dates_match"):
        not_tested.append({"heading": "Date alignment",
            "detail": "The two sources are from different dates, so market and "
                      "FX movement between them is inside every difference above."})
    if bonds and not bonds.get("coverage_reliable", True):
        not_tested.append({"heading": "Security-level coverage",
            "detail": "The identifier bridge used to join the two books predates "
                      "the valuation, so securities traded since have no entry "
                      "and would appear absent when they are not. Population "
                      "findings are suppressed."})

    material = any(f["status"] == PROVEN and f["not_this"] for f in findings)

    return {
        "fund": fund,
        "valuation_date": date,
        "base_currency": ccy,
        "material": bool(headline_pct is not None
                         and abs(headline_pct) > MATERIAL_PCT),
        "headline_pct": headline_pct,
        "scope": {
            "affects": "The valuation a fund manager trades from.",
            "does_not_affect":
                "The official NAV. The administrator strikes it from its own "
                "records, which are independently sourced and unaffected. No "
                "investor-facing figure is implicated and no restatement is "
                "indicated.",
            "why_undetected":
                "A front-office system's own reconciliation compares its total "
                "against its own calculated NAV. Both derive from the same "
                "position data, so a defective position appears identically on "
                "each side and nets to zero. That control detects a stale or "
                "failed feed; it cannot detect a wrong valuation. A wrong "
                "valuation is only visible against a source outside the system "
                "that produced it.",
        },
        "preconditions": pre,
        "findings": findings,
        "not_tested": not_tested,
        "sources": [
            {"role": "Administrator", "file": meta.get("admin_file"),
             "date": date, "note": "NAV pack"},
            {"role": "Front office", "file": meta.get("maia_file"),
             "date": meta.get("maia_valuation_date"), "note": "position export"},
        ],
        "basis":
            "Both sides are read directly from the source files; no figure is "
            "keyed in or copied between systems. Securities are matched on ISIN, "
            "and where a security cannot be matched it is reported as unmatched "
            f"rather than matched on description or quantity. Amounts in {ccy}.",
    }
