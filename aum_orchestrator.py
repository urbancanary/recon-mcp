"""AUM reconciliation orchestrator — the recon-mcp home of what used to be
Athena's GET /api/nav-comparison body (athena_html_v3/server.py:3047-3336,
ported as a unit @ 0600e07; the hard-won selection/date-matching rules in the
comments came with it).

Three-way NAV comparison per fund: Maia | Athena(GA10 marks) | Waystone
(administrator), plus per-bond breaks, par/price/FX attribution, deterministic
evidence report and currency-hedge section.

FILE SOURCES differ from Athena's local-disk version — everything comes from
Supabase Storage via the recon_uploads registry (the same store the /upload/*
endpoints write), cached on local disk per process:

    maia_aum/{fund}/{date}.xlsx      Maia position/priced/allocation views
    maia_ref/{fund}/{date}.xlsx      Maia views carrying ISIN+Ticker (bridge)
    admin/{fund}/{date}.xls          raw administrator pack
    admin_payload/{fund}/{date}.json full parsed payload (hedge_ledger etc.)

so there is no filename guessing: files are keyed by their RESOLVED data date
at upload time (aum upload endpoint dates Maia files via _maia_as_of; admin
packs via the parser's valuation_date).
"""

import asyncio
import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import httpx

import accrual_boundary
import aum_passes
import aum_recon
import bond_recon
import currency_hedge_recon
import maia_evidence_report
import nav_comparison
import nav_parser
import recon_attribution
import recon_db
from funds import FUNDS, aum_funds

logger = logging.getLogger(__name__)

CACHE_DIR = Path(tempfile.gettempdir()) / "recon_mcp_aum_cache"

# Sources in the recon_uploads registry owned by this module. The legacy
# "maia" source (bond-level rows for wnbf/gcrif) is deliberately separate.
SRC_MAIA = "maia_aum"
SRC_MAIA_REF = "maia_ref"
SRC_ADMIN = "admin"
SRC_ADMIN_PAYLOAD = "admin_payload"


# ── Storage-backed file cache ───────────────────────────────────────────────

async def _cached(row: dict) -> Path | None:
    """Local path for a registry row's file, downloading once per process."""
    fp = row.get("file_path") or ""
    if not fp:
        return None
    local = CACHE_DIR / fp
    if local.exists() and local.stat().st_size > 0:
        return local
    data = await recon_db.download_raw_file(fp)
    if not data:
        return None
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    return local


async def _registry(pid: str, source: str) -> list[dict]:
    rows = await recon_db.list_uploads(portfolio_id=pid, source=source)
    return [r for r in rows if r.get("parse_status") != "error"]


# ── Maia ingestion (called by the upload endpoint) ─────────────────────────

async def ingest_maia(pid: str, file_bytes: bytes, filename: str,
                      uploaded_by: str) -> dict:
    """Validate, date and store a Maia export for the AUM recon.

    Rejects files that parse to zero bonds (wrong shape) — 'parsing without
    error is not the same as parsing something'. Dates the file from its own
    contents (_maia_as_of), never the filename. Files carrying ISIN+Ticker
    are additionally registered as reference (bridge) views.
    """
    if pid not in FUNDS:
        return {"status": "error", "error": f"unknown fund {pid}"}

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tf:
        tf.write(file_bytes)
        tmp = tf.name
    try:
        try:
            breakdown = aum_recon.maia_breakdown(tmp)
        except Exception as e:
            return {"status": "error", "error": f"not a readable Maia export: {e}"}
        bonds = (breakdown.get("counts") or {}).get("bonds") or 0
        if not bonds:
            return {"status": "error",
                    "error": "parsed 0 bonds — wrong export shape"}
        date = None
        try:
            date = recon_attribution._maia_as_of(tmp)
        except Exception:
            pass
        if not date:
            return {"status": "error",
                    "error": "could not resolve the export's data date"}

        caps = breakdown.get("capabilities") or {}
        path = await recon_db.store_raw_upload(
            SRC_MAIA, pid, date, file_bytes, filename, uploaded_by,
            bonds_parsed=bonds)
        stored_as = [path]
        if caps.get("isin"):
            # Same bytes registered a second time as a bridge view: the
            # 34-col priced shape has no ISIN and needs one of these to join.
            stored_as.append(await recon_db.store_raw_upload(
                SRC_MAIA_REF, pid, date, file_bytes, filename, uploaded_by,
                bonds_parsed=bonds))
        return {"status": "ok", "portfolio_id": pid, "date": date,
                "bonds": bonds, "capabilities": caps,
                "forwards_unreliable": breakdown.get("forwards_unreliable"),
                "stored": [p for p in stored_as if p]}
    finally:
        Path(tmp).unlink(missing_ok=True)


async def ingest_admin_payload(pid: str, parsed: dict) -> str:
    """Store the FULL parsed admin payload (hedge_ledger, share classes,
    aum_breakdown...) — recon_admin rows are bond-level only and cannot feed
    the AUM comparison. Keyed by the pack's own valuation_date."""
    date = parsed.get("valuation_date")
    if not date:
        return ""
    body = json.dumps(parsed, default=str).encode()
    return await recon_db.upload_to_storage(
        SRC_ADMIN_PAYLOAD, pid, date, body, f"{date}.json")


# ── GA10 marks (the independent third leg) ─────────────────────────────────

_GAE_URL = "https://future-footing-414610.uc.r.appspot.com"  # same as recon_engine


async def _ga10_batch(prices: dict, settle_iso: str, api_key: str) -> dict:
    """One GA10 GAE portfolio/analysis call → {isin: bond_data row}.

    RESILIENT TO POISON BONDS: the GAE batch dies wholesale ('NoneType is
    not iterable') when ANY bond lacks reference data — 7 GDBF corps did
    exactly that and blanked all 28 marks. On failure the batch splits in
    half recursively, so every priceable bond still gets a mark and only
    the genuinely un-enrolled ones drop out.
    """
    if not prices:
        return {}
    inv_date = settle_iso.replace("-", "/")
    payload = {"format": "FLDS", "data": [
        {"BOND_CD": i, "CLOSING PRICE": float(p), "WEIGHTING": 1.0,
         "Inventory Date": inv_date} for i, p in prices.items()]}
    async def _post():
        async with httpx.AsyncClient(timeout=120.0) as c:
            return await c.post(f"{_GAE_URL}/api/v1/portfolio/analysis",
                                json=payload,
                                headers={"Content-Type": "application/json",
                                         "X-API-Key": api_key})
    try:
        r = await _post()
        if r.status_code >= 500 and len(prices) == 1:
            # A lone bond's failure may be transient GAE load rather than
            # missing reference data — one retry before writing it off.
            await asyncio.sleep(1.0)
            r = await _post()
    except Exception as e:
        logger.warning("GA10 batch failed (%d bonds): %s", len(prices), e)
        return {}
    if r.status_code == 200:
        return {b["isin"]: b for b in (r.json().get("bond_data") or [])
                if b.get("isin")}
    if len(prices) == 1:
        logger.info("GA10 cannot price %s (not enrolled?)", next(iter(prices)))
        return {}
    # SEQUENTIAL halves, not gather(): the recursive fan-out in parallel
    # (× two settle dates) overloaded GAE and flaked good bonds — coverage
    # dropped from 21 to 11 of 28. Slower but complete beats fast but wrong.
    items = list(prices.items())
    mid = len(items) // 2
    left = await _ga10_batch(dict(items[:mid]), settle_iso, api_key)
    right = await _ga10_batch(dict(items[mid:]), settle_iso, api_key)
    return {**left, **right}


# GA10 marks are deterministic for a stored pack (same ISINs, same admin
# prices, same settle date), but computing them costs 20s+ — two settle
# dates, sequential batching, and the un-enrolled bonds force a re-split on
# EVERY call. Cache per (settle date, price set); ~a few KB per valuation.
_marks_cache: dict = {}


async def _ga10_marks(holdings: list[dict], settle_iso: str) -> dict:
    """{isin: {clean_price, accrued_per_100, accrued_per_100_c1, ...}} from
    GA10 at the admin's own prices, at TWO settlement dates:

      T+0  the valuation date itself
      C+1  calendar day after — the accrual point that matches Waystone's
           market-settlement convention on the funds measured so far (the
           recon views show C+1 only for the same reason).

    GA10's accrued is an INDEPENDENT calculation, not a restatement of the
    administrator's — which is what makes the accrued comparison genuinely
    three-way. Best-effort: any failure returns {} and the page says so.
    """
    from datetime import datetime, timedelta
    from auth_client import get_api_key
    prices = {h["isin"]: h.get("price") for h in holdings
              if h.get("isin") and h.get("price")}
    if not prices:
        return {}
    cache_key = (settle_iso, tuple(sorted(prices.items())))
    hit = _marks_cache.get(cache_key)
    if hit is not None:
        return hit
    api_key = get_api_key("GA10_API_KEY", requester="recon-mcp-aum")
    if not api_key:
        return {}
    c1 = (datetime.strptime(settle_iso, "%Y-%m-%d")
          + timedelta(days=1)).strftime("%Y-%m-%d")
    # Sequential, same reason as the sequential halves in _ga10_batch.
    t0_map = await _ga10_batch(prices, settle_iso, api_key)
    c1_map = await _ga10_batch(prices, c1, api_key)
    out = {}
    for isin in prices:
        t0, c1b = t0_map.get(isin), c1_map.get(isin)
        if not t0 and not c1b:
            continue
        b = t0 or c1b
        out[isin] = {
            "clean_price": prices.get(isin),
            "accrued_per_100": (t0 or {}).get("accrued_interest"),
            "accrued_per_100_c1": (c1b or {}).get("accrued_interest"),
            "ytw": b.get("yield") or b.get("ytm"),
            "duration": b.get("duration"),
            "coupon": b.get("coupon"),
            "analytics_source": "GA10",
        }
    # Only cache a run that actually produced marks — an empty result from
    # a GAE outage must not stick until restart.
    if out:
        if len(_marks_cache) > 64:
            _marks_cache.clear()
        _marks_cache[cache_key] = out
    return out


# ── Athena transactions valuation (the injected side) ──────────────────────
#
# Athena is building GET /api/internal/txn-valuation/{fund}?date=YYYY-MM-DD
# (service-key protected: X-Service-Key = hex HMAC-SHA256(ATHENA_SERVICE_KEY,
# b"athena-internal"), confirmed 2026-08-10, Athena commit c9b1a87). Until it
# is DEPLOYED the fetch fails and the Athena side stays derived_from_admin —
# the seam exists so flipping to the real side is a data change, not a code
# change. `athena.source` in the payload says which one the reader is seeing.

async def _athena_txn_valuation(pid: str, date: str | None) -> dict | None:
    """Fetch Athena's own transactions-book valuation, or None (→ fallback
    to derived_from_admin). Never raises; never partially succeeds silently —
    a payload without holdings+totals is treated as absent."""
    if not date:
        return None
    import hashlib
    import hmac as _hmac
    try:
        from auth_client import get_api_key, get_service_url
        # ATHENA_API_URL is the PRE-EXISTING auth-mcp key for the Athena app
        # (resolves to athena.guinness.x-trillion.com; live-verified
        # 2026-08-11 serving this endpoint) — reuse it, don't mint a
        # duplicate ATHENA_URL.
        base = (get_service_url("ATHENA_API_URL") or "").rstrip("/")
        key = get_api_key("ATHENA_SERVICE_KEY") or ""
        if not base or not key:
            return None
        token = _hmac.new(key.encode(), b"athena-internal",
                          hashlib.sha256).hexdigest()
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(f"{base}/api/internal/txn-valuation/{pid}",
                            params={"date": date},
                            headers={"X-Service-Key": token})
        if r.status_code != 200:
            logger.info("athena txn-valuation %s %s: HTTP %s — using "
                        "derived_from_admin", pid, date, r.status_code)
            return None
        body = r.json()
        if not (body.get("holdings") and body.get("totals")):
            logger.warning("athena txn-valuation %s %s: incomplete payload "
                           "— using derived_from_admin", pid, date)
            return None
        return body
    except Exception as e:
        logger.info("athena txn-valuation unavailable (%s %s): %s", pid, date, e)
        return None


# ── Date inventory ──────────────────────────────────────────────────────────

async def available_dates(pid: str) -> dict:
    """Admin and Maia dates on record, plus which pairs line up."""
    admin_rows, maia_rows = await asyncio.gather(
        _registry(pid, SRC_ADMIN), _registry(pid, SRC_MAIA))
    admin_dates = sorted({r["date"] for r in admin_rows if r.get("date")},
                         reverse=True)
    maia_dates = sorted({r["date"] for r in maia_rows if r.get("date")},
                        reverse=True)
    return {"portfolio_id": pid,
            "admin_dates": admin_dates,
            "maia_dates": maia_dates,
            "matched_dates": [d for d in maia_dates if d in set(admin_dates)]}


# ── The comparison itself ───────────────────────────────────────────────────

# Assembled-report cache. The payload is deterministic given the exact set
# of stored files, so the key is (fund, anchor date) and the value carries a
# registry FINGERPRINT — any new/replaced upload for the fund changes the
# fingerprint and the next request rebuilds. Measured: cold build 112s
# (GA10 batching dominates), fingerprint hit ~1s.
_result_cache: dict = {}
_RESULT_TTL = 6 * 3600


def _registry_fingerprint(*reg_lists) -> tuple:
    return tuple(sorted(
        (r.get("file_path") or "", r.get("uploaded_at") or "")
        for reg in reg_lists for r in reg))


async def build_aum_comparison(pid: str, date: str | None = None) -> dict:
    """Full AUM comparison payload for a fund.

    THE ADMINISTRATOR'S CALENDAR ANCHORS THE REPORT: ``date`` (or, by
    default, the latest stored Waystone pack) picks the valuation date, and
    the Maia side joins ONLY if a view exists for that same date. Never a
    cross-date comparison — on an Irish bank holiday Waystone strikes no
    NAV, and defaulting to the newest Maia date used to pair a fresh Maia
    file against an older pack and report days of market movement as
    "difference". No same-day Maia → administrator-only view, said plainly.
    """
    if pid not in FUNDS or pid not in aum_funds():
        return {"error": f"fund {pid} has no AUM reconciliation", "status": 404}

    admin_reg, maia_reg, ref_reg = await asyncio.gather(
        _registry(pid, SRC_ADMIN), _registry(pid, SRC_MAIA),
        _registry(pid, SRC_MAIA_REF))
    if not admin_reg:
        return {"error": f"no stored administrator valuation for {pid}",
                "status": 404}

    admin_dates = sorted({r.get("date") for r in admin_reg if r.get("date")},
                         reverse=True)
    anchor = date or admin_dates[0]
    if anchor not in admin_dates:
        return {"error": f"no administrator valuation for {anchor} — "
                         f"available: {', '.join(admin_dates[:8])}",
                "status": 404}

    import time as _time
    fp = _registry_fingerprint(admin_reg, maia_reg, ref_reg)
    hit = _result_cache.get((pid, anchor))
    if hit and hit[0] == fp and (_time.time() - hit[1]) < _RESULT_TTL:
        return hit[2]

    # ── Maia side: SAME DATE ONLY. Files are stored under their RESOLVED
    # data date; among same-date shapes the newest upload wins (the richer
    # re-export typically arrives later — proper capability ranking is
    # backlog 2526).
    maia = maia_breakdown = None
    maia_meta = {"available": False,
                 "error": f"no Maia view dated {anchor} on record"}
    maia_rows_bonds = []
    maia_date = None
    maia_path = None

    cand = [r for r in maia_reg if r.get("date") == anchor]
    cand.sort(key=lambda r: (r.get("date") or "", r.get("uploaded_at") or ""),
              reverse=True)
    for row in cand:
        p = await _cached(row)
        if not p:
            continue
        try:
            b = aum_recon.maia_breakdown(str(p))
        except Exception as e:
            logger.warning("aum: Maia view %s unusable: %s", row.get("file_name"), e)
            maia_meta = {"available": False,
                         "error": f"{row.get('file_name')}: {e}"}
            continue
        if not (b.get("counts") or {}).get("bonds"):
            maia_meta = {"available": False,
                         "error": f"{row.get('file_name')}: parsed 0 bonds"}
            continue
        maia_breakdown = b
        maia_date = row.get("date")
        maia_path = p
        maia_meta = {"available": True, "file": row.get("file_name"),
                     "valuation_date": maia_date,
                     "counts": b.get("counts"), "fx_rates": b.get("fx_rates")}
        maia_rows_bonds = b.get("bond_rows") or []
        break

    # ── Administrator pack: same date as the Maia view actually used, else
    # the newest on record (or the explicitly requested date).
    admin_row = next(r for r in admin_reg if r.get("date") == anchor)
    parsed = None
    payload_rows = await _registry(pid, SRC_ADMIN_PAYLOAD)
    pay = next((r for r in payload_rows if r.get("date") == admin_row.get("date")), None)
    if pay:
        p = await _cached(pay)
        if p:
            try:
                parsed = json.loads(p.read_text())
            except Exception:
                parsed = None
    if parsed is None:
        p = await _cached(admin_row)
        if not p:
            return {"error": "administrator pack could not be fetched from storage",
                    "status": 502}
        try:
            parsed = nav_parser.parse_nav_report(str(p))
        except Exception as e:
            logger.error("aum: admin parse failed for %s: %s", pid, e, exc_info=True)
            return {"error": f"admin valuation parse failed: {e}", "status": 500}

    if maia_breakdown is not None:
        owner_map = nav_comparison._owner_by_settlement(parsed.get("fx_forwards") or [])
        maia = nav_comparison.maia_side(maia_breakdown, owner_map,
                                        maia_breakdown.get("forwards"))

    waystone = nav_comparison.waystone_side(parsed)
    athena_txn = await _athena_txn_valuation(pid, parsed.get("valuation_date"))
    if athena_txn:
        athena = aum_passes.athena_txn_to_side(athena_txn)
    else:
        athena = nav_comparison.athena_side(parsed)
        athena["source"] = "derived_from_admin"

    result = nav_comparison.build(maia or {}, athena, waystone, meta={
        "portfolio_id": pid,
        "fund_name": parsed.get("fund_name") or FUNDS[pid]["name"],
        "valuation_date": parsed.get("valuation_date"),
        "valuation_time": parsed.get("valuation_time"),
        "base_currency": parsed.get("base_currency"),
        "admin_file": admin_row.get("file_name"),
        "maia": maia_meta,
        "dates_match": bool(maia_date and parsed.get("valuation_date") == maia_date),
        "maia_valuation_date": maia_date,
    })
    result["prices"] = nav_comparison.price_comparison(parsed, maia_rows_bonds)

    # ── Materiality verdict on the TOTALS (cash-based tests false-positive:
    # Maia's cash includes unsettled forward legs, the administrator's does
    # not). 1% sits an order of magnitude above pricing/timing noise (0.07%
    # measured 24 Jul) and well below a real break (4.99% on 31 Jul).
    if maia_breakdown is not None:
        m_tot = maia_breakdown.get("total")
        a_tot = (parsed.get("summary") or {}).get("aum_breakdown", {}).get("total_nav")
        if m_tot is not None and a_tot:
            d = m_tot - a_tot
            pct = d / a_tot * 100
            result["integrity"] = {
                "maia_total": round(m_tot, 2),
                "admin_nav": round(a_tot, 2),
                "difference": round(d, 2),
                "difference_pct": round(pct, 3),
                "material": abs(pct) > 1.0,
                "threshold_pct": 1.0,
                "verdict": (
                    "Maia's stated total is {:,.2f} {} the administrator's NAV "
                    "({:+.2f}%). This is larger than pricing differences and the "
                    "gap between the two snapshots can account for, and it is "
                    "Maia's own figure, not a restatement. The administrator's "
                    "valuation should be used.".format(
                        abs(d), "below" if d < 0 else "above", pct)
                    if abs(pct) > 1.0 else
                    "Maia and the administrator agree to {:+.2f}% ({:,.2f}). "
                    "Consistent with pricing and snapshot timing.".format(pct, d)),
            }

    # ── Attribution + per-bond joins need a reference (ISIN) view. Refuse the
    # join rather than fuzzy-match when the bridge date does not line up —
    # a stale bridge reports absence as fact (the 31 Jul -2.88m phantom).
    ref_row = next((r for r in ref_reg if r.get("date") == maia_date), None) \
        or (max(ref_reg, key=lambda r: r.get("date") or "") if ref_reg else None)
    ref_path = await _cached(ref_row) if ref_row else None
    result["attribution"] = {"available": False}
    _bonds = None
    if ref_path and maia_path:
        try:
            result["attribution"] = {
                "available": True,
                "reference_file": ref_row.get("file_name"),
                **recon_attribution.attribute(parsed, str(maia_path), str(ref_path)),
            }
        except Exception as e:
            logger.warning("aum: attribution failed: %s", e)
            result["attribution"] = {"available": False, "error": str(e)}
        try:
            _bonds = bond_recon.compare(parsed, str(maia_path), str(ref_path))
        except Exception as e:
            logger.warning("aum: per-security join unavailable: %s", e)
    elif not ref_path:
        result["attribution"] = {
            "available": False,
            "error": "no Maia view with an ISIN column — per-bond join needs "
                     "one to bridge Ticker to ISIN",
        }
    result["bonds"] = _bonds

    # ── THE ATHENA SIDE'S PROVENANCE, stated at top level so the UI can
    # label honestly: "transactions" is Athena's own book; "derived_from_
    # admin" is the administrator re-presented, agreement by construction.
    result["athena"] = {
        "source": athena.get("source"),
        "coverage": athena.get("coverage"),
        "caveats": athena.get("caveats") or [],
    }

    # ── TWO-PASS COMPARISON. Pass 1 is the table above (each provider on
    # its own prices/FX). Pass 2 revalues every side's OWN par at the
    # administrator's price and FX — constant prices, varying par — so
    # position/completeness mistakes separate visually from marks.
    maia_bonds_p2 = None
    if _bonds:
        maia_bonds_p2 = {
            r["isin"]: {"par": r["maia_par"],
                        "own_value_base": r.get("maia_exposure_base")}
            for r in _bonds.get("rows") or []
            if r.get("in_maia") and r.get("maia_par") is not None}
    athena_par_p2 = None
    if athena_txn:
        athena_par_p2 = {h["isin"]: h["par"]
                         for h in athena_txn.get("holdings") or []
                         if h.get("isin") and h.get("par") is not None}
    result["passes"] = {
        "own": {
            "basis": "Each provider on its own prices and FX rates — "
                     "differences mix positions, marks and FX.",
            "rows": result["rows"],
            "total": result["total"],
        },
        "admin_priced": aum_passes.admin_priced_pass(
            parsed,
            sides={"maia": maia or {}, "athena": athena, "waystone": waystone},
            maia_bonds=maia_bonds_p2,
            athena_par=athena_par_p2,
            bonds_meta=_bonds),
    }
    # Maia figures DRIFT between same-date exports (snapshot timing: the
    # 31-Jul 14:51 view and the post-switch native-ISIN export tell
    # different position stories). The file name is therefore part of the
    # finding, not a footnote — the UI must show it beside the Maia column.
    if maia_meta.get("available"):
        result["passes"]["notes"] = [
            f"Maia columns in BOTH passes derive from "
            f"{maia_meta.get('file')} (data date {maia_date}). Maia figures "
            f"drift between same-date exports, so a different export of the "
            f"same afternoon can tell a different position story."]

    # ── Evidence report: findings with magnitude/status/scope, every sentence
    # derived from figures already computed. funds_total from the registry so
    # the coverage statement cannot overstate what was examined.
    try:
        result["evidence_report"] = maia_evidence_report.build(
            parsed, maia_breakdown, _bonds, meta={
                "portfolio_id": pid,
                "admin_file": admin_row.get("file_name"),
                "maia_file": maia_meta.get("file"),
                "maia_valuation_date": maia_date,
                "dates_match": bool(maia_date
                                    and parsed.get("valuation_date") == maia_date),
                "funds_tested": 1,
                "funds_total": len(aum_funds()),
                "dates_tested": 1,
            })
    except Exception as e:
        logger.warning("aum: evidence report failed: %s", e, exc_info=True)
        result["evidence_report"] = {"error": str(e)}

    # ── Athena/GA10 marks — independent accrued, making the accrued
    # comparison genuinely three-way. Best-effort.
    try:
        result["athena_marks"] = await _ga10_marks(
            parsed.get("holdings") or [], parsed.get("valuation_date"))
    except Exception as e:
        logger.warning("aum: GA10 marks unavailable: %s", e)
        result["athena_marks"] = {}

    # ── Currency / share-class hedge section. Prior pack chosen by DATE,
    # strictly before this valuation — never by position in a list.
    try:
        this_date = parsed.get("valuation_date") or ""
        earlier = sorted((r for r in admin_reg
                          if (r.get("date") or "") < this_date),
                         key=lambda r: r.get("date") or "")
        prior = None
        if earlier:
            p = await _cached(earlier[-1])
            if p:
                prior = nav_parser.parse_nav_report(str(p))
        result["currency"] = currency_hedge_recon.build(parsed, prior)
    except Exception as e:
        logger.warning("aum: currency section failed: %s", e)
        result["currency"] = {"error": str(e)}

    # ── FX-FORWARD ATTRIBUTION ALERT (compliance-facing). Share-class hedge
    # forwards are fund assets but NOT fund currency positioning: a
    # compliance test that counts them sees phantom exposure. Measured
    # 31-Jul: GBP is -0.21% of NAV at fund level but +48.02% all-in — any
    # limit check on the all-in number false-alarms (or worse, a real
    # breach reads as hedged). Flag every currency where the two measures
    # diverge by more than 5 points.
    result["fx_forward_alert"] = _fx_forward_alert(result.get("currency"))

    # ── COMPLIANCE REPORT PRESENCE. Maia's pre-trade compliance dump
    # (maia_ptc) arrives via the OneDrive intake. Absence for the report
    # date must be VISIBLE — "no compliance file was supplied for this
    # date" is a finding the reader decides about, never a blank.
    ptc = await _registry(pid, "maia_ptc")
    ptc_dates = sorted({r.get("date") for r in ptc if r.get("date")}, reverse=True)
    rep_date = parsed.get("valuation_date")
    result["compliance_file"] = {
        "supplied_for_date": rep_date in ptc_dates,
        "report_date": rep_date,
        "latest_on_file": ptc_dates[0] if ptc_dates else None,
    }

    # ── THREE-WAY ACCRUED RECON (Waystone | Maia | GA10). Compared per-100
    # of par in LOCAL currency so FX drops out entirely. GA10 shows both
    # T+0 and C+1 accrual points — Waystone accrues to market settlement,
    # so its figure should sit on C+1, not T+0; agreement there and a gap
    # at T+0 is the convention, not a break.
    result["accrued_recon"] = _accrued_recon(
        parsed, maia_rows_bonds, result.get("athena_marks") or {})

    # ── MANAGEMENT ASSESSMENT — the punchy, honest headline. Never says
    # "agrees" while differences exist: every dollar of the stated gap is
    # assigned to a named cause with a status, and the constant-price rerun
    # (both books at the administrator's own marks) states what remains
    # once time-of-day pricing is stripped out.
    result["assessment"] = _build_assessment(result, maia_breakdown, parsed)
    result["briefing_md"] = _briefing_md(result)

    if len(_result_cache) > 64:
        _result_cache.clear()
    _result_cache[(pid, anchor)] = (fp, _time.time(), result)
    return result


def prewarm(pid: str, date: str | None = None):
    """Fire-and-forget cache fill — called after an upload lands so the
    first human view of a new valuation is warm, not a 100s build."""
    async def _run():
        try:
            await build_aum_comparison(pid, date)
            logger.info("aum prewarm done: %s %s", pid, date or "latest")
        except Exception as e:
            logger.warning("aum prewarm failed: %s %s: %s", pid, date, e)
    try:
        asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        pass  # no loop (sync caller/test) — skip, first view builds cold


def _fx_forward_alert(currency: dict | None, threshold_pts: float = 5.0) -> dict:
    fe = (currency or {}).get("fund_exposure") or {}
    rows = fe.get("rows") or []
    base = fe.get("base_currency")
    flagged = []
    for r in rows:
        if r.get("currency") == base:
            continue  # base ccy is the mirror of the others — always moves
        pf, pa = r.get("pct_nav_fund"), r.get("pct_nav_all")
        if pf is None or pa is None:
            continue
        if abs(pa - pf) > threshold_pts:
            flagged.append({
                "currency": r.get("currency"),
                "pct_nav_fund": pf,
                "pct_nav_all_in": pa,
                "distortion_pts": round(pa - pf, 2),
                "share_class_forward_base": r.get("share_class_forward_base"),
            })
    return {
        "alert": bool(flagged),
        "threshold_pts": threshold_pts,
        "currencies": flagged,
        "message": (
            "Share-class hedge forwards distort all-in currency exposure by "
            "more than {:.0f} points in {} — currency compliance tests MUST "
            "exclude share-class forwards (they hedge investors' classes, "
            "not the fund's positioning), or alerts will fire on phantom "
            "exposure.".format(threshold_pts,
                               ", ".join(f["currency"] for f in flagged))
            if flagged else
            "Share-class forwards do not materially distort fund currency "
            "exposure at this valuation."),
    }


def _accrued_recon(parsed: dict, maia_bond_rows: list[dict],
                   athena_marks: dict) -> dict:
    """Per-bond accrued from all three sources, normalised to per-100 of par
    in the bond's OWN currency:

        Waystone   accrued_income is BASE ccy → /fx /par ×100
        Maia       accrued is LOCAL ccy       → /qty ×100
        GA10       accrued_interest is already per-100 (T+0 and C+1)

    Rows carry the raw diffs; verdicts stay with the reader (and the
    assessment) — this table is evidence, not opinion.
    """
    fx = {"USD": 1.0}
    rates = (parsed.get("fx_rates") or {}).get("rates") or {}
    for c, r in rates.items():
        if r:
            fx[c] = 1.0 / r  # parser stores units-per-base; we need base-per-unit
    base = parsed.get("base_currency") or "USD"
    fx.setdefault(base, 1.0)
    maia_by_isin = {r["isin"]: r for r in (maia_bond_rows or []) if r.get("isin")}

    rows, ga10_missing = [], []
    for h in parsed.get("holdings") or []:
        isin, par = h.get("isin"), h.get("par_amount") or h.get("face_value")
        if not isin or not par:
            continue
        ccy = h.get("currency") or base
        f = fx.get(ccy)
        w100 = None
        acc_base = h.get("accrued_income")
        if acc_base is not None and f:
            w100 = acc_base / f / par * 100
        m = maia_by_isin.get(isin) or {}
        m100 = None
        if m.get("accrued") is not None and m.get("qty"):
            m100 = m["accrued"] / m["qty"] * 100
        g = athena_marks.get(isin) or {}
        g_t0, g_c1 = g.get("accrued_per_100"), g.get("accrued_per_100_c1")
        if not g:
            ga10_missing.append(isin)
        rows.append({
            "isin": isin, "description": h.get("description"),
            "currency": ccy, "par": par, "coupon": h.get("coupon"),
            "waystone_per100": round(w100, 4) if w100 is not None else None,
            "maia_per100": round(m100, 4) if m100 is not None else None,
            "ga10_t0_per100": round(g_t0, 4) if g_t0 is not None else None,
            "ga10_c1_per100": round(g_c1, 4) if g_c1 is not None else None,
            "diff_maia_waystone": round(m100 - w100, 4)
            if (m100 is not None and w100 is not None) else None,
            "diff_ga10c1_waystone": round(g_c1 - w100, 4)
            if (g_c1 is not None and w100 is not None) else None,
        })
    # MONTH-END ACCRUAL BOUNDARY. Where a coupon pays exactly at the accrual
    # point, C+1 accrued resets to nil while the cash has not been received —
    # a full coupon then sits in no line of the valuation. Adds the
    # entitlement-basis column and names any bond affected; every field above
    # is left as it was (see accrual_boundary for the measurement and the
    # revert note).
    coupon_receivable = accrual_boundary.apply(rows)

    return {
        "basis": "accrued per 100 of par, local currency (FX-independent). "
                 "Waystone accrues to MARKET SETTLEMENT — compare it against "
                 "GA10's C+1 column; the T+0 column is the trade-date accrual.",
        "rows": rows,
        "ga10_missing": ga10_missing,
        "ga10_coverage": f"{len(rows) - len(ga10_missing)}/{len(rows)}",
        "coupon_receivable": coupon_receivable,
    }


def _briefing_md(result: dict) -> str:
    """One-page board-style brief, generated from the computed payload only
    — conclusion first, seriousness stated, actions listed. The reader who
    wants the per-bond evidence scrolls the full report; this page is what
    goes in the pack."""
    meta = result.get("meta") or {}
    a = result.get("assessment") or {}
    i = result.get("integrity") or {}
    fx = result.get("fx_forward_alert") or {}
    cf = result.get("compliance_file") or {}
    ar = result.get("accrued_recon") or {}
    ev = result.get("evidence_report") or {}
    fund = meta.get("fund_name") or "Fund"
    vdate = meta.get("valuation_date") or "?"

    # Seriousness: driven by the assessment level, lifted by standing alerts.
    level = a.get("level")
    if level in ("material_unexplained", "investigate"):
        serious, why = "HIGH", "the difference is not fully explained"
    elif fx.get("alert"):
        serious, why = ("ATTENTION REQUIRED",
                        "valuations reconcile, but a control issue is open "
                        "(share-class FX forwards distorting compliance "
                        "measures)")
    elif level == "explained_not_identical":
        serious, why = ("MODERATE",
                        "differences are explained but two causes await "
                        "confirmation")
    else:
        serious, why = "ROUTINE", "the systems state the same total"

    L = []
    L.append(f"# Fund reconciliation — board brief")
    L.append(f"**{fund}** · valuation {vdate}"
             + (f" {meta.get('valuation_time')}" if meta.get("valuation_time") else "")
             + f" · front office vs administrator vs independent (GA10)")
    L.append("")
    L.append(f"## Conclusion — seriousness: {serious}")
    if a.get("available"):
        L.append(a.get("headline", ""))
    L.append(f"Basis for the rating: {why}.")
    L.append("")
    if i:
        L.append("## The number")
        L.append(f"Front office **{i.get('maia_total'):,.2f}** vs administrator "
                 f"**{i.get('admin_nav'):,.2f}** = **{i.get('difference'):,.2f}** "
                 f"({i.get('difference_pct'):+.2f}%).")
        cp = a.get("constant_price")
        if cp:
            L.append(f"Stripping time-of-day pricing (both books at the "
                     f"administrator's marks): **{cp['remaining_difference']:,.2f}** "
                     f"({cp['remaining_pct']:+.3f}%) remains — the part a single "
                     f"price source would not remove.")
        L.append("")
    if a.get("causes"):
        L.append("## Where every dollar of the gap sits")
        L.append("| Cause | Amount | Status |")
        L.append("|---|---:|---|")
        for c in a["causes"]:
            L.append(f"| {c['cause']} | {c['amount']:,.2f} | "
                     f"{c['status'].replace('_', ' ')} |")
        L.append("")
    actions = []
    if fx.get("alert"):
        ccys = ", ".join(f"{c['currency']} ({c['pct_nav_fund']}% fund vs "
                         f"{c['pct_nav_all_in']}% all-in)"
                         for c in fx.get("currencies") or [])
        actions.append(f"**Exclude share-class FX forwards from currency "
                       f"compliance tests.** They distort exposure in {ccys}; "
                       f"alerts fire on phantom positions while a real breach "
                       f"could read as hedged.")
    if cf.get("report_date") and not cf.get("supplied_for_date"):
        actions.append(f"**Compliance report not supplied for {cf['report_date']}**"
                       + (f" (latest on file {cf['latest_on_file']})"
                          if cf.get("latest_on_file") else "")
                       + ". Decide whether a backdated run is required.")
    for c in a.get("causes") or []:
        if c["status"] == "question_open" and abs(c["amount"]) >= 100:
            actions.append(f"**Confirm: {c['cause'].lower()}** "
                           f"({c['amount']:+,.2f}). {c['action']}")
        if c["status"] == "investigate":
            actions.append(f"**Investigate unexplained {c['amount']:+,.2f}** "
                           f"before either figure is relied on.")
    if ar.get("ga10_missing"):
        actions.append(f"**Enrol {len(ar['ga10_missing'])} bonds for independent "
                       f"verification** — GA10 coverage {ar.get('ga10_coverage')}; "
                       f"until then their accrued has no third check.")
    if actions:
        L.append("## Actions required")
        for n, act in enumerate(actions, 1):
            L.append(f"{n}. {act}")
        L.append("")
    L.append("## Scope and caveats")
    scope = ev.get("scope") or {}
    if scope.get("affects"):
        L.append(f"- {scope['affects']}")
    L.append(f"- One fund, one valuation date. Independent (GA10) accrued "
             f"coverage: {ar.get('ga10_coverage', 'n/a')}.")
    L.append("- The Athena column derives from the administrator's book for "
             "this fund — identity, not corroboration, until Athena prices "
             "independently.")
    maia_f = (meta.get("maia") or {}).get("file")
    L.append(f"- Sources: administrator `{meta.get('admin_file')}`"
             + (f", front office `{maia_f}`" if maia_f else ", no front-office view for this date")
             + ". Full per-bond evidence in the detailed report below.")
    return "\n".join(L)


def _build_assessment(result: dict, maia_breakdown: dict | None,
                      parsed: dict) -> dict:
    """Regroup the component differences into NAMED CAUSES that sum exactly
    to the stated gap, plus the constant-price view. Numbers only from
    figures already computed on this payload — nothing re-derived."""
    if maia_breakdown is None:
        return {"available": False,
                "reason": "no usable Maia view — nothing to assess"}
    integ = result.get("integrity") or {}
    stated = integ.get("difference")
    if stated is None:
        return {"available": False, "reason": "totals unavailable"}

    rows = {r["key"]: r for r in (result.get("rows") or [])}

    def _mw(key):
        r = rows.get(key) or {}
        v = r.get("diff_maia_waystone")
        if v is not None:
            return v
        # Maia has no figure for this component (e.g. other_net — no
        # receivables concept). Its CONTRIBUTION to the stated gap is still
        # real: Maia total omits what the admin includes, so the effect is
        # minus the admin's value. Assigning it here keeps it out of
        # "Unexplained", where an understood definitional gap doesn't belong.
        w = r.get("waystone")
        return -w if (r.get("maia") is None and w is not None) else 0.0

    attrib = result.get("attribution") or {}
    at = attrib.get("totals") or {}
    price_eff = at.get("price") if attrib.get("available") else None
    fx_eff = at.get("fx") if attrib.get("available") else None
    par_eff = at.get("par") if attrib.get("available") else None
    clean_diff = _mw("clean_bond_mv")

    causes = []

    def cause(name, amount, status, action):
        causes.append({"cause": name, "amount": round(amount, 2),
                       "status": status, "action": action})

    detail = None
    if price_eff is not None and fx_eff is not None:
        detail = (f"price marks {price_eff:+,.2f}, FX rates {fx_eff:+,.2f}, "
                  f"quantities {par_eff:+,.2f} (constant-par decomposition)")
    cause("Pricing timing — clean bond value", clean_diff,
          "known_cause",
          "Maia snapshots intraday vs the administrator's 12:00 strike. "
          + (detail or "Per-bond decomposition needs an ISIN view.")
          + " Conclusive fix: rerun both books on ONE price source.")
    cause("Accrual measurement", _mw("accrued_income"),
          "question_open",
          "Maia's price-implied accrual runs ahead of the administrator's "
          "booked accrual (more accrued days on every line). Confirm the "
          "day-count basis with the front office.")
    fwd = _mw("fx_forward_pnl_fund") + _mw("fx_forward_pnl_share_class")
    cause("Forward P&L owner attribution", fwd,
          "question_open",
          "The fund/share-class rows offset — largely the same forwards "
          "attributed to different owners. Confirm Maia's owner mapping "
          "against OpenCurrency's per-block totals.")
    cause("Cash definition", _mw("cash"),
          "definitional",
          "Maia's cash includes unsettled forward legs; the administrator's "
          "does not. Not like-for-like by construction.")
    cause("Receivables/payables", _mw("other_net"),
          "definitional",
          "Administrator-only concept (Balance Sheet); Maia has no "
          "equivalent line.")
    residual = stated - sum(c["amount"] for c in causes)
    if abs(residual) >= 0.01:
        cause("Unexplained", residual, "investigate",
              "Not attributable to any identified cause — investigate "
              "before signing off.")

    # Constant-price rerun: strip the marks (price+FX at constant par) and
    # state what remains — the differences a single price source would NOT
    # remove.
    constant_price = None
    if price_eff is not None and fx_eff is not None:
        remaining = stated - price_eff - fx_eff
        constant_price = {
            "basis": "both books at the administrator's prices and FX, "
                     "constant par",
            "pricing_timing_removed": round(price_eff + fx_eff, 2),
            "remaining_difference": round(remaining, 2),
            "remaining_pct": round(remaining / integ["admin_nav"] * 100, 3)
            if integ.get("admin_nav") else None,
        }

    unexplained = sum(c["amount"] for c in causes
                      if c["status"] == "investigate")
    open_q = sum(abs(c["amount"]) for c in causes
                 if c["status"] == "question_open")
    level = ("material_unexplained" if abs(stated) > 0 and integ.get("material")
             else "investigate" if abs(unexplained) >= 0.01
             else "explained_not_identical" if abs(stated) >= 0.01
             else "identical")
    headline = {
        "identical": "The two systems state the same total.",
        "explained_not_identical":
            "NOT identical: the front office is {:,.2f} ({:+.2f}%) {} the "
            "administrator. Every dollar of the gap is assigned to a named "
            "cause below; {:,.2f} sits in causes still awaiting confirmation."
            .format(abs(stated), integ.get("difference_pct") or 0,
                    "below" if stated < 0 else "above", open_q),
        "investigate":
            "NOT identical, with an UNEXPLAINED residual — see the cause "
            "table before relying on either figure.",
        "material_unexplained":
            "MATERIAL difference of {:,.2f} ({:+.2f}%). The administrator's "
            "valuation should be used until this is resolved."
            .format(abs(stated), integ.get("difference_pct") or 0),
    }[level]
    return {"available": True, "level": level, "headline": headline,
            "stated_difference": stated,
            "stated_pct": integ.get("difference_pct"),
            "causes": causes, "constant_price": constant_price}
