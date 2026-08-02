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


async def _ga10_marks(holdings: list[dict], settle_iso: str) -> dict:
    """{isin: {clean_price, accrued_per_100, coupon, analytics_source}} from a
    single GA10 GAE portfolio/analysis batch at the admin's own prices.

    GA10's accrued is an INDEPENDENT calculation, not a restatement of the
    administrator's — which is what makes the accrued comparison genuinely
    three-way. Best-effort: any failure returns {} and the page says so.
    """
    from auth_client import get_api_key
    prices = {h["isin"]: h.get("price") for h in holdings
              if h.get("isin") and h.get("price")}
    if not prices:
        return {}
    api_key = get_api_key("GA10_API_KEY", requester="recon-mcp-aum")
    if not api_key:
        return {}
    inv_date = settle_iso.replace("-", "/")
    payload = {"format": "FLDS", "data": [
        {"BOND_CD": i, "CLOSING PRICE": float(p), "WEIGHTING": 1.0,
         "Inventory Date": inv_date} for i, p in prices.items()]}
    try:
        async with httpx.AsyncClient(timeout=120.0) as c:
            r = await c.post(f"{_GAE_URL}/api/v1/portfolio/analysis",
                             json=payload,
                             headers={"Content-Type": "application/json",
                                      "X-API-Key": api_key})
        if r.status_code != 200:
            logger.warning("GA10 marks: HTTP %s %s", r.status_code, r.text[:200])
            return {}
        out = {}
        for b in (r.json().get("bond_data") or []):
            isin = b.get("isin")
            if not isin:
                continue
            out[isin] = {
                "clean_price": prices.get(isin),
                "accrued_per_100": b.get("accrued_interest"),
                "ytw": b.get("yield") or b.get("ytm"),
                "duration": b.get("duration"),
                "coupon": b.get("coupon"),
                "analytics_source": "GA10",
            }
        return out
    except Exception as e:
        logger.warning("GA10 marks unavailable: %s", e)
        return {}


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

async def build_aum_comparison(pid: str, date: str | None = None) -> dict:
    """Full AUM comparison payload for a fund.

    ``date`` pins the Maia side; default is the newest dated Maia export.
    The administrator pack is then DATE-MATCHED to the Maia view actually
    used — order matters and got this wrong twice in Athena (see the ported
    comments): defaulting the admin side to "latest" silently reconciled a
    30 Jul pack against a 24 Jul Maia export and reported six days of market
    movement as "difference".
    """
    if pid not in FUNDS or pid not in aum_funds():
        return {"error": f"fund {pid} has no AUM reconciliation", "status": 404}

    admin_reg, maia_reg, ref_reg = await asyncio.gather(
        _registry(pid, SRC_ADMIN), _registry(pid, SRC_MAIA),
        _registry(pid, SRC_MAIA_REF))
    if not admin_reg:
        return {"error": f"no stored administrator valuation for {pid}",
                "status": 404}

    # ── Maia side first. Files are stored under their RESOLVED data date, so
    # date ranking is the registry key; capability breaks ties between shapes
    # of the same snapshot (the 34-col priced view outranks the 13-col
    # allocation view; the 629-col full export loses on forwards-as-gross-legs
    # but wins on ISIN — the breakdown's capability score encodes that).
    maia = maia_breakdown = None
    maia_meta = {"available": False}
    maia_rows_bonds = []
    maia_date = None
    maia_path = None

    cand = [r for r in maia_reg if not date or r.get("date") == date]
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
    want = maia_date or date
    admin_row = next((r for r in admin_reg if r.get("date") == want), None) \
        or max(admin_reg, key=lambda r: r.get("date") or "")
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
    athena = nav_comparison.athena_side(parsed)

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

    return result
