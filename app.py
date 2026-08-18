"""
Recon MCP - Reconciliation service for bond portfolio data.

Owns all parsing, enrichment, storage, and display logic for BBG, admin NAV,
Maia holdings, and GA10 QuantLib calculations. Athena and other apps just
call this service for display-ready recon data.
"""

import asyncio
import logging
import os
from datetime import datetime

from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse

from recon_db import (
    get_recon_data,
    get_recon_status,
    backfill_coupon_maturity,
    sync_bond_data,
    sync_orca_holdings,
)
from recon_engine import (
    process_bbg_upload,
    process_admin_upload,
    process_maia_upload,
    recalc_accrued as _recalc_accrued,
    diagnose_accrued_convention,
)
from alerts import alert_upload_failed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Recon MCP",
    description="Bond portfolio reconciliation — owns BBG/admin/Maia parsing, enrichment, and display",
    version="1.0.0",
)

_startup_time = datetime.utcnow()
_backfill_status: dict = {"coupon_maturity": None}

# ── Static UI (the AUM recon app) ───────────────────────────────────────────
# Same-origin vanilla HTML/JS, gated per-request below (the static mount
# itself cannot run the session check, so /ui/ goes through a small handler).
from pathlib import Path as _Path
from fastapi.responses import FileResponse, RedirectResponse

_STATIC_DIR = _Path(__file__).parent / "static"


@app.get("/")
async def root():
    return RedirectResponse("/ui/recon.html", status_code=302)


@app.get("/ui/{asset}")
async def ui_asset(request: Request, asset: str):
    from recon_auth import current_user, unauthorized
    user = await current_user(request)
    if not user:
        return unauthorized(request)
    p = (_STATIC_DIR / asset).resolve()
    if not str(p).startswith(str(_STATIC_DIR.resolve())) or not p.is_file():
        raise HTTPException(status_code=404, detail="not found")
    return FileResponse(p)


async def _startup_backfill():
    """On startup, sync bond data from bond-data Supabase and backfill missing fields."""
    await asyncio.sleep(5)  # Let the server finish starting
    try:
        # Sync bond identity/reference/analytics into local tables
        sync_result = await sync_bond_data()
        _backfill_status["bond_data_sync"] = sync_result
        logger.info("Bond data sync complete: %s", sync_result)

        # Sync Orca holdings (par amounts) for all portfolios
        for pid in ("wnbf", "gcrif"):
            orca_result = await sync_orca_holdings(pid)
            _backfill_status[f"orca_holdings_{pid}"] = orca_result
            logger.info("Orca holdings sync (%s): %s", pid, orca_result)

        bbg_result = await backfill_coupon_maturity("recon_bbg")
        maia_result = await backfill_coupon_maturity("recon_maia")
        _backfill_status["coupon_maturity"] = {
            "recon_bbg": bbg_result,
            "recon_maia": maia_result,
        }
        logger.info("Startup backfill complete: %s", _backfill_status)

        # Auto-recalc any athena_bbg rows missing days_accrued (e.g. after a code deploy
        # that added the column, or after a failed recalc). Runs quietly in background.
        asyncio.create_task(_backfill_missing_days_accrued())

    except Exception as e:
        logger.error(f"Startup backfill failed: {e}")
        _backfill_status["coupon_maturity"] = {"error": str(e)}


async def _backfill_missing_days_accrued():
    """Find all (portfolio_id, date) pairs in athena_bbg with NULL days_accrued
    and trigger a force recalc for each. Runs once on startup."""
    import httpx
    from recon_db import SUPABASE_URL, _headers
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/athena_bbg",
                headers=_headers(),
                params={"days_accrued": "is.null", "select": "portfolio_id,date", "limit": "500"},
            )
        if resp.status_code != 200:
            return
        rows = resp.json()
        # Deduplicate (portfolio_id, date) pairs
        pairs = list({(r["portfolio_id"], r["date"]) for r in rows if r.get("portfolio_id") and r.get("date")})
        if not pairs:
            logger.info("Startup: all athena_bbg rows have days_accrued populated")
            return
        logger.info("Startup: recalculating days_accrued for %d date(s): %s", len(pairs), pairs)
        for pid, date in pairs:
            await _recalc_accrued(pid, date, force=True)
        logger.info("Startup: days_accrued backfill complete")
    except Exception as e:
        logger.warning("Startup days_accrued backfill failed: %s", e)


# Static-vs-trades drift watch. Andy, 2026-08-12: "every day or when athena
# loads we should check our static against trades — the trades are gospel
# unless we flag otherwise." Interval is HARDCODED, not an env var: a daily
# cadence is the same on Railway, the Guinness box and a laptop, so an env
# var here would just be one more thing missing after a port.
_STATIC_WATCH_INTERVAL = 24 * 3600
_STATIC_WATCH_FIRST_RUN = 120          # let startup settle before the first call
_STATIC_WATCH_FUNDS = ("gdbft",)
_last_alerted: dict[str, str] = {}


def _finding_signature(res: dict) -> str:
    """Identity of a finding SET, so a standing defect alerts once rather
    than every day — but any change in which trades disagree alerts again."""
    return "|".join(sorted(
        f"{f.get('isin')}@{f.get('settlement_date')}:{f.get('diff_per100')}"
        for f in (res.get("findings") or [])))


async def _static_watch_loop():
    """Daily static-vs-trades check. Alerts on a CHANGE in the finding set —
    including a drop to zero, which is the "drift resolved" signal and is
    worth saying out loud."""
    import alerts
    await asyncio.sleep(_STATIC_WATCH_FIRST_RUN)
    while True:
        for pid in _STATIC_WATCH_FUNDS:
            try:
                res = await _static_validation(pid, refresh=True)
                if res.get("status") != "ok":
                    logger.warning("static watch %s: %s", pid, res.get("error"))
                    continue
                sig = _finding_signature(res)
                if sig == _last_alerted.get(pid):
                    continue                      # unchanged — already reported
                _last_alerted[pid] = sig
                n = res.get("finding_count", 0)
                if n:
                    await alerts.send_alert(
                        f"Static disagrees with {n} settled trade(s) — {pid}",
                        "Settled-trade confirms are the authority, so the "
                        "static (or the calc path applying it) is wrong. "
                        "Bonds: " + ", ".join(res.get("bonds_affected") or []),
                        level="warning",
                        fields={"checked": res.get("trades_checked"),
                                "agree": res.get("agree"),
                                "unchecked": res.get("trades_unchecked")})
                else:
                    await alerts.send_alert(
                        f"Static now reconciles to every settled trade — {pid}",
                        "All {} checked trade(s) agree.".format(res.get("agree", 0)),
                        level="good")
            except Exception as e:
                logger.error("static watch %s failed: %r", pid, e)
        await asyncio.sleep(_STATIC_WATCH_INTERVAL)


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_startup_backfill())
    asyncio.create_task(_static_watch_loop())
    # Periodic safety-net for the calc-staleness detector. Set
    # RECALC_STALE_INTERVAL_SECONDS=900 to run every 15 min. Unset = off.
    interval_str = os.environ.get("RECALC_STALE_INTERVAL_SECONDS", "")
    if interval_str.isdigit() and int(interval_str) > 0:
        asyncio.create_task(_recalc_stale_loop(int(interval_str)))


async def _recalc_stale_loop(interval_seconds: int):
    """Periodic /recalc/stale runner. Calls the endpoint handler directly
    to avoid an HTTP roundtrip. Errors log but do not break the loop.
    """
    import logging
    log = logging.getLogger(__name__)
    log.info(f"recalc_stale loop started (every {interval_seconds}s)")
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            result = await recalc_stale(limit=200)
            if result.get("fired_isins", 0) > 0 or result.get("failures"):
                log.info(f"recalc_stale_loop: {result}")
        except Exception as e:
            log.error(f"recalc_stale_loop error: {e!r}")


# ── Health + manifest ──────────────────────────────────────────────────────

# Static-vs-trades validation is cached: it costs one GA10 call per trade and
# the answer only changes when a trade is booked or static is corrected.
# Refreshed on the schedule below and on demand via ?refresh=1.
_static_val_cache: dict = {}
_STATIC_VAL_TTL = 6 * 3600


async def _static_validation(pid: str = "gdbft", refresh: bool = False) -> dict:
    import time as _t
    import static_validation
    hit = _static_val_cache.get(pid)
    if hit and not refresh and (_t.time() - hit[0]) < _STATIC_VAL_TTL:
        return hit[1]
    res = await static_validation.validate(pid)
    # Only cache a run that actually completed — an outage must not stick.
    if res.get("status") == "ok":
        _static_val_cache[pid] = (_t.time(), res)
    return res


@app.get("/static/validate/{fund}")
async def static_validate(request: Request, fund: str, refresh: bool = False):
    """Every settled trade's accrued vs our static. Trades are the authority.

    A difference here is a defect in OUR static — never in the confirm — and
    it corrects static going forward only: settled values are frozen.
    """
    denied = await _aum_gate(request)
    if denied:
        return denied
    pid = _funds.resolve(fund) or fund
    return await _static_validation(pid, refresh)


@app.get("/ops/probes")
async def ops_probes():
    """Dependency-aware health for the central control room (see
    mcp_central/CLAUDE.md § Ops Visibility). Deliberately UNGATED, like
    /health — the dashboard polls it without a session."""
    import static_validation
    probes = []
    try:
        probes.append(static_validation.probe(await _static_validation("gdbft")))
    except Exception as e:
        probes.append({"id": "static_vs_trades", "status": "red", "value": None,
                       "expected": "check runs", "detail": str(e)})
    order = {"red": 0, "amber": 1, "green": 2}
    overall = min((p["status"] for p in probes),
                  key=lambda s: order.get(s, 3), default="green")
    return {"app": "recon-mcp", "overall": overall, "probes": probes}


@app.get("/health")
async def health():
    uptime = (datetime.utcnow() - _startup_time).total_seconds()
    return {
        "status": "ok",
        "service": "recon-mcp",
        "uptime_seconds": round(uptime),
        "backfill_status": _backfill_status,
    }


@app.get("/brian-manifest")
async def brian_manifest():
    return {
        "id": "recon",
        "name": "Recon",
        "tier": "engine",
        "sort_order": 3,
        "enabled": True,
        "version_hash": "v2_20260802",
        "summary": "Fund reconciliation — three-way AUM (Maia | GA10 | administrator) with its own UI, plus bond-level BBG/admin/Maia/GA10 recon joined by ISIN.",
        "base_url": "https://recon-mcp-production.up.railway.app",
        "capabilities": [
            {
                "name": "AUM reconciliation",
                "description": "Three-way fund-level NAV comparison (Maia | Athena/GA10 | administrator) with materiality verdict, par/price/FX attribution, per-bond breaks, deterministic evidence report and currency-hedge section. Fund set from the registry; files keyed by resolved data date.",
                "examples": ["GET /aum/gdbf", "GET /aum/gdbf/dates", "POST /aum/upload/maia?fund=gdbf"],
            },
            {
                "name": "Reconciliation data",
                "description": "Get joined recon data for a portfolio/date. Returns BBG, admin, Maia, and GA10 data with derived diffs computed in the database.",
                "examples": ["GET /recon/data?portfolio_id=wnbf&date=2026-03-31"],
            },
            {
                "name": "Upload parsing",
                "description": "Parse and store BBG exports, admin NAV reports, and Maia holdings files. Enriches with bond reference data and triggers GA10 analytics.",
                "examples": ["POST /upload/bbg", "POST /upload/admin", "POST /upload/maia"],
            },
        ],
        "pages": [
            {"id": "recon-ui", "name": "Fund Reconciliation", "path": "/ui/recon.html",
             "description": "The AUM reconciliation app: verdict, components, evidence report, attribution, per-bond breaks, uploads."}
        ],
        "tour": [],
    }


# ── Upload endpoints ──────────────────────────────────────────────────────

@app.post("/upload/bbg")
async def upload_bbg(
    file: UploadFile = File(...),
    portfolio_id: str = None,
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Parse a Bloomberg portfolio export, store in recon_bbg, trigger GA10 recalc."""
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File must be .xlsx or .xls")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    result = await process_bbg_upload(
        file_bytes=contents,
        filename=file.filename,
        uploaded_by=x_user_email or "unknown",
        portfolio_override=portfolio_id,
    )
    if result.get("status") == "error":
        await alert_upload_failed("bbg", file.filename, result.get("error", "unknown"), x_user_email)
        raise HTTPException(status_code=422, detail=result.get("error"))
    # Auto-trigger accrued recalc so the recon tab populates immediately
    pid = result.get("portfolio_id")
    bbg_date = result.get("date")
    if pid and bbg_date:
        asyncio.create_task(_recalc_accrued(pid, bbg_date, force=True))
    return result


@app.post("/upload/auto")
async def upload_auto(
    file: UploadFile = File(...),
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Auto-detect BBG vs admin NAV by reading the file header, then route
    to the correct processor. Used for the legacy /api/gcrif/upload path
    which accepts either format."""
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File must be .xlsx or .xls")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    from bbg_parser import is_bbg_export

    if is_bbg_export(contents):
        result = await process_bbg_upload(
            file_bytes=contents, filename=file.filename,
            uploaded_by=x_user_email or "unknown",
        )
        if result.get("status") != "error":
            pid = result.get("portfolio_id")
            bbg_date = result.get("date")
            if pid and bbg_date:
                asyncio.create_task(_recalc_accrued(pid, bbg_date, force=True))
    else:
        result = await process_admin_upload(
            file_bytes=contents, filename=file.filename,
            uploaded_by=x_user_email or "unknown",
        )

    if result.get("status") == "error":
        await alert_upload_failed("auto", file.filename, result.get("error", "unknown"), x_user_email)
        raise HTTPException(status_code=422, detail=result.get("error"))
    return result


@app.post("/upload/admin")
async def upload_admin(
    file: UploadFile = File(...),
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Parse an admin NAV report, store in recon_admin."""
    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xls", ".tsv", ".txt", ".csv")):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    result = await process_admin_upload(
        file_bytes=contents,
        filename=file.filename,
        uploaded_by=x_user_email or "unknown",
    )
    if result.get("status") == "error":
        await alert_upload_failed("admin", file.filename, result.get("error", "unknown"), x_user_email)
        raise HTTPException(status_code=422, detail=result.get("error"))
    return result


@app.post("/upload/maia")
async def upload_maia(
    file: UploadFile = File(...),
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Parse a Maia holdings file, store in recon_maia."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename")
    if not file.filename.lower().endswith((".xlsx", ".xls", ".csv", ".tsv", ".txt")):
        raise HTTPException(status_code=400, detail="Unsupported file type")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    result = await process_maia_upload(
        file_bytes=contents,
        filename=file.filename,
        uploaded_by=x_user_email or "unknown",
    )
    if result.get("status") == "error":
        await alert_upload_failed("maia", file.filename, result.get("error", "unknown"), x_user_email)
        raise HTTPException(status_code=422, detail=result.get("error"))
    return result


# ── AUM reconciliation (fund-level, three-way Maia|GA10|administrator) ─────
#
# The recon that used to live inside Athena (/api/nav-comparison). Fund set
# comes from funds.FUNDS; files come from Supabase Storage keyed by their
# RESOLVED data date (see aum_orchestrator's module docstring).

async def _aum_gate(request: Request):
    """Session / service-key / box-noauth gate for the AUM surface only —
    the pre-existing machine endpoints keep their current open behaviour
    (see recon_auth's module docstring)."""
    from recon_auth import current_user, unauthorized
    user = await current_user(request)
    return None if user else unauthorized(request)


@app.get("/funds")
async def list_funds(request: Request):
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    return {"funds": [
        {"id": pid, **{k: v for k, v in f.items() if k != "aliases"},
         "aum": pid in _funds.aum_funds()}
        for pid, f in _funds.FUNDS.items()
    ]}


@app.get("/aum/funds")
async def aum_funds_list():
    """Funds this service can actually reconcile — DERIVED from stored data.

    Declared BEFORE /aum/{fund} because FastAPI matches in definition order and
    the parameterised route would otherwise swallow "funds" as a fund id.

    Exists so consumers stop hardcoding fund lists. Athena gated its recon pages
    on sets written into router.js, which went stale and were inverted in
    practice — Recon Report was offered only for a model portfolio that has no
    administrator, while the fund with a live feed could not reach it.
    """
    # `funds`, NOT aum_orchestrator: this is a static list off FUNDS, and the
    # orchestrator was never imported in this handler (every other /aum route
    # imports it locally), so the name was undefined and every call 500'd.
    # Found 2026-08-13 by the Athena page audit — the 500 was silently gating
    # whether the recon pages appeared in Athena's nav at all.
    import funds as _funds_mod
    return {"funds": _funds_mod.aum_funds()}


@app.get("/aum/{fund}")
async def aum_comparison(request: Request, fund: str, date: str = None):
    """Full three-way AUM comparison payload for a fund (see aum_orchestrator).

    ``date`` pins the Maia side (YYYY-MM-DD); the administrator pack is then
    date-matched to the Maia view actually used."""
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import aum_orchestrator
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    result = await aum_orchestrator.build_aum_comparison(pid, date)
    if result.get("error"):
        raise HTTPException(status_code=result.pop("status", 500),
                            detail=result["error"])
    return result


@app.get("/aum/{fund}/briefing.md")
async def aum_briefing_md(request: Request, fund: str, date: str = None):
    """The one-page board brief as raw markdown (for packs/emails)."""
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import aum_orchestrator
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    result = await aum_orchestrator.build_aum_comparison(pid, date)
    if result.get("error"):
        raise HTTPException(status_code=result.pop("status", 500),
                            detail=result["error"])
    from fastapi.responses import Response
    vdate = (result.get("meta") or {}).get("valuation_date") or "latest"
    return Response(
        content=result.get("briefing_md") or "",
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition":
                 f'attachment; filename="recon_brief_{pid}_{vdate}.md"'})


@app.get("/aum/{fund}/dates")
async def aum_dates(request: Request, fund: str):
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import aum_orchestrator
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    return await aum_orchestrator.available_dates(pid)


@app.post("/aum/upload/maia")
async def aum_upload_maia(
    request: Request,
    file: UploadFile = File(...),
    fund: str = "gdbf",
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Store a Maia export for the AUM recon, dated from its own contents.

    Separate from the legacy /upload/maia (bond-level recon_maia rows for
    wnbf/gcrif): this path validates shape (rejects 0-bond parses), resolves
    the data date via _maia_as_of, and registers ISIN-bearing files as
    bridge views too."""
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import aum_orchestrator
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Expected a .xlsx Maia export")
    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")
    result = await aum_orchestrator.ingest_maia(
        pid, contents, file.filename, x_user_email or "unknown")
    if result.get("status") == "error":
        raise HTTPException(status_code=422, detail=result.get("error"))
    return result


@app.post("/aum/upload/auto")
async def aum_upload_auto(
    request: Request,
    file: UploadFile = File(...),
    fund: str = "gdbf",
    x_user_email: str = Header(None, alias="X-User-Email"),
):
    """Content-routed intake for the OneDrive MAIA folder (or any drop).

    Filenames there carry no signal (tempMaia…, 'maia postions gdbf'), so
    the TYPE comes from reading the file (maia_classify): administrator
    packs → the admin pipeline; parseable Maia position/priced/allocation
    views → the AUM-recon ingest; AUM summaries and compliance dumps →
    raw-stored under their own registry source (kept, dated, not yet used
    by the comparison); anything else → rejected with the reason."""
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import aum_orchestrator
    import maia_classify
    import recon_db
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    kind = {"type": "unknown", "detail": "not an xlsx"}
    if (file.filename or "").lower().endswith((".xlsx", ".xls")):
        kind = maia_classify.classify_xlsx(contents)

    who = x_user_email or "unknown"
    if kind["type"] == "admin_pack":
        result = await process_admin_upload(contents, file.filename, who)
        result["detected_type"] = "admin_pack"
        # Warm the report cache so the first human view of this valuation
        # is ~1s, not a 100s GA10 build.
        aum_orchestrator.prewarm(result.get("portfolio_id") or pid,
                                 result.get("date"))
        return result
    if kind["type"] in ("maia_priced", "maia_full", "maia_allocation"):
        result = await aum_orchestrator.ingest_maia(pid, contents, file.filename, who)
        if result.get("status") == "error":
            raise HTTPException(status_code=422, detail=result.get("error"))
        result["detected_type"] = kind["type"]
        aum_orchestrator.prewarm(pid, result.get("date"))
        return result
    if kind["type"] in ("maia_aum", "maia_positions", "maia_compliance"):
        # Not (yet) consumable by the comparison — keep the file, dated by
        # upload day, under its own source so nothing from the folder is
        # silently dropped and a later feature can read the history.
        from datetime import date as _d
        src = {"maia_aum": "maia_aum_summary", "maia_positions": "maia_pos_raw",
               "maia_compliance": "maia_ptc"}[kind["type"]]
        path = await recon_db.store_raw_upload(
            src, pid, _d.today().isoformat(), contents, file.filename, who)
        return {"status": "stored", "detected_type": kind["type"],
                "detail": kind["detail"], "stored": path}
    raise HTTPException(status_code=422,
                        detail=f"unrecognised content: {kind['detail']}")


@app.get("/aum/{fund}/uploads")
async def aum_uploads(request: Request, fund: str):
    """Upload history for the AUM recon sources (audit trail for the UI)."""
    denied = await _aum_gate(request)
    if denied:
        return denied
    import funds as _funds
    import recon_db
    pid = _funds.resolve(fund)
    if not pid:
        raise HTTPException(status_code=404, detail=f"unknown fund {fund}")
    rows = await recon_db.list_uploads(portfolio_id=pid)
    keep = {"maia_aum", "maia_ref", "admin", "admin_payload",
            "maia_aum_summary", "maia_pos_raw", "maia_ptc"}
    return {"uploads": [r for r in rows if r.get("source") in keep]}


# ── Read endpoints ─────────────────────────────────────────────────────────

@app.get("/recon/data")
async def recon_data(portfolio_id: str = "wnbf", date: str = None):
    """Get display-ready recon data for a portfolio/date.

    Returns joined rows from all four tables (recon_bbg, recon_admin, recon_maia,
    recon_calcs) via the recon_view database view, with derived diffs computed
    server-side. Client just renders the rows.
    """
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")
    return await get_recon_data(portfolio_id, date)


ALLOWED_VIEWS = {
    "v_athena_bbg_accrued",
    "v_athena_admin_accrued",
    "v_athena_maia_accrued",
    "v_athena_all_accrued",
    "v_athena_bbg_yield",
    "v_athena_bbg_duration",
    "v_athena_bbg_value",
    "v_athena_admin_value",
    "v_athena_maia_value",
    "v_nav_summary",
}


@app.get("/recon/view/{view_name}")
async def recon_view_query(view_name: str, portfolio_id: str = "wnbf", date: str = None):
    """Query a named recon view directly. Returns pre-computed diffs from SQL."""
    if view_name not in ALLOWED_VIEWS:
        raise HTTPException(status_code=404, detail=f"Unknown view: {view_name}")
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")

    from recon_db import SUPABASE_URL, _headers
    import httpx

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/{view_name}",
            headers=_headers(),
            params={
                "portfolio_id": f"eq.{portfolio_id}",
                "date": f"eq.{date}",
                "select": "*",
            },
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Supabase query failed: {resp.status_code}")
        rows = resp.json()

    # For GCRIF (CNH fund), convert USD bond values to CNH
    if portfolio_id == "gcrif":
        fx = 6.912  # default
        try:
            async with httpx.AsyncClient(timeout=5) as fx_client:
                fx_resp = await fx_client.get(
                    f"{SUPABASE_URL}/rest/v1/recon_maia",
                    headers=_headers(),
                    params={
                        "portfolio_id": "eq.gcrif",
                        "fx_cnh_per_usd": "not.is.null",
                        "select": "fx_cnh_per_usd",
                        "order": "date.desc",
                        "limit": "1",
                    },
                )
                if fx_resp.status_code == 200:
                    fx_rows = fx_resp.json()
                    if fx_rows and fx_rows[0].get("fx_cnh_per_usd"):
                        fx = float(fx_rows[0]["fx_cnh_per_usd"])
        except Exception:
            pass

        for r in rows:
            ccy = (r.get("currency") or "").upper()
            if ccy == "USD":
                # admin_mv excluded: admin already reports MV in fund currency (CNH)
                for field in ("athena_mv", "bbg_mv", "maia_mv"):
                    if r.get(field) is not None:
                        try:
                            r[field] = float(r[field]) * fx
                        except (ValueError, TypeError):
                            pass
                r["_gcrif_fx"] = fx  # carry FX for later athena_mv recalc

    # Filter out BBG-echoed prices and substitute independent prices
    if "value" in view_name:
        # Fetch independent prices: admin + CBonds/GA10
        admin_prices = {}
        cbonds_prices = {}

        if date:
            try:
                async with httpx.AsyncClient(timeout=10) as ind_client:
                    # Admin prices (for all portfolios, not just GCRIF)
                    adm_task = ind_client.get(
                        f"{SUPABASE_URL}/rest/v1/recon_admin",
                        headers=_headers(),
                        params={
                            "portfolio_id": f"eq.{portfolio_id}",
                            "date": f"eq.{date}",
                            "select": "isin,price",
                        },
                    )
                    # CBonds/GA10 prices from ga10-pricing (non-BBG source for recon date)
                    import os
                    ga10_url = os.environ.get("GA10_PRICING_URL", "")
                    cb_task = ind_client.get(f"{ga10_url}/prices/by-date?date={date}")

                    import asyncio
                    adm_resp, cb_resp = await asyncio.gather(adm_task, cb_task, return_exceptions=True)

                    if not isinstance(adm_resp, Exception) and adm_resp.status_code == 200:
                        for ar in adm_resp.json():
                            if ar.get("isin") and ar.get("price") is not None:
                                admin_prices[ar["isin"]] = float(ar["price"])

                    if not isinstance(cb_resp, Exception) and cb_resp.status_code == 200:
                        for cb in cb_resp.json().get("bonds", []):
                            src = cb.get("source")
                            px = cb.get("price")
                            isin = cb.get("isin")
                            if isin and px is not None and src not in ("BBG", None):
                                if isin not in cbonds_prices:  # first non-BBG wins
                                    cbonds_prices[isin] = {"price": float(px), "source": src}
            except Exception:
                pass

        for r in rows:
            ap = r.get("athena_price")
            bp = r.get("bbg_price")
            isin = r.get("isin")

            # If athena_price matches BBG exactly, it's not independent — substitute
            if ap is not None and bp is not None:
                try:
                    if abs(float(ap) - float(bp)) < 0.001:
                        # Priority: CBonds/GA10 > admin > BBG
                        # But skip CBonds if it also matches BBG (echo via scheduled_job)
                        cb = cbonds_prices.get(isin)
                        adm_px = admin_prices.get(isin)

                        cb_is_echo = cb and abs(cb["price"] - float(bp)) < 0.001
                        if cb and not cb_is_echo:
                            r["athena_price"] = cb["price"]
                            r["athena_price_source"] = cb["source"]
                        elif adm_px is not None:
                            r["athena_price"] = adm_px
                            r["athena_price_source"] = "admin"
                        else:
                            r["athena_price"] = None
                            r["athena_price_source"] = None
                            r["athena_mv"] = None

                        if r["athena_price"] is not None:
                            r["px_diff"] = round(float(r["athena_price"]) - float(bp), 6)
                            par = r.get("athena_par") or r.get("nominal") or r.get("bbg_par") or r.get("admin_nominal")
                            if par is not None:
                                mv = float(par) * float(r["athena_price"]) / 100
                                # For GCRIF USD bonds, convert to CNH
                                if r.get("_gcrif_fx"):
                                    mv = mv * r["_gcrif_fx"]
                                r["athena_mv"] = mv
                        else:
                            r["px_diff"] = None

                        r["mv_diff"] = None
                except (ValueError, TypeError):
                    pass

    # Strip internal fields
    for r in rows:
        r.pop("_gcrif_fx", None)

    return {
        "view": view_name,
        "portfolio_id": portfolio_id,
        "date": date,
        "bonds": rows,
        "count": len(rows),
    }


@app.get("/recon/status")
async def recon_status(portfolio_id: str = None):
    """Get date×source coverage matrix."""
    rows = await get_recon_status(portfolio_id)
    return {"coverage": rows}


@app.get("/recon/latest-date")
async def recon_latest_date(portfolio_id: str = "wnbf", source: str = "bbg"):
    """Return the most recent date with data for a given source/portfolio."""
    from recon_db import SUPABASE_URL, _headers
    import httpx
    table = f"recon_{source}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_headers(),
            params={
                "portfolio_id": f"eq.{portfolio_id}",
                "select": "date",
                "order": "date.desc",
                "limit": "1",
            },
        )
        if resp.status_code == 200:
            rows = resp.json()
            if rows:
                return {"date": rows[0]["date"], "source": source, "portfolio_id": portfolio_id}
    return {"date": None, "source": source, "portfolio_id": portfolio_id}


@app.post("/sync/bond-data")
async def trigger_sync():
    """Manually trigger sync of bond identity/reference/analytics from bond-data.
    Also triggers GA10 recalc for all existing data to pick up convention changes."""
    result = await sync_bond_data()
    return result


@app.post("/recalc/accrued")
async def recalc_accrued_endpoint(portfolio_id: str = "wnbf", date: str = None, force: bool = False):
    """Fast accrued-only recalc. Delegates to recon_engine.recalc_accrued.
    Also fires automatically after every BBG upload (force=True).
    """
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")
    return await _recalc_accrued(portfolio_id, date, force)


@app.post("/diagnose/accrued")
async def diagnose_accrued_endpoint(
    isin: str,
    accrued: float,
    date: str,
    par: float = None,
):
    """Reverse-engineer what convention produces the given accrued amount for an ISIN/date.

    Pass any accrued value (from Maia, admin, or any other source) and get back
    a ranked list of (day_count, frequency, offset) combinations sorted by closeness.

    Example: /diagnose/accrued?isin=HK0000942448&accrued=12345.67&date=2026-04-08
    """
    return await diagnose_accrued_convention(isin, accrued, date, par)


@app.post("/webhooks/static-changed")
async def webhook_static_changed(request: Request, background_tasks: BackgroundTasks):
    """Supabase webhook: fires when local_bond_reference or bond_identity changes.

    Finds every (portfolio_id, date) row in athena_bbg for the affected ISIN and
    queues recalc_accrued for each — so Athena shows correct numbers immediately
    after any static data edit (day_count, frequency, coupon, maturity, bdc).

    Setup in Supabase Dashboard → Database → Webhooks:
      - Table: local_bond_reference  (athena project)  Events: UPDATE
      - Table: bond_identity         (bond-data project) Events: UPDATE
      Both pointing at: POST <recon-mcp-url>/webhooks/static-changed
      Header: X-Webhook-Secret = WEBHOOK_SECRET env var
    """
    import os, httpx
    secret = os.environ.get("WEBHOOK_SECRET")
    if secret and request.headers.get("X-Webhook-Secret", "") != secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    body = await request.json()
    record = body.get("record") or {}
    old_record = body.get("old_record") or {}
    isin = record.get("isin") or old_record.get("isin")
    if not isin:
        return {"status": "ignored", "reason": "no ISIN in payload"}

    table      = body.get("table", "unknown")
    event_type = body.get("type", "unknown")
    logger.info("webhook_static_changed: %s on %s isin=%s", event_type, table, isin)

    # Detect whether calc-affecting static actually changed. The Postgres trigger
    # fires on every UPDATE (e.g. source/source_description churn), so without
    # this guard we'd thrash ga10-pricing-mcp on no-op edits. We compare the
    # incoming and prior calc_hash; if old_record had no hash it's a first-write
    # and there's no historical analytics to recalc.
    new_calc_hash = record.get("calc_hash")
    old_calc_hash = old_record.get("calc_hash")
    calc_changed = (
        table == "bond_identity"
        and new_calc_hash
        and old_calc_hash
        and new_calc_hash != old_calc_hash
    )

    # Find every (portfolio_id, date) for this ISIN that we have accrued data for
    from recon_db import SUPABASE_URL, _headers
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/athena_bbg",
            headers=_headers(),
            params={"isin": f"eq.{isin}", "select": "portfolio_id,date"},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"athena_bbg fetch failed: {resp.status_code}")

    combos = {(r["portfolio_id"], r["date"]) for r in resp.json()}

    # Recalc fan-out is owned by the bond_recalc_queue + trg_notify_ga10_recalc
    # path on the bond-data Supabase. recon-mcp no longer pings ga10-pricing-mcp
    # for calc-history rewrites — that responsibility belongs upstream of us.
    # We still refresh athena_bbg below so portfolio recon picks up the new
    # static immediately.

    if not combos:
        return {
            "status": "ok",
            "isin": isin,
            "recalculated": 0,
            "calc_changed": calc_changed,
            "message": "ISIN not in athena_bbg",
        }

    async def _run_recalcs():
        results = await asyncio.gather(
            *[_recalc_accrued(pid, dt, force=True) for pid, dt in combos],
            return_exceptions=True,
        )
        n_ok = sum(1 for r in results if not isinstance(r, Exception))
        logger.info(
            "webhook_static_changed: isin=%s recalced %d/%d portfolio-dates",
            isin, n_ok, len(combos),
        )

    background_tasks.add_task(_run_recalcs)

    return {
        "status": "ok",
        "isin": isin,
        "table": table,
        "calc_changed": calc_changed,
        "portfolio_dates_queued": len(combos),
        "combos": [{"portfolio_id": p, "date": d} for p, d in sorted(combos)],
    }


@app.post("/sweep/refresh-athena")
async def sweep_refresh_athena(request: Request):
    """Synchronous bulk refresh of athena_bbg/recon_calcs for a list of ISINs.

    Driven by the ga10-pricing-mcp 5-min cron after recalcHistory bumps
    bond_analytics_dated. Per-ISIN result lets the caller decide whether to
    bump prior_calc_hash (only on success) so failed ISINs are retried next
    tick. Replaces the bond_identity webhook trigger.

    Auth: X-Webhook-Secret header must match WEBHOOK_SECRET env var.
    Body: {"isins": ["XS...", ...]}  (1-200)
    """
    import os, httpx
    secret = os.environ.get("WEBHOOK_SECRET")
    if secret and request.headers.get("X-Webhook-Secret", "") != secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    body = await request.json()
    isins = body.get("isins")
    if not isinstance(isins, list) or not isins:
        raise HTTPException(status_code=400, detail="body.isins must be a non-empty list")
    if len(isins) > 200:
        raise HTTPException(status_code=400, detail="max 200 isins per call")

    from recon_db import SUPABASE_URL, _headers
    isins_succeeded: list[str] = []
    isins_failed: list[dict] = []
    total_refreshed = 0

    async with httpx.AsyncClient(timeout=15) as client:
        for isin in isins:
            try:
                resp = await client.get(
                    f"{SUPABASE_URL}/rest/v1/athena_bbg",
                    headers=_headers(),
                    params={"isin": f"eq.{isin}", "select": "portfolio_id,date"},
                )
                if resp.status_code != 200:
                    isins_failed.append({"isin": isin, "error": f"athena_bbg fetch {resp.status_code}"})
                    continue

                combos = {(r["portfolio_id"], r["date"]) for r in resp.json()}
                if not combos:
                    isins_succeeded.append(isin)
                    continue

                results = await asyncio.gather(
                    *[_recalc_accrued(pid, dt, force=True) for pid, dt in combos],
                    return_exceptions=True,
                )
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    isins_failed.append({"isin": isin, "error": str(errors[0])[:200]})
                else:
                    isins_succeeded.append(isin)
                    total_refreshed += len(combos)
            except Exception as e:
                logger.exception("sweep_refresh_athena: isin=%s failed", isin)
                isins_failed.append({"isin": isin, "error": str(e)[:200]})

    logger.info(
        "sweep_refresh_athena: requested=%d succeeded=%d failed=%d portfolio_dates_refreshed=%d",
        len(isins), len(isins_succeeded), len(isins_failed), total_refreshed,
    )

    return {
        "requested": len(isins),
        "isins_succeeded": isins_succeeded,
        "isins_failed": isins_failed,
        "portfolio_dates_refreshed": total_refreshed,
    }


@app.post("/enrich/from-recon-bbg")
async def enrich_from_recon_bbg(portfolio_id: str = None):
    """Backfill local_bond_identity and local_bond_reference with maturity dates
    already stored in recon_bbg. Useful after deploying the enrichment feature —
    no need to re-upload files. Respects the locked flag on local_bond_reference."""
    from recon_db import SUPABASE_URL, _headers, enrich_bond_data_from_bbg
    import httpx

    params = {"select": "isin,maturity_date", "maturity_date": "not.is.null", "limit": "5000"}
    if portfolio_id:
        params["portfolio_id"] = f"eq.{portfolio_id}"

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(), params=params)

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"recon_bbg fetch failed: {resp.status_code}")

    # Deduplicate: use the most recently correct maturity per ISIN (they're consistent across dates)
    maturity_date_bonds = {}
    for r in resp.json():
        isin = r.get("isin")
        mat = r.get("maturity_date")
        if isin and mat:
            maturity_date_bonds[isin] = mat

    result = await enrich_bond_data_from_bbg(maturity_date_bonds, coupon_bonds={})
    result["source_isins"] = len(maturity_date_bonds)
    return result


@app.post("/recalc/all")
async def trigger_recalc_all():
    """Manually trigger GA10 recalc for all (portfolio, date) pairs in recon_bbg."""
    from recon_engine import recalc_all_existing
    result = await recalc_all_existing()
    return result


@app.post("/recalc/bond")
async def recalc_single_bond(isin: str, date: str, portfolio_id: str = "wnbf"):
    """Force recalc a single bond via GA10 gateway. Updates recon_calcs + athena_bbg."""
    import os
    import httpx
    from datetime import datetime, timedelta
    from recon_db import SUPABASE_URL, _headers, store_calcs, _upsert

    gw_url = os.environ.get("GA10_GATEWAY_URL", "")

    # Get BBG data for this bond (price + par)
    async with httpx.AsyncClient(timeout=45) as client:
        bbg_resp = await client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(), params={
            "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}", "isin": f"eq.{isin}",
            "select": "isin,par,price,accrued",
        })
        bbg_rows = bbg_resp.json() if bbg_resp.status_code == 200 else []
        if not bbg_rows:
            return {"error": f"No BBG data for {isin} on {date}"}
        bbg = bbg_rows[0]
        price = bbg.get("price")
        par = bbg.get("par") or 0
        if not price:
            return {"error": f"No price for {isin}"}

        price = float(price)
        par = float(par)

        # Settlement dates: T+0, C+1
        d0 = datetime.strptime(date, "%Y-%m-%d")
        c1_date = (d0 + timedelta(days=1)).strftime("%Y-%m-%d")

        # Fetch conventions from bond_reference directly (not gateway cache)
        from recon_db import BOND_DATA_URL, _bond_data_headers
        ref_resp = await client.get(
            f"{BOND_DATA_URL}/rest/v1/bond_reference",
            headers=_bond_data_headers(),
            params={"isin": f"eq.{isin}", "select": "coupon,maturity_date,day_count,frequency,issue_date,accrual_date"},
        )
        overrides = {}
        if ref_resp.status_code == 200:
            refs = ref_resp.json()
            if refs:
                r = refs[0]
                if r.get("coupon") is not None: overrides["coupon"] = float(r["coupon"])
                if r.get("maturity_date"): overrides["maturity_date"] = str(r["maturity_date"])
                if r.get("day_count"): overrides["day_count"] = r["day_count"]
                if r.get("frequency"): overrides["frequency"] = r["frequency"]
                if r.get("issue_date"): overrides["issue_date"] = str(r["issue_date"])
                # Only pass first_coupon_end when it differs from issue_date (genuine stub period).
                # If accrual_date == issue_date, GA10 treats it as an immediate coupon at issue
                # and corrupts the schedule into sub-annual periods.
                accrual = r.get("accrual_date")
                issue = r.get("issue_date")
                if accrual and accrual != issue:
                    overrides["first_coupon_end"] = str(accrual)
                if r.get("calendar"): overrides["calendar"] = r["calendar"]
                if r.get("business_convention"): overrides["business_convention"] = r["business_convention"]

        # Call gateway for T+0 and C+1 with explicit overrides
        async def call_gw(settle):
            resp = await client.post(f"{gw_url}/api/v3/bond/analysis", json={
                "isin": isin, "price": price, "settlement_date": settle,
                "overrides": overrides if overrides else None,
            })
            if resp.status_code == 200:
                return resp.json().get("analytics", {})
            return {}

        t0, c1 = await asyncio.gather(call_gw(date), call_gw(c1_date))

        # Build recon_calcs row
        calc = {
            "isin": isin,
            "source_price": price,
            "ga10_accrued": t0.get("accrued_interest"),
            "ga10_accrued_c1": c1.get("accrued_interest"),
            "ga10_accrued_t1": None,
            "ga10_accrued_t2": None,
            "ga10_accrued_t3": None,
            "ga10_yield": t0.get("yield_to_maturity") or t0.get("ytm"),
            "ga10_yield_c1": c1.get("ytm") or c1.get("yield_to_maturity"),
            "ga10_yield_t1": None,
            "ga10_yield_worst": t0.get("ytw") or t0.get("yield_to_maturity") or t0.get("ytm"),
            "ga10_ytal": t0.get("ytal"),
            "ga10_duration": t0.get("modified_duration") or t0.get("duration"),
            "ga10_duration_worst": t0.get("duration_worst"),
            "ga10_spread": t0.get("spread"),
            "ga10_convexity": t0.get("convexity"),
            "ga10_dv01": t0.get("dv01") or t0.get("pvbp"),
            "convention_used": t0.get("day_count"),
            "last_coupon_date": t0.get("last_coupon_date"),
            "issue_date": t0.get("issue_date"),
        }
        await store_calcs(portfolio_id, date, [calc])

        # Build athena_bbg row if we have par
        if par:
            mult = par / 100
            def _scale(v):
                return v * mult if v is not None else None
            athena_row = {
                "isin": isin,
                "par": par,
                "source_price": price,
                "accrued_t0": _scale(t0.get("accrued_interest")),
                "accrued_c1": _scale(c1.get("accrued_interest")),
                "accrued_t1": None, "accrued_c2": None, "accrued_c3": None,
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
            # Upsert just this one bond — don't use store_athena_bbg which deletes stale rows
            upsert_row = {"portfolio_id": portfolio_id, "date": date, **athena_row}
            await _upsert("athena_bbg", [upsert_row], "portfolio_id,date,isin")

        return {
            "isin": isin,
            "date": date,
            "portfolio_id": portfolio_id,
            "price": price,
            "par": par,
            "accrued_t0_per100": t0.get("accrued_interest"),
            "accrued_c1_per100": c1.get("accrued_interest"),
            "accrued_c1_scaled": c1.get("accrued_interest", 0) * (par / 100) if par and c1.get("accrued_interest") else None,
            "yield": t0.get("yield_to_maturity") or t0.get("ytm"),
            "duration": t0.get("modified_duration") or t0.get("duration"),
            "frequency": t0.get("frequency"),
            "day_count": t0.get("day_count"),
        }


@app.post("/recalc/stale")
async def recalc_stale(limit: int = 200):
    """Safety-net cron: recalc rows whose provenance no longer matches.

    Reads v_stale_calcs (sql/005) for static_drift, plus a separate query
    against recon_calcs for engine_drift (using the engine ids returned by
    GA10 /engine/version, since the view's engine columns require GUC vars
    not easily set per-request via PostgREST).

    Groups stale rows by (portfolio_id, date) and calls the existing
    recalc_with_bbg_prices for each group, which stamps fresh provenance —
    so the same row should not appear stale on the next pass.

    Wire to a Railway cron (or any external scheduler) at e.g. */15 * * * *.
    Idempotent: safe to fire concurrently or repeatedly.
    """
    import httpx
    from recon_db import SUPABASE_URL, _headers
    from recon_engine import recalc_with_bbg_prices
    from calc_hashes import fetch_engine_version, engine_ids_from

    engine_pricing_id, engine_gateway_id = engine_ids_from(await fetch_engine_version())

    stale: dict[tuple[str, str], set[str]] = {}

    async with httpx.AsyncClient(timeout=30) as client:
        # Static drift via v_stale_calcs (engine columns inert without GUC)
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/v_stale_calcs",
            headers=_headers(),
            params={
                "select": "portfolio_id,date,isin",
                "static_drift": "is.true",
                "limit": str(limit),
            },
        )
        if resp.status_code == 200:
            for r in resp.json():
                stale.setdefault((r["portfolio_id"], r["date"]), set()).add(r["isin"])

        # Engine drift detected client-side against fetched current ids
        if engine_pricing_id:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/recon_calcs",
                headers=_headers(),
                params={
                    "select": "portfolio_id,date,isin",
                    "calc_engine_pricing_id": f"neq.{engine_pricing_id}",
                    "limit": str(limit),
                },
            )
            if resp.status_code == 200:
                for r in resp.json():
                    stale.setdefault((r["portfolio_id"], r["date"]), set()).add(r["isin"])

    if not stale:
        return {"checked": 0, "fired_groups": 0, "fired_isins": 0}

    fired_isins = 0
    fired_groups = 0
    failures: list[str] = []

    for (portfolio_id, date), isins_set in stale.items():
        isins = list(isins_set)
        # Pull BBG price + par for this group, then call recalc_with_bbg_prices
        # which already stamps provenance from step 2.
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/recon_bbg",
                headers=_headers(),
                params={
                    "portfolio_id": f"eq.{portfolio_id}",
                    "date": f"eq.{date}",
                    "isin": f"in.({','.join(isins)})",
                    "select": "isin,price,par",
                },
            )
            if resp.status_code != 200:
                failures.append(f"{portfolio_id}/{date}: bbg fetch {resp.status_code}")
                continue
            bbg_rows = resp.json()

        bbg_prices = {r["isin"]: float(r["price"]) for r in bbg_rows if r.get("price") is not None}
        bbg_par    = {r["isin"]: float(r["par"])   for r in bbg_rows if r.get("par")   is not None}

        if not bbg_prices:
            failures.append(f"{portfolio_id}/{date}: no BBG prices for stale ISINs")
            continue

        try:
            count = await recalc_with_bbg_prices(bbg_prices, date, portfolio_id, bbg_par)
            fired_isins += count
            fired_groups += 1
        except Exception as e:
            failures.append(f"{portfolio_id}/{date}: {e!r}")

    return {
        "checked": sum(len(v) for v in stale.values()),
        "fired_groups": fired_groups,
        "fired_isins": fired_isins,
        "engine_pricing_id": engine_pricing_id,
        "engine_gateway_id": engine_gateway_id,
        "failures": failures,
    }


@app.get("/recon/athena-v-ga10")
async def athena_v_ga10(portfolio_id: str = "wnbf", date: str = None):
    """Detect static drift in stored bond_analytics_dated via calc_hash compare.

    No GA10 recalc — relies on the calc_hash stamping mechanism. When
    bond_identity.calc_hash differs from bond_analytics_dated.calc_hash, the
    static (coupon, day-count, maturity, etc.) has changed since the row
    was last stamped and the hopper needs to recompute it.

    Replaces the previous per-bond GA10 v3 /bond/analysis recompute (~2s
    per call × N bonds = often >30s and timing out at Railway's HTTP cap,
    producing the "Internal Server Error" the Athena recon page rendered
    as an Unexpected token JSON parse error). Now ~1s for any portfolio
    size because all work is in 2 batched Supabase queries.

    Description mismatch logic preserved: compares the stored row's
    description against v_bond_identity_resolved.description, flags
    desc_mismatches for the summary panel.

    Response shape preserved so the calc-drift widget keeps rendering.
    When calc_hash matches: fresh = stored, diff = zeros, drift = false
    (the stamp is the proof). When calc_hash differs: fresh = null,
    diff = null, drift = true with an "error" message naming the cause.
    """
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")

    import httpx
    import asyncio

    from recon_db import (
        SUPABASE_URL, BOND_DATA_URL,
        _headers, _bond_data_headers,
    )

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Portfolio bonds from recon_bbg (gives ISIN universe + bbg price/desc).
        if portfolio_id:
            bbg_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/recon_bbg",
                headers=_headers(),
                params={
                    "portfolio_id": f"eq.{portfolio_id}",
                    "date": f"eq.{date}",
                    "select": "isin,par,price,description",
                },
            )
            bbg_rows = bbg_resp.json() if bbg_resp.status_code == 200 else []
        else:
            bbg_rows = []

        portfolio_isins = [r["isin"] for r in bbg_rows if r.get("isin")]
        bbg_map = {r["isin"]: r for r in bbg_rows}

        if not portfolio_isins:
            return {
                "portfolio_id": portfolio_id,
                "date": date,
                "bonds": [],
                "count": 0,
                "summary": {"total": 0, "matched": 0, "drifted": 0, "desc_mismatches": 0,
                            "engine": {"stored": "ga10-stamped", "fresh": "calc_hash_compare"},
                            "thresholds": {"ytm_bps": 5, "duration": 0.1, "spread_bps": 10}},
            }

        # 2. Stored analytics + current identity (parallel, filtered to portfolio ISINs).
        isin_list = ",".join(portfolio_isins)
        analytics_task = client.get(
            f"{BOND_DATA_URL}/rest/v1/bond_analytics_dated",
            headers=_bond_data_headers(),
            params={
                "isin": f"in.({isin_list})",
                "price_date": f"eq.{date}",
                "source": "in.(scheduled_job,carried_forward,BBG)",
                "select": (
                    "isin,source,provider_detail,price,description,"
                    "yield_to_maturity,modified_duration,spread,accrued_interest,"
                    "calc_hash"
                ),
                "order": "isin.asc,source.asc",
            },
        )
        identity_task = client.get(
            f"{BOND_DATA_URL}/rest/v1/v_bond_identity_resolved",
            headers=_bond_data_headers(),
            params={
                "isin": f"in.({isin_list})",
                "select": "isin,description,calc_hash",
            },
        )

        analytics_resp, identity_resp = await asyncio.gather(
            analytics_task, identity_task, return_exceptions=True
        )

        if isinstance(analytics_resp, Exception) or analytics_resp.status_code != 200:
            raise HTTPException(status_code=502, detail="bond_analytics_dated fetch failed")
        analytics_rows = analytics_resp.json()

        identity_map = {}
        if not isinstance(identity_resp, Exception) and identity_resp.status_code == 200:
            for row in identity_resp.json():
                identity_map[row["isin"]] = row

        # Pick the best analytics row per ISIN — prefer scheduled_job > BBG > carried_forward.
        # Ordering done in SQL gives us one row per ISIN in source.asc; iterate and keep first.
        priority = {"scheduled_job": 0, "BBG": 1, "carried_forward": 2}
        chosen: dict[str, dict] = {}
        for row in analytics_rows:
            isin = row["isin"]
            existing = chosen.get(isin)
            if existing is None:
                chosen[isin] = row
                continue
            if priority.get(row["source"], 99) < priority.get(existing["source"], 99):
                chosen[isin] = row

        # 3. Build comparison + summary.
        results = []
        drifted = 0
        desc_mismatches = 0

        for isin in portfolio_isins:
            stored = chosen.get(isin)
            bbg = bbg_map.get(isin, {})
            identity = identity_map.get(isin, {})

            stored_desc = (stored or {}).get("description") or bbg.get("description")
            identity_desc = identity.get("description")
            desc_match = True
            if stored_desc and identity_desc and stored_desc.strip() != identity_desc.strip():
                desc_match = False
                desc_mismatches += 1

            if not stored:
                # No stored analytics row for this ISIN on this date — flag as drifted.
                results.append({
                    "isin": isin,
                    "source": None,
                    "price": bbg.get("price"),
                    "description": {"stored": stored_desc, "identity": identity_desc, "match": desc_match},
                    "stored": {"ytm": None, "duration": None, "spread": None, "accrued": None},
                    "fresh": {"ytm": None, "duration": None, "spread": None, "accrued": None},
                    "diff": {"ytm_bps": None, "duration": None, "spread": None, "accrued": None},
                    "drift": True,
                    "error": "no stored analytics row for this date",
                })
                drifted += 1
                continue

            s_ytm = stored.get("yield_to_maturity") or 0
            s_dur = stored.get("modified_duration") or 0
            s_spr = stored.get("spread") or 0
            s_accrued = stored.get("accrued_interest") or 0
            row_hash = stored.get("calc_hash")
            curr_hash = identity.get("calc_hash")

            hash_match = bool(row_hash) and bool(curr_hash) and row_hash == curr_hash

            if hash_match:
                # Stamp is valid — stored values are good. Show fresh=stored, no diff.
                results.append({
                    "isin": isin,
                    "source": stored.get("source"),
                    "price": stored.get("price"),
                    "description": {"stored": stored_desc, "identity": identity_desc, "match": desc_match},
                    "stored": {
                        "ytm": round(s_ytm, 4),
                        "duration": round(s_dur, 3),
                        "spread": round(s_spr, 1),
                        "accrued": round(s_accrued, 4),
                    },
                    "fresh": {
                        "ytm": round(s_ytm, 4),
                        "duration": round(s_dur, 3),
                        "spread": round(s_spr, 1),
                        "accrued": round(s_accrued, 4),
                    },
                    "diff": {"ytm_bps": 0, "duration": 0, "spread": 0, "accrued": 0},
                    "drift": False,
                })
            else:
                # calc_hash mismatch — static drifted since this row was stamped.
                # Surface the hashes so a recon viewer can see the proof of drift.
                results.append({
                    "isin": isin,
                    "source": stored.get("source"),
                    "price": stored.get("price"),
                    "description": {"stored": stored_desc, "identity": identity_desc, "match": desc_match},
                    "stored": {
                        "ytm": round(s_ytm, 4),
                        "duration": round(s_dur, 3),
                        "spread": round(s_spr, 1),
                        "accrued": round(s_accrued, 4),
                    },
                    "fresh": {"ytm": None, "duration": None, "spread": None, "accrued": None},
                    "diff": {"ytm_bps": None, "duration": None, "spread": None, "accrued": None},
                    "drift": True,
                    "hashes": {"stamped": row_hash, "current": curr_hash},
                    "error": "static drifted since stamp — hopper needs to recompute",
                })
                drifted += 1

        # Sort: drifted bonds first, then desc mismatches, then by ISIN.
        results.sort(key=lambda r: (
            not r.get("drift", False),
            r.get("description", {}).get("match", True),
            r["isin"],
        ))

        return {
            "portfolio_id": portfolio_id,
            "date": date,
            "bonds": results,
            "count": len(results),
            "summary": {
                "total": len(results),
                "matched": len(results) - drifted,
                "drifted": drifted,
                "desc_mismatches": desc_mismatches,
                "engine": {"stored": "ga10-stamped", "fresh": "calc_hash_compare"},
                "thresholds": {"ytm_bps": 5, "duration": 0.1, "spread_bps": 10},
            },
        }


@app.post("/recalc/portfolio")
async def recalc_portfolio(portfolio_id: str = "gcrif", date: str = None):
    """
    Batch recalc all bonds for a portfolio on a given date.

    For each ISIN:
      - Fetches BBG price + par from recon_bbg
      - Fetches conventions from v_portfolio_bond_reference (bond_reference + bond_identity)
      - Calls GA10 for 4 settlement dates: T+0, T+1, T+2, T+3 (= C+1, C+2, C+3)
      - Stores accrued, ytm, ytw, ytal, duration + metadata (days_accrued,
        last_coupon_date, accrual_convention, biz_convention) AS RETURNED BY GA10
      - Upserts recon_calcs and athena_bbg, overwriting any existing row for
        this portfolio/date/isin

    Returns a grid of results — one row per ISIN.
    """
    import os
    import httpx
    from datetime import datetime as dt, timedelta
    from recon_db import SUPABASE_URL, BOND_DATA_URL, _headers, _bond_data_headers, store_calcs, _upsert

    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")

    gw_url = os.environ.get("GA10_GATEWAY_URL", "")
    d0 = dt.strptime(date, "%Y-%m-%d").date()

    # Settlement offsets: T+0 = same day, then +1, +2, +3 calendar days
    offsets = {"t0": 0, "c1": 1, "t1": 1, "c2": 2, "t2": 2, "c3": 3, "t3": 3}
    settle_dates = {k: (d0 + timedelta(days=v)).strftime("%Y-%m-%d") for k, v in offsets.items()}

    async with httpx.AsyncClient(timeout=60) as client:

        # 1. Get all ISINs + BBG prices + par for this portfolio/date
        bbg_resp = await client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(), params={
            "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
            "select": "isin,par,price",
        })
        bbg_rows = bbg_resp.json() if bbg_resp.status_code == 200 else []
        if not bbg_rows:
            return JSONResponse({"error": f"No BBG data for {portfolio_id} on {date}"}, status_code=404)

        bbg_by_isin = {r["isin"]: r for r in bbg_rows}

        # 2. Get conventions for all ISINs from v_portfolio_bond_reference
        ref_resp = await client.get(
            f"{BOND_DATA_URL}/rest/v1/v_portfolio_bond_reference",
            headers=_bond_data_headers(),
            params={
                "portfolio_id": f"eq.{portfolio_id}",
                "select": "isin,coupon,maturity_date,day_count,frequency,issue_date,accrual_date,calendar,business_convention",
            },
        )
        ref_rows = ref_resp.json() if ref_resp.status_code == 200 else []
        ref_by_isin = {r["isin"]: r for r in ref_rows}

        def build_overrides(r):
            ov = {}
            if r.get("coupon") is not None: ov["coupon"] = float(r["coupon"])
            if r.get("maturity_date"): ov["maturity_date"] = str(r["maturity_date"])
            if r.get("day_count"): ov["day_count"] = r["day_count"]
            if r.get("frequency"): ov["frequency"] = r["frequency"]
            if r.get("issue_date"): ov["issue_date"] = str(r["issue_date"])
            accrual = r.get("accrual_date"); issue = r.get("issue_date")
            if accrual and accrual != issue:
                ov["first_coupon_end"] = str(accrual)
            if r.get("calendar"): ov["calendar"] = r["calendar"]
            if r.get("business_convention"): ov["business_convention"] = r["business_convention"]
            return ov

        def parse_ga10(resp_json):
            """Extract metrics from GA10 response. Returns dict or None."""
            if not resp_json:
                return None
            a = resp_json.get("analytics") or resp_json
            if not a or a.get("error"):
                return None
            conv = a.get("conventions") or {}
            return {
                "accrued":      a.get("accrued_interest"),
                "ytm":          a.get("ytm") or a.get("yield_to_maturity"),
                "ytw":          a.get("ytw"),
                "ytal":         a.get("ytal"),
                "duration":     a.get("duration") or a.get("modified_duration"),
                "duration_worst": a.get("duration_worst"),
                "spread":       a.get("spread"),
                "convexity":    a.get("convexity"),
                "dv01":         a.get("pvbp") or a.get("dv01"),
                "days_accrued": a.get("accrued_days"),
                "last_coupon_date": a.get("last_coupon_date") or conv.get("last_coupon_date"),
                "accrual_convention": conv.get("day_count") or a.get("day_count"),
                "biz_convention": conv.get("business_day_convention") or a.get("business_convention"),
                "frequency_used": conv.get("frequency") or a.get("frequency"),
            }

        async def call_ga10(isin, price, settle, overrides):
            try:
                resp = await client.post(f"{gw_url}/api/v3/bond/analysis", json={
                    "isin": isin, "price": price, "settlement_date": settle,
                    "overrides": overrides if overrides else None,
                })
                return parse_ga10(resp.json()) if resp.status_code == 200 else None
            except Exception:
                return None

        # 3. Loop through each ISIN, call GA10 for 4 unique settlement dates concurrently
        results = []
        unique_dates = ["t0", "c1", "c2", "c3"]  # t1=c1, t2=c2, t3=c3

        for isin, bbg in bbg_by_isin.items():
            price = bbg.get("price")
            par = float(bbg.get("par") or 0)
            if not price:
                continue
            price = float(price)
            overrides = build_overrides(ref_by_isin.get(isin, {}))

            # Call GA10 for 4 settlement dates in parallel
            ga10_results = await asyncio.gather(*[
                call_ga10(isin, price, settle_dates[k], overrides)
                for k in unique_dates
            ])
            r = dict(zip(unique_dates, ga10_results))

            # t1=c1, t2=c2, t3=c3
            r["t1"] = r["c1"]; r["t2"] = r["c2"]; r["t3"] = r["c3"]

            # Build recon_calcs row — per-100 values
            def v(key, field): return r[key][field] if r.get(key) and r[key] else None

            calc = {
                "isin": isin, "source_price": price,
                "ga10_accrued":      v("t0", "accrued"),
                "ga10_accrued_c1":   v("c1", "accrued"),
                "ga10_accrued_t1":   v("t1", "accrued"),
                "ga10_accrued_t2":   v("c2", "accrued"),
                "ga10_accrued_t3":   v("c3", "accrued"),
                "ga10_yield":        v("t0", "ytm"),
                "ga10_yield_c1":     v("c1", "ytm"),
                "ga10_yield_t1":     v("t1", "ytm"),
                "ga10_yield_worst":  v("t0", "ytw"),
                "ga10_ytal":         v("t0", "ytal"),
                "ga10_duration":     v("t0", "duration"),
                "ga10_duration_worst": v("t0", "duration_worst"),
                "ga10_spread":       v("t0", "spread"),
                "ga10_convexity":    v("t0", "convexity"),
                "ga10_dv01":         v("t0", "dv01"),
                # Metadata from GA10 response (not from DB — reflects what GA10 actually used)
                "days_accrued_c1":        v("c1", "days_accrued"),
                "last_coupon_date":        v("c1", "last_coupon_date"),
                "accrual_convention_used": v("c1", "accrual_convention"),
                "biz_convention_used":     v("c1", "biz_convention"),
                "frequency_used":          v("c1", "frequency_used"),
            }
            await store_calcs(portfolio_id, date, [calc])

            # Update athena_bbg with par-scaled accrued + GA10 metadata
            if par:
                mult = par / 100
                await _upsert("athena_bbg", [{
                    "portfolio_id": portfolio_id, "date": date, "isin": isin,
                    "par": par, "source_price": price,
                    "accrued_t0": v("t0", "accrued") * mult if v("t0", "accrued") else None,
                    "accrued_c1": v("c1", "accrued") * mult if v("c1", "accrued") else None,
                    "accrued_t1": v("t1", "accrued") * mult if v("t1", "accrued") else None,
                    "accrued_c2": v("c2", "accrued") * mult if v("c2", "accrued") else None,
                    "accrued_c3": v("c3", "accrued") * mult if v("c3", "accrued") else None,
                    "last_coupon_date": v("c1", "last_coupon_date"),
                    "days_accrued": v("c1", "days_accrued"),
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                }], "portfolio_id,date,isin")

            # Build result row for the response grid
            results.append({
                "isin": isin,
                "price": price,
                "par": par,
                "overrides_sent": list(overrides.keys()),
                "t0":  r["t0"],
                "c1":  r["c1"],
                "c2":  r["c2"],
                "c3":  r["c3"],
            })

    return {
        "portfolio_id": portfolio_id,
        "date": date,
        "bonds_processed": len(results),
        "settlement_dates": {k: settle_dates[k] for k in unique_dates},
        "results": results,
    }


@app.post("/backfill/coupon-maturity")
async def trigger_backfill(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    """Manually trigger the coupon/maturity backfill. Admin only."""
    # Simple admin gate — could be improved with proper auth later
    import os
    expected = os.environ.get("RECON_MCP_ADMIN_KEY", "")
    if expected and x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Forbidden")

    bbg = await backfill_coupon_maturity("recon_bbg")
    maia = await backfill_coupon_maturity("recon_maia")
    return {"recon_bbg": bbg, "recon_maia": maia}
