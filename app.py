"""
Recon MCP - Reconciliation service for bond portfolio data.

Owns all parsing, enrichment, storage, and display logic for BBG, admin NAV,
Maia holdings, and GA10 QuantLib calculations. Athena and other apps just
call this service for display-ready recon data.
"""

import asyncio
import logging
from datetime import datetime

from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from recon_db import (
    get_recon_data,
    get_recon_status,
    backfill_coupon_maturity,
    sync_bond_data,
)
from recon_engine import (
    process_bbg_upload,
    process_admin_upload,
    process_maia_upload,
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


async def _startup_backfill():
    """On startup, sync bond data from bond-data Supabase and backfill missing fields."""
    await asyncio.sleep(5)  # Let the server finish starting
    try:
        # Sync bond identity/reference/analytics into local tables
        sync_result = await sync_bond_data()
        _backfill_status["bond_data_sync"] = sync_result
        logger.info("Bond data sync complete: %s", sync_result)

        bbg_result = await backfill_coupon_maturity("recon_bbg")
        maia_result = await backfill_coupon_maturity("recon_maia")
        _backfill_status["coupon_maturity"] = {
            "recon_bbg": bbg_result,
            "recon_maia": maia_result,
        }
        logger.info("Startup backfill complete: %s", _backfill_status)
    except Exception as e:
        logger.error(f"Startup backfill failed: {e}")
        _backfill_status["coupon_maturity"] = {"error": str(e)}


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_startup_backfill())


# ── Health + manifest ──────────────────────────────────────────────────────

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
        "version_hash": "v1_20260405",
        "summary": "Bond reconciliation — BBG, admin NAV, Maia holdings, and GA10 QuantLib calcs joined by ISIN.",
        "base_url": "https://recon-mcp-production.up.railway.app",
        "capabilities": [
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
            {"id": "main", "name": "Main", "path": "/", "description": "Recon service root."}
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
                for field in ("athena_mv", "bbg_mv", "admin_mv", "maia_mv", "nominal"):
                    if r.get(field) is not None:
                        try:
                            r[field] = float(r[field]) * fx
                        except (ValueError, TypeError):
                            pass
                r["_fx_converted"] = True
                r["_fx_rate"] = fx

    # Filter out BBG-echoed prices: if athena_price matches bbg_price exactly,
    # it's not independent — GA10 was fed BBG prices and returned them.
    if "value" in view_name:
        for r in rows:
            ap = r.get("athena_price")
            bp = r.get("bbg_price")
            if ap is not None and bp is not None:
                try:
                    if abs(float(ap) - float(bp)) < 0.001:
                        r["athena_price"] = None
                        r["athena_price_source"] = None
                        r["athena_mv"] = None
                        r["px_diff"] = None
                        r["mv_diff"] = None
                except (ValueError, TypeError):
                    pass

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


@app.post("/recalc/all")
async def trigger_recalc_all():
    """Manually trigger GA10 recalc for all (portfolio, date) pairs in recon_bbg."""
    from recon_engine import recalc_all_existing
    result = await recalc_all_existing()
    return result


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
