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
)
from recon_engine import (
    process_bbg_upload,
    process_admin_upload,
    process_maia_upload,
)

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
    """On startup, fill in any missing coupon/maturity from bond_reference."""
    await asyncio.sleep(5)  # Let the server finish starting
    try:
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


@app.get("/recon/status")
async def recon_status(portfolio_id: str = None):
    """Get date×source coverage matrix."""
    rows = await get_recon_status(portfolio_id)
    return {"coverage": rows}


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
