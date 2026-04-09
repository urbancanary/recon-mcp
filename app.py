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
    sync_orca_holdings,
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
    # Auto-trigger accrued recalc so the recon tab populates immediately
    pid = result.get("portfolio_id")
    bbg_date = result.get("date")
    if pid and bbg_date:
        asyncio.create_task(_do_recalc_accrued(pid, bbg_date, force=True))
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
                asyncio.create_task(_do_recalc_accrued(pid, bbg_date, force=True))
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
                    ga10_url = os.environ.get("GA10_PRICING_URL", "https://ga10-pricing.urbancanary.workers.dev")
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


async def _do_recalc_accrued(portfolio_id: str, date: str, force: bool = False) -> dict:
    """Core accrued recalc logic — callable internally or from the HTTP endpoint."""

    import httpx
    from datetime import datetime, timedelta
    from recon_db import SUPABASE_URL, _headers, _upsert

    def _accrued_at(settle: "datetime", coupon: float, freq: int, mat: "datetime",
                    coup_months: list, coup_day: int, day_count: str, par: float) -> float:
        """Compute accrued interest for a single settlement date."""
        # Find last coupon date before this settlement
        last_coupon = None
        for y in [settle.year, settle.year - 1]:
            for m in sorted(coup_months, reverse=True):
                try:
                    cd = datetime(y, m, coup_day)
                except ValueError:
                    cd = datetime(y, m, 28)
                if cd < settle:
                    if last_coupon is None or cd > last_coupon:
                        last_coupon = cd
                    break
        if not last_coupon:
            return 0.0

        if "30" in day_count:
            d1_day = min(last_coupon.day, 30)
            d2_day = min(settle.day, 30) if d1_day == 30 else settle.day
            days = (settle.year - last_coupon.year) * 360 + (settle.month - last_coupon.month) * 30 + (d2_day - d1_day)
            accrued_per_100 = coupon / freq * days / (360 / freq)
        else:
            actual_days = (settle - last_coupon).days
            next_coupon = None
            for m in sorted(coup_months):
                try:
                    nc = datetime(last_coupon.year if m > last_coupon.month else last_coupon.year + 1, m, coup_day)
                except ValueError:
                    nc = datetime(last_coupon.year if m > last_coupon.month else last_coupon.year + 1, m, 28)
                if nc > last_coupon:
                    next_coupon = nc
                    break
            if not next_coupon:
                next_coupon = last_coupon + timedelta(days=182)
            period_days = (next_coupon - last_coupon).days
            accrued_per_100 = coupon / freq * actual_days / period_days

        return round(accrued_per_100 * par / 100, 6)

    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Fetch all sources in parallel
        bbg_task = client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(), params={
            "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
            "select": "isin,par,price,accrued,maturity_date",
        })
        athena_task = client.get(f"{SUPABASE_URL}/rest/v1/athena_bbg", headers=_headers(), params={
            "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
            "select": "isin,par,accrued_c1,source_price",
        })
        ref_task = client.get(f"{SUPABASE_URL}/rest/v1/local_bond_reference", headers=_headers(), params={
            "select": "isin,coupon,maturity_date,day_count",
        })
        holdings_task = client.get(f"{SUPABASE_URL}/rest/v1/orca_holdings", headers=_headers(), params={
            "portfolio_id": f"eq.{portfolio_id}",
            "select": "isin,par_amount",
        })

        bbg_resp, athena_resp, ref_resp, holdings_resp = await asyncio.gather(
            bbg_task, athena_task, ref_task, holdings_task
        )

        bbg_bonds = {r["isin"]: r for r in (bbg_resp.json() if bbg_resp.status_code == 200 else [])}
        athena_map = {r["isin"]: r for r in (athena_resp.json() if athena_resp.status_code == 200 else [])}
        ref_map = {r["isin"]: r for r in (ref_resp.json() if ref_resp.status_code == 200 else [])}
        holdings_map = {r["isin"]: float(r["par_amount"]) for r in (holdings_resp.json() if holdings_resp.status_code == 200 else []) if r.get("par_amount")}

        # 2. Find bonds needing recalc
        trade_date = datetime.strptime(date, "%Y-%m-%d")
        needs_recalc = []
        for isin, bbg in bbg_bonds.items():
            if force:
                needs_recalc.append(isin)
                continue
            athena = athena_map.get(isin, {})
            c1 = athena.get("accrued_c1")
            bbg_accrued = bbg.get("accrued")
            if c1 is None or bbg_accrued is None:
                needs_recalc.append(isin)
            elif bbg_accrued != 0 and abs((c1 - bbg_accrued) / bbg_accrued) > 0.0001:
                needs_recalc.append(isin)

        if not needs_recalc:
            return {"recalculated": 0, "message": "All bonds already match BBG"}

        # 3. Compute accrued at all settlement offsets (T+0, C+1/T+1, C+2, C+3)
        updated_rows = []
        skipped = []

        for isin in needs_recalc:
            ref = ref_map.get(isin)
            bbg = bbg_bonds[isin]
            athena = athena_map.get(isin, {})

            if not ref or not ref.get("coupon") or not ref.get("maturity_date"):
                skipped.append({"isin": isin, "reason": "missing reference data"})
                continue

            coupon = float(ref["coupon"])
            # Prefer BBG-parsed maturity (from Long Name, exact date) over CBonds which rounds to month-end
            maturity = bbg.get("maturity_date") or ref["maturity_date"]
            day_count = ref.get("day_count") or "30/360"

            # Par priority: orca_holdings > bbg > athena_bbg
            par = holdings_map.get(isin) or (float(bbg["par"]) if bbg.get("par") else None) or (float(athena["par"]) if athena.get("par") else None)
            if not par:
                skipped.append({"isin": isin, "reason": "no par amount"})
                continue

            price = float(bbg["price"]) if bbg.get("price") else (float(athena["source_price"]) if athena.get("source_price") else None)
            if not price:
                skipped.append({"isin": isin, "reason": "no price"})
                continue

            mat = datetime.strptime(maturity[:10], "%Y-%m-%d")
            freq = 2  # semi-annual
            coup_months = sorted(set([(mat.month - 1) % 12 + 1, ((mat.month + 5) % 12) + 1]))
            coup_day = min(mat.day, 28)

            args = (coupon, freq, mat, coup_months, coup_day, day_count, par)
            used_convention = day_count if "30" not in day_count else "30/360"
            updated_rows.append({
                "isin": isin,
                "par": par,
                "source_price": price,
                "day_count": used_convention,
                "accrued_t0": _accrued_at(trade_date,            *args),
                "accrued_c1": _accrued_at(trade_date + timedelta(days=1), *args),
                "accrued_t1": _accrued_at(trade_date + timedelta(days=1), *args),
                "accrued_c2": _accrued_at(trade_date + timedelta(days=2), *args),
                "accrued_c3": _accrued_at(trade_date + timedelta(days=3), *args),
            })

        # 4. Upsert
        if updated_rows:
            upsert_rows = [{
                "portfolio_id": portfolio_id,
                "date": date,
                **r,
            } for r in updated_rows]
            await _upsert("athena_bbg", upsert_rows, "portfolio_id,date,isin")

        return {
            "recalculated": len(updated_rows),
            "checked": len(needs_recalc),
            "skipped": skipped,
            "bonds": [{"isin": r["isin"], "par": r["par"], "accrued_c1": r["accrued_c1"]} for r in updated_rows],
        }


@app.post("/recalc/accrued")
async def recalc_accrued(portfolio_id: str = "wnbf", date: str = None, force: bool = False):
    """Fast accrued-only recalc using local_bond_reference conventions and
    orca_holdings for par. No GA10 dependency — computes 30/360 accrued directly.

    Compares result vs BBG and only updates bonds that differ.
    Pass force=true to recalc ALL bonds regardless of current match status.
    """
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")
    return await _do_recalc_accrued(portfolio_id, date, force)


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

    gw_url = os.environ.get("GA10_GATEWAY_URL", "https://ga10-gateway.urbancanary.workers.dev")

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
            "ga10_duration": t0.get("modified_duration") or t0.get("duration"),
            "ga10_duration_worst": t0.get("duration_worst"),
            "ga10_spread": t0.get("spread"),
            "ga10_convexity": t0.get("convexity"),
            "ga10_dv01": t0.get("dv01") or t0.get("pvbp"),
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




@app.get("/recon/athena-v-ga10")
async def athena_v_ga10(portfolio_id: str = "wnbf", date: str = None):
    """Compare stored GA10 v3 bond_analytics against fresh v4 recalculation.

    Athena displays stored v3 results. This endpoint recalculates each bond
    via GA10 v4 gateway with the same price to catch:
    - Convention drift (coupon, day count, frequency changes)
    - Engine version differences (v3 vs v4)
    - Stale carried-forward data
    - Description mismatches vs bond_identity

    Calls are batched concurrently (5 at a time) to stay within timeout.
    """
    if not date:
        raise HTTPException(status_code=400, detail="date parameter required")

    import os
    import httpx
    import asyncio

    ga10_url = os.environ.get("GA10_PRICING_URL", "https://ga10-pricing.urbancanary.workers.dev")
    gw_url = os.environ.get("GA10_GATEWAY_URL", "https://ga10-gateway.urbancanary.workers.dev")
    # Use v3 until v4 is deployed on the gateway, then switch via env var
    gw_version = os.environ.get("GA10_RECON_VERSION", "v3")

    async with httpx.AsyncClient(timeout=120) as client:
        # 1. Fetch stored bond_analytics + portfolio holdings + bond_identity in parallel
        from recon_db import SUPABASE_URL, _headers

        stored_task = client.get(f"{ga10_url}/prices/by-date?date={date}")
        bbg_task = client.get(
            f"{SUPABASE_URL}/rest/v1/recon_bbg",
            headers=_headers(),
            params={
                "portfolio_id": f"eq.{portfolio_id}",
                "date": f"eq.{date}",
                "select": "isin,par,price,description",
            },
        ) if portfolio_id else None
        identity_task = client.get(
            f"{SUPABASE_URL}/rest/v1/local_bond_identity",
            headers=_headers(),
            params={"select": "isin,branded_description,branded_ticker"},
        )

        tasks = [stored_task, identity_task]
        if bbg_task:
            tasks.append(bbg_task)
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        stored_resp = responses[0]
        identity_resp = responses[1]
        bbg_resp = responses[2] if bbg_task else None

        if isinstance(stored_resp, Exception) or stored_resp.status_code != 200:
            raise HTTPException(status_code=502, detail="ga10-pricing fetch failed")
        all_bonds = stored_resp.json().get("bonds", [])

        # Build identity lookup for description recon
        identity_map = {}
        if not isinstance(identity_resp, Exception) and identity_resp.status_code == 200:
            for row in identity_resp.json():
                identity_map[row["isin"]] = row

        # Build portfolio filter
        portfolio_isins = None
        if bbg_resp and not isinstance(bbg_resp, Exception) and bbg_resp.status_code == 200:
            rows = bbg_resp.json()
            if rows:
                portfolio_isins = {r["isin"]: r for r in rows}

        # Filter to portfolio bonds or CBonds watchlist bonds
        if portfolio_isins:
            bonds = [b for b in all_bonds if b["isin"] in portfolio_isins]
        else:
            bonds = [b for b in all_bonds if b.get("source") in ("scheduled_job", "carried_forward", "BBG")]

        if not bonds:
            return {
                "portfolio_id": portfolio_id,
                "date": date,
                "bonds": [],
                "count": 0,
                "summary": {"total": 0, "matched": 0, "drifted": 0, "desc_mismatches": 0},
            }

        # 2. Recalculate bonds via GA10 v4 gateway — batched concurrently
        BATCH_SIZE = 5

        async def calc_one(stored_bond):
            isin = stored_bond["isin"]
            price = stored_bond.get("price")
            if not price:
                return None
            try:
                resp = await client.post(
                    f"{gw_url}/api/{gw_version}/bond/analysis",
                    json={"isin": isin, "price": price, "settlement_date": date},
                    timeout=20,
                )
                if resp.status_code == 200:
                    return {"isin": isin, "analytics": resp.json().get("analytics", {})}
                return {"isin": isin, "error": f"GA10 {gw_version} {resp.status_code}"}
            except Exception as e:
                return {"isin": isin, "error": str(e)}

        fresh_results = {}
        for i in range(0, len(bonds), BATCH_SIZE):
            batch = bonds[i:i + BATCH_SIZE]
            batch_results = await asyncio.gather(*[calc_one(b) for b in batch])
            for r in batch_results:
                if r:
                    fresh_results[r["isin"]] = r

        # 3. Compare stored vs fresh + description recon
        results = []
        drifted = 0
        desc_mismatches = 0

        for stored in bonds:
            isin = stored["isin"]
            price = stored.get("price")
            if not price:
                continue

            s_ytm = stored.get("yield_to_maturity") or 0
            s_dur = stored.get("modified_duration") or 0
            s_spr = stored.get("spread") or 0
            s_accrued = stored.get("accrued_interest") or 0
            source = stored.get("source", "unknown")

            # Description recon: stored vs bond_identity
            stored_desc = stored.get("description")
            if not stored_desc:
                conv = stored.get("conventions") or {}
                stored_desc = conv.get("description")
            identity_desc = identity_map.get(isin, {}).get("branded_description")
            desc_match = True
            if stored_desc and identity_desc and stored_desc.strip() != identity_desc.strip():
                desc_match = False
                desc_mismatches += 1

            fresh = fresh_results.get(isin, {})
            if fresh.get("error"):
                results.append({
                    "isin": isin, "source": source, "price": price,
                    "description": {"stored": stored_desc, "identity": identity_desc, "match": desc_match},
                    "error": fresh["error"],
                })
                continue

            a = fresh.get("analytics", {})
            f_ytm = a.get("ytm") or a.get("yield_to_maturity") or 0
            f_dur = a.get("duration") or a.get("modified_duration") or 0
            f_spr = a.get("spread") or a.get("z_spread") or 0
            f_accrued = a.get("accrued_interest") or 0

            d_ytm_bps = round((s_ytm - f_ytm) * 100, 1)
            d_dur = round(s_dur - f_dur, 3)
            d_spr = round(s_spr - f_spr, 1)
            d_accrued = round(s_accrued - f_accrued, 4)

            has_drift = abs(d_ytm_bps) > 5 or abs(d_dur) > 0.1 or abs(d_spr) > 10
            if has_drift:
                drifted += 1

            results.append({
                "isin": isin,
                "description": {
                    "stored": stored_desc,
                    "identity": identity_desc,
                    "match": desc_match,
                },
                "source": source,
                "price": price,
                "stored": {
                    "ytm": round(s_ytm, 4),
                    "duration": round(s_dur, 3),
                    "spread": round(s_spr, 1),
                    "accrued": round(s_accrued, 4),
                },
                "fresh": {
                    "ytm": round(f_ytm, 4),
                    "duration": round(f_dur, 3),
                    "spread": round(f_spr, 1),
                    "accrued": round(f_accrued, 4),
                },
                "diff": {
                    "ytm_bps": d_ytm_bps,
                    "duration": d_dur,
                    "spread": d_spr,
                    "accrued": d_accrued,
                },
                "drift": has_drift,
            })

        # Sort: drifted bonds first, then desc mismatches, then by ISIN
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
                "engine": {"stored": "v3", "fresh": gw_version},
                "thresholds": {"ytm_bps": 5, "duration": 0.1, "spread_bps": 10},
            },
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
