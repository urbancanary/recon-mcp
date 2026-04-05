"""
Recon database layer — per-source Supabase tables.

recon_bbg:    Bloomberg portfolio export data
recon_admin:  Admin NAV report data (Guinness)
recon_maia:   Maia holdings export data
recon_calcs:  GA10 QuantLib calculations (from BBG prices)

Each source has its own table with only the columns it provides.
All recon queries go through here. Portfolio + date are always explicit keys.
"""

import os
import logging
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SUPABASE_URL = "https://iociqthaxysqqqamonqa.supabase.co"

# Athena Supabase anon key — RLS disabled on recon tables
_key_loaded = False
SUPABASE_KEY = ""


def _ensure_key():
    global SUPABASE_KEY, _key_loaded
    if _key_loaded:
        return
    _key_loaded = True
    SUPABASE_KEY = os.environ.get("ATHENA_SUPABASE_KEY", "")
    if not SUPABASE_KEY:
        try:
            from auth_client import get_api_key
            SUPABASE_KEY = get_api_key("ATHENA_SUPABASE_KEY")
        except Exception:
            pass
    if not SUPABASE_KEY:
        SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlvY2lxdGhheHlzcXFxYW1vbnFhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkwNDYzOTAsImV4cCI6MjA4NDYyMjM5MH0.9xD-bbEzN7Xv5npRjv5PuguQrBGZBiqTm8Cs2n8crOs"
        logger.info("Using Athena Supabase anon key (fallback)")


def _headers():
    _ensure_key()
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


# ── Bond reference lookup (bond-data Supabase project) ─────────────────────

BOND_DATA_URL = "https://xdgicslrdudsqlsudsgv.supabase.co"
BOND_DATA_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhkZ2ljc2xyZHVkc3Fsc3Vkc2d2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1NDc5NTMsImV4cCI6MjA4OTEyMzk1M30.Pn7MNCqJX_AolIoNclKd7Qu7ifCqTHdCZ-rnaeNpYZk"


async def lookup_bond_reference(isins: list[str]) -> dict[str, dict]:
    """Look up description, currency, coupon, maturity from bond_reference table."""
    if not isins:
        return {}
    headers = {
        "apikey": BOND_DATA_KEY,
        "Authorization": f"Bearer {BOND_DATA_KEY}",
    }
    isin_list = ",".join(isins)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{BOND_DATA_URL}/rest/v1/bond_reference",
                headers=headers,
                params={
                    "isin": f"in.({isin_list})",
                    "select": "isin,ticker_description,currency,coupon,maturity_date,standard_country,issuer_name",
                },
            )
            if resp.status_code == 200:
                result = {}
                for r in resp.json():
                    result[r["isin"]] = {
                        "description": r.get("ticker_description") or r.get("issuer_name") or "",
                        "currency": r.get("currency") or "USD",
                        "coupon": r.get("coupon"),
                        "maturity_date": str(r.get("maturity_date") or ""),
                        "country": r.get("standard_country") or "",
                    }
                return result
    except Exception as e:
        logger.warning(f"Bond reference lookup failed: {e}")
    return {}


# ── Storage: upload raw files + track metadata ─────────────────────────────

STORAGE_BUCKET = "recon-uploads"


def _file_hash(data: bytes) -> str:
    """SHA256 hash of raw file bytes for dedup."""
    import hashlib
    return hashlib.sha256(data).hexdigest()


async def upload_to_storage(source: str, portfolio_id: str, date: str,
                            file_bytes: bytes, filename: str) -> str:
    """Upload raw file to Supabase Storage. Returns storage path.

    Path format: {source}/{portfolio_id}/{date}{ext}
    e.g. bbg/wnbf/2026-03-31.xlsx
    """
    from pathlib import Path
    ext = Path(filename).suffix or ".xlsx"
    safe_date = date.replace("/", "-")
    path = f"{source}/{portfolio_id}/{safe_date}{ext}"

    _ensure_key()
    if not SUPABASE_KEY:
        logger.error("upload_to_storage: NO SUPABASE_KEY")
        return ""

    # Guess content type from extension
    content_type_map = {
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".csv": "text/csv",
        ".tsv": "text/tab-separated-values",
        ".txt": "text/plain",
    }
    content_type = content_type_map.get(ext.lower(), "application/octet-stream")

    async with httpx.AsyncClient(timeout=30) as client:
        # Use upsert header to overwrite if the same path already exists
        resp = await client.post(
            f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{path}",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": content_type,
                "x-upsert": "true",
            },
            content=file_bytes,
        )
        if resp.status_code not in (200, 201):
            logger.error(f"Storage upload failed: {resp.status_code} {resp.text[:500]}")
            return ""

    logger.info(f"Storage upload OK: {path} ({len(file_bytes)} bytes)")
    return path


async def track_upload(portfolio_id: str, source: str, date: str,
                       file_path: str, file_name: str, file_size: int,
                       file_hash: str, uploaded_by: str,
                       bonds_parsed: int = 0, parse_status: str = "ok",
                       parse_error: str = None) -> int:
    """Insert/upsert a row in recon_uploads."""
    row = {
        "portfolio_id": portfolio_id,
        "source": source,
        "date": date,
        "file_path": file_path,
        "file_name": file_name,
        "file_size": file_size,
        "file_hash": file_hash,
        "uploaded_by": uploaded_by,
        "bonds_parsed": bonds_parsed,
        "parse_status": parse_status,
        "parse_error": parse_error,
    }
    return await _upsert("recon_uploads", [row], "portfolio_id,source,date")


async def store_raw_upload(source: str, portfolio_id: str, date: str,
                           file_bytes: bytes, filename: str, uploaded_by: str,
                           bonds_parsed: int = 0) -> str:
    """One-call helper: upload to Storage + track in recon_uploads.

    Returns the storage path, or empty string on failure.
    """
    if not date:
        logger.warning(f"store_raw_upload: no date, skipping ({source}/{portfolio_id})")
        return ""

    path = await upload_to_storage(source, portfolio_id, date, file_bytes, filename)
    if not path:
        return ""

    await track_upload(
        portfolio_id=portfolio_id,
        source=source,
        date=date,
        file_path=path,
        file_name=filename,
        file_size=len(file_bytes),
        file_hash=_file_hash(file_bytes),
        uploaded_by=uploaded_by,
        bonds_parsed=bonds_parsed,
    )
    return path


async def list_uploads(portfolio_id: str = None, source: str = None) -> list[dict]:
    """List all uploads, optionally filtered by portfolio and/or source."""
    params = {"select": "*", "order": "date.desc,uploaded_at.desc"}
    if portfolio_id:
        params["portfolio_id"] = f"eq.{portfolio_id}"
    if source:
        params["source"] = f"eq.{source}"

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/recon_uploads",
            headers=_headers(),
            params=params,
        )
        return resp.json() if resp.status_code == 200 else []


async def download_raw_file(file_path: str) -> bytes | None:
    """Download a raw file from Storage by path."""
    _ensure_key()
    if not SUPABASE_KEY:
        return None

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{file_path}",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
        )
        if resp.status_code == 200:
            return resp.content
        logger.warning(f"Storage download failed: {resp.status_code} for {file_path}")
        return None


# ── Write: per-source store functions ──────────────────────────────────────

async def _upsert(table: str, rows: list[dict], conflict_keys: str) -> int:
    """Generic upsert to a Supabase table."""
    if not rows:
        return 0
    _ensure_key()
    if not SUPABASE_KEY:
        logger.error(f"{table}: NO SUPABASE_KEY — cannot store data")
        return 0

    logger.info(f"{table}: storing {len(rows)} rows")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict_keys}",
            headers={**_headers(), "Prefer": "return=minimal,resolution=merge-duplicates"},
            json=rows,
        )
        if resp.status_code not in (200, 201):
            logger.error(f"{table} upsert failed: {resp.status_code} {resp.text[:500]}")
            return 0

    logger.info(f"{table}: stored {len(rows)} rows OK")
    return len(rows)


async def store_bbg(portfolio_id: str, date: str, bonds: list[dict], uploaded_by: str = None) -> int:
    """Store BBG data. Upserts by (portfolio_id, date, isin)."""
    rows = [{
        "portfolio_id": portfolio_id,
        "date": date,
        "isin": b.get("isin"),
        "description": b.get("description"),
        "currency": b.get("currency", "USD"),
        "coupon": b.get("coupon"),
        "maturity_date": b.get("maturity_date") or None,
        "par": b.get("par"),
        "price": b.get("price"),
        "accrued": b.get("accrued"),
        "yield_to_worst": b.get("yield_to_worst"),
        "duration": b.get("duration"),
        "mv": b.get("mv"),
        "uploaded_by": uploaded_by,
    } for b in bonds]
    return await _upsert("recon_bbg", rows, "portfolio_id,date,isin")


async def store_admin(portfolio_id: str, date: str, bonds: list[dict], uploaded_by: str = None) -> int:
    """Store admin NAV data. Upserts by (portfolio_id, date, isin)."""
    rows = [{
        "portfolio_id": portfolio_id,
        "date": date,
        "isin": b.get("isin"),
        "description": b.get("description"),
        "currency": b.get("currency", "USD"),
        "coupon": b.get("coupon"),
        "maturity_date": b.get("maturity_date"),
        "country": b.get("country"),
        "par": b.get("par"),
        "price": b.get("price"),
        "accrued": b.get("accrued"),
        "mv": b.get("mv"),
        "uploaded_by": uploaded_by,
    } for b in bonds]
    return await _upsert("recon_admin", rows, "portfolio_id,date,isin")


async def store_maia(portfolio_id: str, date: str, bonds: list[dict], uploaded_by: str = None) -> int:
    """Store Maia holdings data. Upserts by (portfolio_id, date, isin)."""
    rows = [{
        "portfolio_id": portfolio_id,
        "date": date,
        "isin": b.get("isin"),
        "description": b.get("description"),
        "currency": b.get("currency", "USD"),
        "coupon": b.get("coupon"),
        "maturity_date": b.get("maturity_date") or None,
        "par": b.get("par"),
        "price": b.get("price"),
        "mv": b.get("mv"),
        "uploaded_by": uploaded_by,
    } for b in bonds]
    return await _upsert("recon_maia", rows, "portfolio_id,date,isin")


async def store_athena_bbg(portfolio_id: str, date: str, rows: list[dict]) -> int:
    """Store Athena's calculations from BBG prices (absolute dollars, par-scaled).

    Each row: {isin, par, source_price, accrued_t0, accrued_c1, accrued_t1, accrued_c2, accrued_c3}
    """
    upsert_rows = [{
        "portfolio_id": portfolio_id,
        "date": date,
        "isin": r.get("isin"),
        "par": r.get("par"),
        "source_price": r.get("source_price"),
        "accrued_t0": r.get("accrued_t0"),
        "accrued_c1": r.get("accrued_c1"),
        "accrued_t1": r.get("accrued_t1"),
        "accrued_c2": r.get("accrued_c2"),
        "accrued_c3": r.get("accrued_c3"),
    } for r in rows]
    return await _upsert("athena_bbg", upsert_rows, "portfolio_id,date,isin")


async def store_calcs(portfolio_id: str, date: str, calcs: list[dict]) -> int:
    """Store GA10 calculation results. Upserts by (portfolio_id, date, isin)."""
    rows = [{
        "portfolio_id": portfolio_id,
        "date": date,
        "isin": c.get("isin"),
        "source_price": c.get("source_price"),
        "ga10_accrued": c.get("ga10_accrued"),
        "ga10_accrued_c1": c.get("ga10_accrued_c1"),
        "ga10_accrued_t1": c.get("ga10_accrued_t1"),
        "ga10_accrued_t2": c.get("ga10_accrued_t2"),
        "ga10_accrued_t3": c.get("ga10_accrued_t3"),
        "ga10_yield": c.get("ga10_yield"),
        "ga10_yield_c1": c.get("ga10_yield_c1"),
        "ga10_yield_t1": c.get("ga10_yield_t1"),
        "ga10_yield_worst": c.get("ga10_yield_worst"),
        "ga10_duration": c.get("ga10_duration"),
        "ga10_duration_worst": c.get("ga10_duration_worst"),
        "ga10_spread": c.get("ga10_spread"),
        "ga10_convexity": c.get("ga10_convexity"),
        "ga10_dv01": c.get("ga10_dv01"),
    } for c in calcs]
    return await _upsert("recon_calcs", rows, "portfolio_id,date,isin")


# ── Read: fetch recon data (joins all sources) ────────────────────────────

_athena_price_cache: dict[tuple, tuple[float, dict]] = {}
_ATHENA_PRICE_TTL = 300  # 5 minutes


async def _fetch_independent_athena_prices(isins: list[str], recon_date: str) -> dict[str, dict]:
    """Get the most recent non-BBG price for each ISIN on or before recon_date.

    Fires all date queries in parallel (7-day window) to ga10-pricing, then picks
    the most recent non-BBG price per ISIN. Cached in memory for 5 minutes.

    Returns: {isin: {price, price_date, source}}
    """
    import os
    import time
    import asyncio

    cache_key = (recon_date, tuple(sorted(isins)))
    cached = _athena_price_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _ATHENA_PRICE_TTL:
        return cached[1]

    ga10_url = os.environ.get("GA10_PRICING_URL", "https://ga10-pricing.urbancanary.workers.dev")

    try:
        start = datetime.strptime(recon_date, "%Y-%m-%d")
    except ValueError:
        return {}

    target_isins = set(isins)
    lookback_days = 7  # 7 days is usually enough to catch any non-weekend/holiday gap

    # Fire all date queries in parallel
    async with httpx.AsyncClient(timeout=15) as client:
        tasks = []
        date_strs = []
        for offset in range(lookback_days):
            d = (start - timedelta(days=offset)).strftime("%Y-%m-%d")
            date_strs.append(d)
            tasks.append(client.get(f"{ga10_url}/prices/by-date?date={d}"))
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Walk responses in order (offset=0 is recon_date, then 1 day back, etc.)
    # Keep the first valid non-BBG price per ISIN — that's the most recent one.
    found: dict[str, dict] = {}
    for offset, resp in enumerate(responses):
        if not (target_isins - set(found.keys())):
            break
        if isinstance(resp, Exception) or resp.status_code != 200:
            continue
        try:
            bonds = resp.json().get("bonds", [])
        except Exception:
            continue
        for b in bonds:
            isin = b.get("isin")
            if isin not in target_isins or isin in found:
                continue
            src = b.get("source")
            price = b.get("price") or b.get("clean_price")
            if src == "BBG" or price is None:
                continue
            found[isin] = {
                "price": float(price),
                "price_date": b.get("price_date") or date_strs[offset],
                "source": src,
            }

    _athena_price_cache[cache_key] = (time.time(), found)
    return found


async def get_recon_data(portfolio_id: str, date: str) -> dict:
    """Fetch recon data for a portfolio/date from the recon_view.

    1. Query recon_view (joins all 4 source tables, computes derived fields)
    2. Fetch independent Athena prices from ga10-pricing (date-locked ≤ recon_date)
    3. Override athena_price with the most recent non-BBG price on or before recon_date
    4. Return display-ready rows
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/recon_view",
            headers=_headers(),
            params={
                "portfolio_id": f"eq.{portfolio_id}",
                "date": f"eq.{date}",
                "select": "*",
                "order": "isin.asc",
            },
        )
        rows = resp.json() if resp.status_code == 200 else []

    # Date-locked independent Athena prices from ga10-pricing
    if rows:
        isins = [r["isin"] for r in rows if r.get("isin")]
        try:
            athena_prices = await _fetch_independent_athena_prices(isins, date)
            for row in rows:
                ap = athena_prices.get(row["isin"])
                if ap:
                    row["athena_price"] = ap["price"]
                    row["athena_price_source"] = ap["source"]
                    row["athena_price_date"] = ap["price_date"]
                    row["athena_price_stale_days"] = (
                        (datetime.strptime(date, "%Y-%m-%d") - datetime.strptime(ap["price_date"], "%Y-%m-%d")).days
                        if ap.get("price_date") else None
                    )
                    # Recompute athena_mv with the independent price
                    if row.get("athena_par") is not None:
                        row["athena_mv"] = (
                            float(row["athena_par"]) * ap["price"] / 100
                            + float(row.get("athena_accrued_c1") or 0)
                        )
                else:
                    # No independent price available — clear athena_price rather than leave BBG value
                    row["athena_price"] = None
                    row["athena_price_source"] = None
                    row["athena_mv"] = None
        except Exception as e:
            logger.warning(f"Independent Athena price lookup failed: {e}")

    # Use computed BBG MV (from par × price) if raw BBG MV is missing or in different units
    for row in rows:
        if row.get("bbg_mv_computed") is not None:
            row["bbg_mv"] = row["bbg_mv_computed"]

    # Determine which sources are present
    sources_available = {
        "bbg": any(r.get("bbg_par") is not None or r.get("bbg_price") is not None for r in rows),
        "admin": any(r.get("admin_par") is not None or r.get("admin_price") is not None for r in rows),
        "maia": any(r.get("maia_par") is not None or r.get("maia_price") is not None for r in rows),
        "ga10": any(r.get("athena_duration") is not None or r.get("athena_accrued") is not None for r in rows),
    }

    # Compute totals
    def _sum(field):
        return sum(r[field] for r in rows if r.get(field) is not None) or 0

    totals = {
        "bbg_accrued": _sum("bbg_accrued"),
        "admin_accrued": _sum("admin_accrued"),
        "athena_accrued": _sum("athena_accrued_c1"),
        "bbg_mv": _sum("bbg_mv"),
        "admin_mv": _sum("admin_mv"),
        "maia_mv": _sum("maia_mv"),
        "athena_mv": _sum("athena_mv"),
        "bbg_par": _sum("bbg_par"),
        "admin_par": _sum("admin_par"),
        "athena_par": _sum("athena_par"),
    }

    return {
        "portfolio_id": portfolio_id,
        "date": date,
        "bonds": rows,
        "totals": totals,
        "sources_available": sources_available,
    }


async def get_recon_status(portfolio_id: str = None) -> list[dict]:
    """Get date×source coverage matrix from all per-source tables."""
    params_base = {}
    if portfolio_id:
        params_base["portfolio_id"] = f"eq.{portfolio_id}"

    async with httpx.AsyncClient(timeout=15) as client:
        import asyncio
        sel = "portfolio_id,date,isin"
        bbg_req = client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(),
                             params={**params_base, "select": sel})
        admin_req = client.get(f"{SUPABASE_URL}/rest/v1/recon_admin", headers=_headers(),
                               params={**params_base, "select": sel})
        maia_req = client.get(f"{SUPABASE_URL}/rest/v1/recon_maia", headers=_headers(),
                              params={**params_base, "select": sel})
        calcs_req = client.get(f"{SUPABASE_URL}/rest/v1/recon_calcs", headers=_headers(),
                               params={**params_base, "select": sel})

        bbg_resp, admin_resp, maia_resp, calcs_resp = await asyncio.gather(
            bbg_req, admin_req, maia_req, calcs_req
        )

    from collections import defaultdict
    date_sources = defaultdict(lambda: defaultdict(int))

    for r in (bbg_resp.json() if bbg_resp.status_code == 200 else []):
        date_sources[r["date"]]["bbg"] += 1
    for r in (admin_resp.json() if admin_resp.status_code == 200 else []):
        date_sources[r["date"]]["admin"] += 1
    for r in (maia_resp.json() if maia_resp.status_code == 200 else []):
        date_sources[r["date"]]["maia"] += 1
    for r in (calcs_resp.json() if calcs_resp.status_code == 200 else []):
        date_sources[r["date"]]["ga10"] += 1

    all_dates = sorted(date_sources.keys(), reverse=True)
    rows = []
    for d in all_dates:
        row = {"date": d}
        for src, count in date_sources[d].items():
            row[src] = f"{count} bonds"
        rows.append(row)

    return rows


# ── Backfill: populate missing coupon/maturity from bond_reference ─────────

async def backfill_coupon_maturity(table: str = "recon_bbg") -> dict:
    """Self-healing backfill: find rows in `table` with null coupon, look them up
    in bond_reference, and update in place.

    Safe to run every startup — idempotent.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_headers(),
            params={"coupon": "is.null", "select": "portfolio_id,date,isin", "limit": "1000"},
        )
        if resp.status_code != 200:
            return {"updated": 0, "error": f"fetch failed: {resp.status_code}"}
        missing = resp.json()

    if not missing:
        return {"updated": 0}

    isins = list({r["isin"] for r in missing})
    ref_by_isin = await lookup_bond_reference(isins)
    if not ref_by_isin:
        return {"updated": 0, "reason": "bond_reference returned nothing"}

    updated = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for r in missing:
            ref = ref_by_isin.get(r["isin"])
            if not ref:
                continue
            coupon = ref.get("coupon")
            maturity = ref.get("maturity_date") or None
            if coupon is None and not maturity:
                continue
            try:
                patch_resp = await client.patch(
                    f"{SUPABASE_URL}/rest/v1/{table}",
                    headers={**_headers(), "Prefer": "return=minimal"},
                    params={
                        "portfolio_id": f"eq.{r['portfolio_id']}",
                        "date": f"eq.{r['date']}",
                        "isin": f"eq.{r['isin']}",
                    },
                    json={"coupon": coupon, "maturity_date": maturity},
                )
                if patch_resp.status_code in (200, 204):
                    updated += 1
            except Exception as e:
                logger.warning(f"backfill patch failed for {r['isin']}: {e}")

    logger.info(f"{table}: backfilled coupon/maturity on {updated} rows")
    return {"updated": updated, "checked": len(missing)}
