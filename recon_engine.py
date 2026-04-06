"""
recon_engine.py — Orchestration for recon-mcp.

Handles upload processing, portfolio detection, parsing, enrichment,
GA10 calculation triggers, and Supabase storage.

This is the business logic layer that sits between the HTTP endpoints
(app.py) and the data layer (recon_db.py).
"""

import logging
import os
import re
import asyncio
import httpx

from recon_db import (
    store_bbg, store_admin, store_maia, store_calcs, store_athena_bbg,
    store_raw_upload, lookup_bond_reference, SUPABASE_URL, _headers,
)
from alerts import (
    alert_ga10_partial_failure, alert_upload_failed,
    alert_upload_success, alert_data_quality,
)

logger = logging.getLogger(__name__)

GA10_PRICING_URL = os.environ.get("GA10_PRICING_URL", "https://ga10-pricing.urbancanary.workers.dev")

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
MAIA_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


# ── Portfolio detection ─────────────────────────────────────────────────────

async def _latest_gcrif_isins() -> set[str]:
    """Get ISINs from the most recent GCRIF recon_admin row. Used for portfolio detection."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/recon_admin",
                headers=_headers(),
                params={
                    "portfolio_id": "eq.gcrif",
                    "select": "isin,date",
                    "order": "date.desc",
                    "limit": "500",
                },
            )
            if resp.status_code == 200:
                rows = resp.json()
                if rows:
                    latest_date = rows[0]["date"]
                    return {r["isin"] for r in rows if r["date"] == latest_date}
    except Exception as e:
        logger.warning(f"Could not fetch latest GCRIF ISINs: {e}")
    return set()


def _bbg_portfolio_isins(bbg: dict) -> set[str]:
    """Extract all ISINs from a parsed BBG file."""
    isins = set()
    for key in ("price_bonds", "bonds", "position_bonds", "mv_bonds"):
        isins.update((bbg.get(key) or {}).keys())
    return isins


async def bbg_is_gcrif(bbg: dict) -> bool:
    """Check if a BBG file belongs to GCRIF by matching ISINs against admin holdings."""
    bbg_isins = _bbg_portfolio_isins(bbg)
    if not bbg_isins:
        return False

    gcrif_isins = await _latest_gcrif_isins()
    if not gcrif_isins:
        # Fallback: HK-prefix heuristic
        return any(i.startswith("HK") for i in bbg_isins)

    overlap = bbg_isins & gcrif_isins
    return len(overlap) / len(bbg_isins) > 0.3


async def detect_bbg_portfolio(bbg_result: dict, portfolio_override: str | None = None) -> str:
    """Detect portfolio_id from BBG ISINs. Defaults to 'wnbf' if no override."""
    if portfolio_override:
        return portfolio_override
    if await bbg_is_gcrif(bbg_result):
        return "gcrif"
    return "wnbf"


async def maia_is_gcrif(tsv: str) -> bool:
    """Check if Maia TSV data belongs to GCRIF."""
    maia_isins = set()
    for line in tsv.strip().split("\n")[:100]:
        for col in line.split("\t"):
            if ISIN_RE.match(col.strip()):
                maia_isins.add(col.strip())
    if not maia_isins:
        return False

    gcrif_isins = await _latest_gcrif_isins()
    if not gcrif_isins:
        return any(i.startswith("HK") for i in maia_isins)

    overlap = maia_isins & gcrif_isins
    return len(overlap) / len(maia_isins) > 0.3


# ── Maia TSV parsing ────────────────────────────────────────────────────────

MAIA_HEADER_MAP = {
    "isin": "isin", "ticker": "ticker", "description": "description",
    "grouping": "grouping", "last px": "price", "position": "par",
    "holding": "par", "qty": "par", "exp fund ccy": "mv",
    "exp (fund ccy)": "mv", "nav contri fund ccy": "mv",
    "exposure ($ usd)": "mv", "currency": "currency", "date": "date",
}

MAIA_LEGACY_MAP = {
    "isin": 14, "ticker": 2, "description": 1, "currency": 11,
    "par": 4, "mv": 5, "price": 12, "grouping": 0,
}


def extract_maia_date(tsv: str, filename: str = "") -> str | None:
    """Extract date from Maia TSV or filename.

    Tries:
    1. DD/MM/YYYY in any cell of the TSV
    2. Date embedded in filename (e.g. MAIA227032026.xlsx → 27/03/2026)
    """
    # Try TSV cells first
    for line in tsv.strip().split("\n")[:50]:
        for col in line.split("\t"):
            m = MAIA_DATE_RE.match(col.strip())
            if m:
                return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # Fallback: extract from filename
    # Patterns: MAIA227032026.xlsx (MAIA2 + DDMMYYYY), MAIA_27-03-2026.xlsx, maia_2026-03-27.xlsx
    if filename:
        import re
        # MAIA2DDMMYYYY
        m = re.search(r'MAIA2?(\d{2})(\d{2})(\d{4})', filename, re.IGNORECASE)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # YYYY-MM-DD in filename
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # DD-MM-YYYY in filename
        m = re.search(r'(\d{2})-(\d{2})-(\d{4})', filename)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return None


def parse_maia_tsv(tsv: str) -> list[dict]:
    """Parse Maia TSV into bond dicts. Auto-detects columns from header row."""
    col_map = None
    bonds = []

    for line in tsv.strip().split("\n"):
        cols = [c.strip() for c in line.split("\t")]
        if len(cols) < 6:
            continue

        if col_map is None:
            detected = {}
            for i, c in enumerate(cols):
                field = MAIA_HEADER_MAP.get(c.lower().strip())
                if field and field not in detected:
                    detected[field] = i
            if "isin" in detected:
                col_map = detected
                continue
            if len(cols) >= 15:
                col_map = MAIA_LEGACY_MAP
            else:
                continue

        def _col(field):
            idx = col_map.get(field)
            return cols[idx].strip() if idx is not None and idx < len(cols) else ""

        def _num(field):
            try:
                return float(_col(field).replace(",", ""))
            except (ValueError, TypeError):
                return None

        isin = _col("isin")
        if not ISIN_RE.match(isin):
            continue

        grouping = _col("grouping").lower()
        ticker = _col("ticker").lower()
        if grouping == "cash" or ticker == "cash" or ticker.startswith("cash_"):
            continue

        par = _num("par")
        mv = _num("mv")
        if par is None and mv is None:
            continue

        bonds.append({
            "isin": isin,
            "description": _col("description"),
            "currency": _col("currency") or None,
            "par": par,
            "price": _num("price"),
            "mv": mv,
        })

    return bonds


# ── GA10 orchestration ──────────────────────────────────────────────────────

async def recalc_with_bbg_prices(bbg_prices: dict, price_date: str,
                                  portfolio_id: str, bbg_par: dict = None) -> int:
    """Store BBG prices in GA10, trigger recalc, fetch results, store in recon_calcs.

    Returns the number of calcs stored.
    """
    isins = list(bbg_prices.keys())
    if not isins:
        return 0

    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            # 1. Store BBG prices in GA10
            prices_payload = [
                {"isin": isin, "source": "BBG", "price_date": price_date, "clean_price": px}
                for isin, px in bbg_prices.items()
            ]
            resp = await client.post(
                f"{GA10_PRICING_URL}/prices/store",
                json={"prices": prices_payload},
            )
            logger.info("BBG prices stored: %s", resp.status_code)

            # 2. Trigger calculation
            resp = await client.post(
                f"{GA10_PRICING_URL}/prices/calculate",
                json={"isins": isins, "price_date": price_date, "source": "BBG"},
            )
            calc_result = resp.json() if resp.status_code == 200 else {}
            calculated = calc_result.get("calculated", 0)

            # 3. Poll for async completion if needed
            if calculated == 0:
                for attempt in range(6):
                    await asyncio.sleep(10)
                    resp = await client.get(
                        f"{GA10_PRICING_URL}/prices/by-date?date={price_date}&source=BBG"
                    )
                    if resp.status_code == 200:
                        ready = len([b for b in resp.json().get("bonds", [])
                                    if b.get("isin") in set(isins)])
                        logger.info("BBG recalc poll %d: %d/%d", attempt + 1, ready, len(isins))
                        if ready >= len(isins) * 0.8:
                            calculated = ready
                            break

            # 4. Fetch calculated analytics and store in Supabase
            if calculated > 0:
                resp = await client.get(
                    f"{GA10_PRICING_URL}/prices/by-date?date={price_date}&source=BBG"
                )
                if resp.status_code == 200:
                    ga10_bonds = resp.json().get("bonds", [])
                    calcs = []
                    athena_bbg_rows = []
                    par_lookup = bbg_par or {}
                    for b in ga10_bonds:
                        isin = b.get("isin")
                        if isin not in bbg_prices:
                            continue
                        par = par_lookup.get(isin) or 0

                        # recon_calcs — store raw per-100 values for reference
                        calcs.append({
                            "isin": isin,
                            "source_price": bbg_prices.get(isin),
                            "ga10_accrued": b.get("accrued_interest"),
                            "ga10_accrued_c1": b.get("accrued_interest_c1"),
                            "ga10_accrued_t1": b.get("accrued_interest_t1"),
                            "ga10_accrued_t2": b.get("accrued_interest_t2"),
                            "ga10_accrued_t3": b.get("accrued_interest_t3"),
                            "ga10_yield": b.get("yield_to_maturity"),
                            "ga10_yield_c1": b.get("ytm_c1"),
                            "ga10_yield_t1": b.get("ytm_t1"),
                            "ga10_yield_worst": b.get("ytw_bbg") or b.get("yield_to_maturity"),
                            "ga10_duration": b.get("modified_duration"),
                            "ga10_duration_worst": b.get("duration_worst"),
                            "ga10_spread": b.get("spread"),
                            "ga10_convexity": b.get("convexity"),
                            "ga10_dv01": b.get("dv01"),
                        })

                        # athena_bbg — par-scaled absolute dollars (the display layer)
                        if par:
                            mult = par / 100
                            def _scale(v):
                                return v * mult if v is not None else None
                            athena_bbg_rows.append({
                                "isin": isin,
                                "par": par,
                                "source_price": bbg_prices.get(isin),
                                "accrued_t0": _scale(b.get("accrued_interest")),
                                "accrued_c1": _scale(b.get("accrued_interest_c1")),
                                "accrued_t1": _scale(b.get("accrued_interest_t1")),
                                "accrued_c2": _scale(b.get("accrued_interest_t2")),
                                "accrued_c3": _scale(b.get("accrued_interest_t3")),
                            })

                    if calcs:
                        await store_calcs(portfolio_id, price_date, calcs)
                    if athena_bbg_rows:
                        await store_athena_bbg(portfolio_id, price_date, athena_bbg_rows)
                    return len(calcs)

    except Exception as e:
        logger.error("BBG price recalc failed: %r", e)

    return 0


# ── High-level upload handlers ──────────────────────────────────────────────

async def process_bbg_upload(file_bytes: bytes, filename: str,
                              uploaded_by: str, portfolio_override: str = None) -> dict:
    """Parse BBG file, enrich, store, trigger GA10 recalc.

    Returns a summary dict with portfolio_id, date, bond count, and status.
    """
    from bbg_parser import parse_bbg_export

    try:
        bbg_result = parse_bbg_export(file_bytes)
    except Exception as e:
        return {"status": "error", "error": f"Parse failed: {e}"}

    pid = await detect_bbg_portfolio(bbg_result, portfolio_override)
    bbg_result["portfolio_id"] = pid

    # Normalise date
    raw_date = bbg_result.get("as_of_date")
    if raw_date:
        from datetime import datetime as _dt
        for dfmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
            try:
                bbg_result["as_of_date_iso"] = _dt.strptime(raw_date, dfmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    bbg_date = bbg_result.get("as_of_date_iso") or bbg_result.get("settle_date")
    if not bbg_date:
        return {"status": "error", "error": "could not determine as-of date from file"}

    # Build bond rows + enrich with bond_reference
    price_bonds = bbg_result.get("price_bonds", {})
    accrued_bonds = bbg_result.get("bonds", {})
    yield_bonds = bbg_result.get("ytw_bonds", bbg_result.get("yield_bonds", {}))
    oad_bonds = bbg_result.get("oad_bonds", {})
    mv_bonds = bbg_result.get("mv_bonds", {})
    position_bonds = bbg_result.get("position_bonds", {})
    all_isins = list(set(list(price_bonds.keys()) + list(accrued_bonds.keys())))

    ref_by_isin = await lookup_bond_reference(all_isins)

    bbg_bonds = []
    for isin in all_isins:
        ref = ref_by_isin.get(isin, {})
        bbg_bonds.append({
            "isin": isin,
            "description": ref.get("description", ""),
            "currency": ref.get("currency", "USD"),
            "coupon": ref.get("coupon"),
            "maturity_date": ref.get("maturity_date") or None,
            "price": price_bonds.get(isin),
            "accrued": accrued_bonds.get(isin),
            "yield_to_worst": yield_bonds.get(isin),
            "duration": oad_bonds.get(isin),
            "mv": mv_bonds.get(isin),
            "par": position_bonds.get(isin),
        })

    # Store parsed data + raw file in parallel
    await asyncio.gather(
        store_bbg(pid, bbg_date, bbg_bonds, uploaded_by),
        store_raw_upload(
            source="bbg", portfolio_id=pid, date=bbg_date,
            file_bytes=file_bytes, filename=filename,
            uploaded_by=uploaded_by, bonds_parsed=len(bbg_bonds),
        ),
    )

    # Trigger GA10 recalc (fire-and-forget — can take minutes)
    asyncio.create_task(recalc_with_bbg_prices(price_bonds, bbg_date, pid, position_bonds))

    asyncio.create_task(alert_upload_success("bbg", pid, bbg_date, len(bbg_bonds), filename))

    return {
        "status": "ok",
        "portfolio_id": pid,
        "date": bbg_date,
        "bonds_parsed": len(bbg_bonds),
    }


async def process_admin_upload(file_bytes: bytes, filename: str, uploaded_by: str) -> dict:
    """Parse admin NAV file, store in recon_admin + recon_uploads."""
    from nav_parser import parse_nav_report

    try:
        result = parse_nav_report(file_bytes)
    except Exception as e:
        return {"status": "error", "error": f"Parse failed: {e}"}

    admin_date = result.get("valuation_date")
    if not admin_date:
        return {"status": "error", "error": "no valuation date in file"}

    admin_pid = result.get("portfolio_id", "gcrif")
    admin_bonds = []
    for h in result.get("holdings", []):
        if h.get("isin"):
            admin_bonds.append({
                "isin": h["isin"],
                "description": h.get("description") or h.get("ticker"),
                "currency": h.get("currency", "USD"),
                "coupon": h.get("coupon"),
                "maturity_date": h.get("maturity_date"),
                "country": h.get("country"),
                "par": h.get("face_value") or h.get("par_amount"),
                "price": h.get("price") or h.get("clean_price"),
                "accrued": h.get("accrued_interest") or h.get("accrued_income"),
                "mv": h.get("market_value"),
            })

    await asyncio.gather(
        store_admin(admin_pid, admin_date, admin_bonds, uploaded_by),
        store_raw_upload(
            source="admin", portfolio_id=admin_pid, date=admin_date,
            file_bytes=file_bytes, filename=filename,
            uploaded_by=uploaded_by, bonds_parsed=len(admin_bonds),
        ),
    )

    asyncio.create_task(alert_upload_success("admin", admin_pid, admin_date, len(admin_bonds), filename))

    return {
        "status": "ok",
        "portfolio_id": admin_pid,
        "date": admin_date,
        "bonds_parsed": len(admin_bonds),
        "total_nav": result.get("summary", {}).get("total_nav"),
    }


async def process_maia_upload(file_bytes: bytes, filename: str, uploaded_by: str) -> dict:
    """Parse Maia file (Excel/TSV), convert to TSV, store in recon_maia + recon_uploads."""
    import pandas as pd
    import io

    try:
        if filename.lower().endswith((".csv", ".tsv", ".txt")):
            text = file_bytes.decode("utf-8", errors="replace")
            sep = "\t" if "\t" in text else ","
            df = pd.read_csv(io.StringIO(text), sep=sep, header=None, dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(file_bytes), header=None, dtype=str)
    except Exception as e:
        logger.error(f"Maia file parse failed ({filename}, {len(file_bytes)} bytes): {e}")
        return {"status": "error", "error": f"File parse failed: {e}"}

    tsv = df.to_csv(sep="\t", index=False, header=False)
    logger.info(f"Maia TSV: {len(tsv)} chars, {len(tsv.strip().splitlines())} lines from {filename}")

    maia_date = extract_maia_date(tsv, filename=filename)
    if not maia_date:
        logger.error(f"Maia date not found. Filename: {filename}. First 3 lines: {tsv.strip().splitlines()[:3]}")
        return {"status": "error", "error": "no date found in Maia file"}

    maia_pid = "gcrif" if await maia_is_gcrif(tsv) else "wnbf"
    maia_bonds = parse_maia_tsv(tsv)
    logger.info(f"Maia parsed: {len(maia_bonds)} bonds, date={maia_date}, pid={maia_pid}")

    if not maia_bonds:
        logger.error(f"Maia no valid bonds. First 5 lines: {tsv.strip().splitlines()[:5]}")
        return {"status": "error", "error": "no valid bond rows parsed"}

    # Enrich with bond_reference
    ref_by_isin = await lookup_bond_reference([b["isin"] for b in maia_bonds])
    for b in maia_bonds:
        ref = ref_by_isin.get(b["isin"], {})
        if not b.get("description"):
            b["description"] = ref.get("description", "")
        if not b.get("currency"):
            b["currency"] = ref.get("currency", "USD")
        b["coupon"] = ref.get("coupon")
        b["maturity_date"] = ref.get("maturity_date") or None

    await asyncio.gather(
        store_maia(maia_pid, maia_date, maia_bonds, uploaded_by),
        store_raw_upload(
            source="maia", portfolio_id=maia_pid, date=maia_date,
            file_bytes=file_bytes, filename=filename,
            uploaded_by=uploaded_by, bonds_parsed=len(maia_bonds),
        ),
    )

    asyncio.create_task(alert_upload_success("maia", maia_pid, maia_date, len(maia_bonds), filename))

    return {
        "status": "ok",
        "portfolio_id": maia_pid,
        "date": maia_date,
        "bonds_parsed": len(maia_bonds),
    }
