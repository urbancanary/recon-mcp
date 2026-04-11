"""
recon_engine.py — Orchestration for recon-mcp.

Handles upload processing, portfolio detection, parsing, enrichment,
GA10 calculation triggers, and Supabase storage.

This is the business logic layer that sits between the HTTP endpoints
(app.py) and the data layer (recon_db.py).
"""

import hashlib
import logging
import os
import re
import asyncio
import httpx

from recon_db import (
    store_bbg, store_admin, store_maia, store_calcs, store_athena_bbg,
    store_raw_upload, lookup_bond_reference, sync_bond_data, sync_orca_holdings,
    enrich_bond_data_from_bbg,
    SUPABASE_URL, _headers, BOND_DATA_URL, _bond_data_headers,
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
    # Patterns: MAIA227032026.xlsx, maia_views310326.xlsx, MAIA_27-03-2026.xlsx, maia_2026-03-27.xlsx
    if filename:
        import re
        # MAIA2DDMMYYYY (8-digit year)
        m = re.search(r'MAIA2?(\d{2})(\d{2})(\d{4})', filename, re.IGNORECASE)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # 6-digit DDMMYY anywhere in filename (e.g. maia_views310326.xlsx)
        m = re.search(r'(\d{2})(\d{2})(\d{2})(?=\.\w+$)', filename)
        if m:
            year = int(m.group(3))
            year_full = 2000 + year if year < 100 else year
            return f"{year_full}-{m.group(2)}-{m.group(1)}"
        # YYYY-MM-DD in filename
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # DD-MM-YYYY in filename
        m = re.search(r'(\d{2})-(\d{2})-(\d{4})', filename)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return None


def parse_maia_tsv(tsv: str) -> tuple[list[dict], dict]:
    """Parse Maia TSV into bond dicts. Auto-detects columns from header row.

    Returns: (bonds, metadata) where metadata includes fx_cny_usd if found.
    """
    col_map = None
    bonds = []
    meta = {}

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

        # Extract FX rate from cash rows: "CNY Cash" / "CNY Curncy" has Last Px = CNY/USD rate
        ticker = _col("ticker").lower()
        description = _col("description").lower()
        if ("cny" in ticker or "cny" in description) and ("cash" in ticker or "cash" in description or "curncy" in ticker):
            price = _num("price")
            if price and 0.1 < price < 0.2:  # sanity check: CNY/USD is ~0.14
                meta["fx_cny_usd"] = price
                meta["fx_cnh_per_usd"] = round(1.0 / price, 6)
                logger.info(f"Maia FX extracted: CNY/USD={price}, CNH/USD={meta['fx_cnh_per_usd']}")
            continue

        if not ISIN_RE.match(isin):
            continue

        grouping = _col("grouping").lower()
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

    return bonds, meta


# ── Admin prices → bond-data ────────────────────────────────────────────────

async def _store_admin_prices_to_bond_data(admin_bonds: list[dict], price_date: str):
    """Write admin prices to bond_analytics_dated in bond-data Supabase.

    This gives Athena a price history even when CBonds/ETF scraper hasn't run.
    Source = 'admin'. Upserts by (isin, price_date, source).
    """
    from recon_db import BOND_DATA_URL, _bond_data_headers
    rows = []
    for b in admin_bonds:
        price = b.get("price")
        if not b.get("isin") or price is None:
            continue
        rows.append({
            "isin": b["isin"],
            "price_date": price_date,
            "source": "admin",
            "price": float(price),
            "currency": b.get("currency", "USD"),
            "description": b.get("description"),
            "coupon": float(b["coupon"]) if b.get("coupon") is not None else None,
            "maturity_date": b.get("maturity_date") or None,
        })

    if not rows:
        return

    try:
        headers = {**_bond_data_headers(), "Prefer": "return=minimal,resolution=merge-duplicates"}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{BOND_DATA_URL}/rest/v1/bond_analytics_dated?on_conflict=isin,price_date,source",
                headers=headers,
                json=rows,
            )
            if resp.status_code in (200, 201):
                logger.info(f"Admin prices → bond_analytics_dated: {len(rows)} rows stored for {price_date}")
            else:
                logger.warning(f"Admin prices → bond_analytics_dated failed: {resp.status_code} {resp.text[:300]}")
    except Exception as e:
        logger.warning(f"Admin prices → bond_analytics_dated error: {e}")


# ── GA10 orchestration (accrued removed — GA10 is the single engine) ─────

# The direct accrued calculator was removed (commit eba3819) because it
# guessed coupon schedules wrong. GA10/CBonds owns the bond schedule.
# If GA10 has gaps, fix bond_reference and retrigger recalc_all_existing.


async def _removed_compute_accrued_for_all() -> dict:
    """Compute accrued interest directly from bond_reference for every
    (portfolio, date, isin) tuple in recon_bbg + recon_admin.

    No GA10 call needed. Uses coupon, maturity, day_count, frequency
    from local_bond_reference. Stores results in athena_bbg / athena_admin.

    Called after sync_bond_data to pick up convention changes.
    """
    from accrued_calc import compute_accrued_multi
    from recon_db import SUPABASE_URL, _headers, store_athena_bbg, _upsert

    # Fetch bond reference data (coupon, maturity, day_count)
    async with httpx.AsyncClient(timeout=15) as client:
        ref_resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/local_bond_reference",
            headers=_headers(),
            params={"select": "isin,coupon,maturity_date,day_count"},
        )
        refs = {r["isin"]: r for r in (ref_resp.json() if ref_resp.status_code == 200 else [])}

        # Fetch all BBG rows (portfolio, date, isin, par)
        bbg_resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/recon_bbg",
            headers=_headers(),
            params={"select": "portfolio_id,date,isin,par", "limit": "5000"},
        )
        bbg_rows = bbg_resp.json() if bbg_resp.status_code == 200 else []

        # Fetch all admin rows
        admin_resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/recon_admin",
            headers=_headers(),
            params={"select": "portfolio_id,date,isin,par", "limit": "5000"},
        )
        admin_rows = admin_resp.json() if admin_resp.status_code == 200 else []

    from datetime import date as _date

    def _compute_for_rows(rows: list[dict]) -> list[dict]:
        results = []
        for r in rows:
            ref = refs.get(r["isin"])
            par = float(r["par"]) if r.get("par") else None
            if not ref or ref.get("coupon") is None or not ref.get("maturity_date") or not par:
                continue

            try:
                maturity = _date.fromisoformat(str(ref["maturity_date"]))
                val_date = _date.fromisoformat(str(r["date"]))

                # Infer frequency from accrual pattern
                # Default semi-annual; if day_count suggests annual or bond is CNY, try annual
                freq = "Semiannual"
                # Simple heuristic: if maturity month matches issue pattern for annual bonds
                # This is imperfect — should store frequency in bond_reference
                # For now, check if day_count is ACT/365 (common for annual CNY bonds)
                dc = ref.get("day_count") or "30/360"
                if dc.upper() in ("ACT/365", "ACTUAL/365"):
                    freq = "Annual"

                multi = compute_accrued_multi(
                    coupon=float(ref["coupon"]),
                    maturity_date=maturity,
                    par=par,
                    valuation_date=val_date,
                    frequency=freq,
                    day_count=dc,
                )

                results.append({
                    "portfolio_id": r["portfolio_id"],
                    "date": str(r["date"]),
                    "isin": r["isin"],
                    "par": par,
                    "source_price": None,
                    "accrued_t0": multi["t0"],
                    "accrued_c1": multi["c1"],
                    "accrued_t1": multi["t1"],
                    "accrued_c2": multi["c2"],
                    "accrued_c3": multi["c3"],
                })
            except Exception as e:
                logger.warning(f"Accrued calc failed for {r['isin']} on {r['date']}: {e}")
                continue

        return results

    # Only fill gaps — don't overwrite existing GA10 values in athena_bbg
    # Fetch existing athena_bbg ISINs to skip
    existing_bbg = set()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/athena_bbg",
            headers=_headers(),
            params={"select": "portfolio_id,date,isin", "accrued_c1": "not.is.null", "limit": "5000"},
        )
        if resp.status_code == 200:
            for r in resp.json():
                existing_bbg.add((r["portfolio_id"], str(r["date"]), r["isin"]))

    bbg_results = _compute_for_rows(bbg_rows)
    # Filter to only gaps (where GA10 didn't produce a value)
    bbg_gaps = [r for r in bbg_results
                if (r["portfolio_id"], r["date"], r["isin"]) not in existing_bbg]
    logger.info(f"Accrued calc: {len(bbg_results)} computed, {len(bbg_gaps)} are gaps (GA10 had no value)")

    bbg_stored = 0
    if bbg_gaps:
        from collections import defaultdict
        groups = defaultdict(list)
        for r in bbg_gaps:
            groups[(r["portfolio_id"], r["date"])].append(r)
        for (pid, dt), batch in groups.items():
            bbg_stored += await store_athena_bbg(pid, dt, batch)

    # Compute for admin rows → store in athena_admin
    admin_results = _compute_for_rows(admin_rows)
    admin_stored = 0
    if admin_results:
        admin_upsert = [{
            "portfolio_id": r["portfolio_id"],
            "date": r["date"],
            "isin": r["isin"],
            "par": r["par"],
            "accrued_t0": r["accrued_t0"],
            "accrued_c1": r["accrued_c1"],
            "accrued_t1": r["accrued_t1"],
            "accrued_c2": r["accrued_c2"],
            "accrued_c3": r["accrued_c3"],
        } for r in admin_results]
        admin_stored = await _upsert("athena_admin", admin_upsert, "portfolio_id,date,isin")

    logger.info(f"Direct accrued calc: {bbg_stored} athena_bbg + {admin_stored} athena_admin rows")
    return {"athena_bbg": bbg_stored, "athena_admin": admin_stored}


# ── GA10 orchestration ──────────────────────────────────────────────────────

async def recalc_all_existing() -> dict:
    """Retrigger GA10 recalc for every (portfolio, date) pair in recon_bbg.

    Finds all unique (portfolio_id, date) tuples that have BBG prices,
    fetches those prices from recon_bbg, and fires recalc_with_bbg_prices
    for each. Updates athena_bbg + recon_calcs with fresh results.

    Called after sync_bond_data to pick up any convention changes.
    """
    from recon_db import SUPABASE_URL, _headers

    # Get all unique (portfolio_id, date) pairs from recon_bbg
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/recon_bbg",
            headers=_headers(),
            params={"select": "portfolio_id,date,isin,price,par", "limit": "5000"},
        )
        if resp.status_code != 200:
            logger.error(f"recalc_all: failed to fetch recon_bbg: {resp.status_code}")
            return {"error": "failed to fetch recon_bbg"}
        rows = resp.json()

    # Group by (portfolio_id, date)
    groups: dict[tuple, dict] = {}
    for r in rows:
        key = (r["portfolio_id"], r["date"])
        if key not in groups:
            groups[key] = {"prices": {}, "par": {}}
        if r.get("price") is not None:
            groups[key]["prices"][r["isin"]] = float(r["price"])
        if r.get("par") is not None:
            groups[key]["par"][r["isin"]] = float(r["par"])

    logger.info(f"recalc_all: {len(groups)} (portfolio, date) pairs to recalc")

    results = {}
    for (pid, date), data in groups.items():
        if not data["prices"]:
            continue
        try:
            count = await recalc_with_bbg_prices(data["prices"], date, pid, data["par"])
            results[f"{pid}/{date}"] = count
            logger.info(f"recalc_all: {pid}/{date} → {count} calcs")
        except Exception as e:
            results[f"{pid}/{date}"] = f"error: {e}"
            logger.error(f"recalc_all: {pid}/{date} failed: {e}")

    return {"recalced": len(results), "details": results}


async def recalc_with_bbg_prices(bbg_prices: dict, price_date: str,
                                  portfolio_id: str, bbg_par: dict = None) -> int:
    """Store BBG prices in GA10, trigger recalc, fetch results, store in recon_calcs.

    Retries missing bonds up to 3 times with increasing delay.
    Alerts on partial failure after final retry.

    Returns the number of calcs stored.
    """
    isins = list(bbg_prices.keys())
    if not isins:
        return 0

    isin_set = set(isins)

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

            # 2. Calculate + retry loop
            max_retries = 3
            delays = [15, 30, 60]  # seconds between retries
            remaining_isins = list(isins)

            for attempt in range(max_retries + 1):
                if not remaining_isins:
                    break

                # Trigger calculation for remaining ISINs
                resp = await client.post(
                    f"{GA10_PRICING_URL}/prices/calculate",
                    json={"isins": remaining_isins, "price_date": price_date, "source": "BBG"},
                )
                calc_result = resp.json() if resp.status_code == 200 else {}
                calculated = calc_result.get("calculated", 0)

                # Wait for async processing if needed
                if calculated == 0:
                    await asyncio.sleep(10)

                # Fetch results and check which ISINs came back with data
                resp = await client.get(
                    f"{GA10_PRICING_URL}/prices/by-date?date={price_date}&source=BBG"
                )
                if resp.status_code != 200:
                    logger.warning(f"GA10 fetch failed on attempt {attempt + 1}: {resp.status_code}")
                    if attempt < max_retries:
                        await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
                    continue

                ga10_bonds = resp.json().get("bonds", [])

                # Check which of OUR ISINs have non-null accrued (= successful calc)
                returned_isins = set()
                for b in ga10_bonds:
                    isin = b.get("isin")
                    if isin in isin_set and b.get("accrued_interest") is not None:
                        returned_isins.add(isin)

                remaining_isins = [i for i in isins if i not in returned_isins]

                if attempt == 0:
                    logger.info(f"GA10 initial: {len(returned_isins)}/{len(isins)} bonds returned")
                else:
                    logger.info(f"GA10 retry {attempt}: {len(returned_isins)}/{len(isins)} bonds ({len(remaining_isins)} still missing)")

                if not remaining_isins:
                    break

                if attempt < max_retries:
                    delay = delays[min(attempt, len(delays) - 1)]
                    logger.info(f"GA10: {len(remaining_isins)} missing, retrying in {delay}s: {remaining_isins[:5]}")
                    await asyncio.sleep(delay)

            # Alert if bonds still missing after all retries
            if remaining_isins:
                logger.error(f"GA10 PARTIAL FAILURE: {len(remaining_isins)}/{len(isins)} bonds missing after {max_retries} retries: {remaining_isins}")
                asyncio.create_task(alert_ga10_partial_failure(
                    portfolio_id, price_date, len(isins), len(isins) - len(remaining_isins), remaining_isins
                ))

            # 3. Fetch final results and store
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
                    if isin not in isin_set:
                        continue
                    # Skip bonds with no accrued at all (T+0 or C+1)
                    if b.get("accrued_interest") is None and b.get("accrued_interest_c1") is None:
                        continue
                    par = par_lookup.get(isin) or 0

                    calcs.append({
                        "isin": isin,
                        "source_price": bbg_prices.get(isin),
                        "ga10_accrued": b.get("accrued_interest"),
                        "ga10_accrued_c1": b.get("accrued_interest_c1"),
                        "ga10_accrued_t1": b.get("accrued_interest_t1"),
                        "ga10_accrued_t2": b.get("accrued_interest_t2"),
                        "ga10_accrued_t3": b.get("accrued_interest_t3"),
                        # Use T+0 fields; C+1 can be garbage on weekends
                        "ga10_yield": b.get("yield_to_maturity"),
                        "ga10_yield_c1": b.get("ytm_c1"),
                        "ga10_yield_t1": b.get("ytm_t1"),
                        "ga10_yield_worst": b.get("ytw_bbg") or b.get("yield_to_maturity"),
                        "ga10_duration": b.get("modified_duration"),
                        "ga10_duration_worst": b.get("duration_worst"),
                        "ga10_spread": b.get("spread"),
                        "ga10_convexity": b.get("convexity"),
                        "ga10_dv01": b.get("dv01"),
                        "convention_used": b.get("day_count"),
                        "last_coupon_date": b.get("last_coupon_date"),
                        "issue_date": b.get("issue_date"),
                    })

                    if par:
                        mult = par / 100
                        def _scale(v, m=mult):
                            return v * m if v is not None else None
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

                logger.info(f"GA10 recalc complete: {len(calcs)} calcs, {len(athena_bbg_rows)} athena_bbg for {portfolio_id}/{price_date}")
                return len(calcs)

    except Exception as e:
        logger.error("BBG price recalc failed: %r", e)

    return 0


# ── Fast accrued recalc (local, no GA10 dependency) ─────────────────────────

async def recalc_accrued(portfolio_id: str, date: str, force: bool = False) -> dict:
    """Compute accrued interest locally for all bonds in a BBG upload.

    Uses local_bond_reference conventions (coupon, freq, day_count, accrual_date)
    and orca_holdings for par. No GA10 dependency — runs in seconds.

    Called automatically after every BBG upload (force=True) and also available
    via the /recalc/accrued HTTP endpoint.
    """
    from datetime import datetime, timedelta
    from recon_db import SUPABASE_URL, _headers, _upsert, BOND_DATA_URL, _bond_data_headers

    # Bloomberg CDR CNH Currency calendar, ported from
    # json_receiver_project/google_analysis10/google_analysis10.py:_add_china_holidays.
    # Source: State Council announcements, mirrored by Bloomberg.
    # Update this list each December when the State Council announces the next year.
    # Needed because QuantLib's China(IB) calendar also lacks future holidays and
    # recon-mcp doesn't use QuantLib at all.
    _CNY_HOLIDAYS = frozenset([
        # 2025
        (2025, 1, 1),
        (2025, 1, 28), (2025, 1, 29), (2025, 1, 30), (2025, 1, 31),
        (2025, 2, 1), (2025, 2, 2), (2025, 2, 3), (2025, 2, 4),
        (2025, 4, 4), (2025, 4, 5),
        (2025, 5, 1), (2025, 5, 2), (2025, 5, 5),
        (2025, 5, 31),
        (2025, 10, 1), (2025, 10, 2), (2025, 10, 3), (2025, 10, 6), (2025, 10, 7),
        # 2026
        (2026, 1, 1), (2026, 1, 2),
        (2026, 2, 16), (2026, 2, 17), (2026, 2, 18), (2026, 2, 19),
        (2026, 2, 20), (2026, 2, 23),
        (2026, 4, 3), (2026, 4, 4), (2026, 4, 7),
        (2026, 5, 1), (2026, 5, 4), (2026, 5, 5),
        (2026, 5, 25),
        (2026, 6, 19),
        (2026, 7, 1),
        (2026, 9, 25), (2026, 9, 26),
        (2026, 10, 1), (2026, 10, 2), (2026, 10, 5), (2026, 10, 6), (2026, 10, 7),
        # 2027
        (2027, 1, 1),
        (2027, 2, 5), (2027, 2, 8), (2027, 2, 9),
        (2027, 2, 10), (2027, 2, 11), (2027, 2, 12),
        (2027, 3, 26), (2027, 3, 27), (2027, 3, 29),
        (2027, 4, 5),
        (2027, 5, 1), (2027, 5, 3), (2027, 5, 4), (2027, 5, 5),
        (2027, 5, 13),
        (2027, 6, 9),
        (2027, 7, 1),
        (2027, 9, 15), (2027, 9, 16),
        (2027, 10, 1), (2027, 10, 2),
        # 2028
        (2028, 1, 3),
        (2028, 1, 24), (2028, 1, 25), (2028, 1, 26), (2028, 1, 27),
        (2028, 1, 28), (2028, 1, 31),
        (2028, 4, 3), (2028, 4, 4),
        (2028, 4, 14), (2028, 4, 15), (2028, 4, 17),
        (2028, 5, 1), (2028, 5, 2), (2028, 5, 3), (2028, 5, 4), (2028, 5, 5),
        (2028, 5, 29),
        (2028, 7, 1),
        (2028, 10, 1), (2028, 10, 2),
    ])

    def _is_non_business(dt, currency):
        """Weekend-or-CNY-holiday check. CNY calendar only applies to CNY/CNH bonds."""
        if dt.weekday() >= 5:
            return True
        if currency in ('CNY', 'CNH') and (dt.year, dt.month, dt.day) in _CNY_HOLIDAYS:
            return True
        return False

    def _adjust_bdc(dt, bdc, currency=None):
        """Apply business day convention. Rolls past weekends, and past Bloomberg
        CDR CNH holidays when the bond is in CNY/CNH."""
        if not bdc or bdc == 'Unadjusted':
            return dt
        if not _is_non_business(dt, currency):
            return dt
        # Walk forward up to 14 days to find the next business day
        following = dt
        for _ in range(14):
            following = following + timedelta(days=1)
            if not _is_non_business(following, currency):
                break
        if bdc == 'Following':
            return following
        if bdc == 'ModifiedFollowing':
            # Spill into next month → roll backward from dt instead
            if following.month != dt.month:
                preceding = dt
                for _ in range(14):
                    preceding = preceding - timedelta(days=1)
                    if not _is_non_business(preceding, currency):
                        return preceding
                return dt
            return following
        if bdc == 'Preceding':
            preceding = dt
            for _ in range(14):
                preceding = preceding - timedelta(days=1)
                if not _is_non_business(preceding, currency):
                    return preceding
            return dt
        return dt

    def _last_coupon_before(settle, coup_months, coup_day, bdc='Unadjusted', currency=None):
        last_coupon = None
        for y in [settle.year, settle.year - 1]:
            for m in sorted(coup_months, reverse=True):
                try:
                    cd = datetime(y, m, coup_day)
                except ValueError:
                    cd = datetime(y, m, 28)
                cd = _adjust_bdc(cd, bdc, currency)
                if cd < settle:
                    if last_coupon is None or cd > last_coupon:
                        last_coupon = cd
                    break
        return last_coupon

    def _accrued_at(settle, coupon, freq, mat, coup_months, coup_day, day_count, par,
                    accrual_start=None, bdc='Unadjusted', currency=None):
        last_coupon = _last_coupon_before(settle, coup_months, coup_day, bdc, currency)
        if last_coupon and accrual_start and last_coupon < accrual_start:
            last_coupon = accrual_start
        # Phantom coupon: coup_day arithmetic can land just after issue date — discard it
        if last_coupon and accrual_start and last_coupon >= accrual_start and (last_coupon - accrual_start).days < 30:
            last_coupon = accrual_start
        if not last_coupon:
            if accrual_start and accrual_start < settle:
                last_coupon = accrual_start
            else:
                return 0.0

        if "30" in day_count:
            d1 = min(last_coupon.day, 30)
            d2 = min(settle.day, 30) if d1 == 30 else settle.day
            days = (settle.year - last_coupon.year) * 360 + (settle.month - last_coupon.month) * 30 + (d2 - d1)
            per100 = coupon / freq * days / (360 / freq)
        elif "365" in day_count:
            # Chinese bond (PBOC/NAFMII) convention: accrue at full annual rate / 365,
            # regardless of payment frequency (semi-annual bonds do NOT divide coupon by 2).
            accrual_freq = 1 if freq == 2 else freq
            per100 = coupon / accrual_freq * (settle - last_coupon).days / 365
        else:
            actual = (settle - last_coupon).days
            nc = None
            for m in sorted(coup_months):
                try:
                    nc = datetime(last_coupon.year if m > last_coupon.month else last_coupon.year + 1, m, coup_day)
                except ValueError:
                    nc = datetime(last_coupon.year if m > last_coupon.month else last_coupon.year + 1, m, 28)
                if nc > last_coupon:
                    break
            period = (nc - last_coupon).days if nc else 182
            per100 = coupon / freq * actual / period

        return round(per100 * par / 100, 6)

    async with httpx.AsyncClient(timeout=15) as client:
        bbg_resp, athena_resp, ref_resp, holdings_resp, maia_resp, admin_resp = await asyncio.gather(
            client.get(f"{SUPABASE_URL}/rest/v1/recon_bbg", headers=_headers(), params={
                "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
                "select": "isin,par,price,accrued,accrued_pct,maturity_date",
            }),
            client.get(f"{SUPABASE_URL}/rest/v1/athena_bbg", headers=_headers(), params={
                "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
                "select": "isin,par,accrued_c1,source_price,static_hash",
            }),
            client.get(f"{SUPABASE_URL}/rest/v1/local_bond_reference", headers=_headers(), params={
                "select": "isin,currency,maturity_date,day_count,frequency,accrual_date",
            }),
            client.get(f"{SUPABASE_URL}/rest/v1/orca_holdings", headers=_headers(), params={
                "portfolio_id": f"eq.{portfolio_id}", "select": "isin,par_amount",
            }),
            # Fall-back sources when recon_bbg is empty for the date (e.g.
            # triggering a recalc against a maia or admin-only date so the
            # maia recon view has athena_bbg rows with gap=0).
            client.get(f"{SUPABASE_URL}/rest/v1/recon_maia", headers=_headers(), params={
                "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
                "select": "isin,par,price,maturity_date",
            }),
            client.get(f"{SUPABASE_URL}/rest/v1/recon_admin", headers=_headers(), params={
                "portfolio_id": f"eq.{portfolio_id}", "date": f"eq.{date}",
                "select": "isin,par,price",
            }),
        )

    bbg_bonds    = {r["isin"]: r for r in (bbg_resp.json()      if bbg_resp.status_code      == 200 else [])}
    athena_map   = {r["isin"]: r for r in (athena_resp.json()   if athena_resp.status_code   == 200 else [])}
    ref_map      = {r["isin"]: r for r in (ref_resp.json()      if ref_resp.status_code      == 200 else [])}
    holdings_map = {r["isin"]: float(r["par_amount"]) for r in (holdings_resp.json() if holdings_resp.status_code == 200 else []) if r.get("par_amount")}
    maia_bonds   = {r["isin"]: r for r in (maia_resp.json()     if maia_resp.status_code     == 200 else [])}
    admin_bonds  = {r["isin"]: r for r in (admin_resp.json()    if admin_resp.status_code    == 200 else [])}

    # If recon_bbg has no rows for this date, fall back to maia then admin as
    # the position/price source. This lets the same recalc_accrued code path
    # populate athena_bbg for maia-only dates so the v_athena_maia_accrued
    # view can join at gap=0 and expose all four T+x offsets.
    if not bbg_bonds and maia_bonds:
        logger.info("recalc_accrued: no recon_bbg for %s/%s — falling back to recon_maia (%d bonds)",
                    portfolio_id, date, len(maia_bonds))
        bbg_bonds = {
            isin: {
                "isin": isin,
                "par": r.get("par"),
                "price": r.get("price"),
                "maturity_date": r.get("maturity_date"),
                "accrued": None,
                "accrued_pct": None,
            }
            for isin, r in maia_bonds.items() if r.get("par") is not None
        }
    elif not bbg_bonds and admin_bonds:
        logger.info("recalc_accrued: no recon_bbg for %s/%s — falling back to recon_admin (%d bonds)",
                    portfolio_id, date, len(admin_bonds))
        bbg_bonds = {
            isin: {
                "isin": isin,
                "par": r.get("par"),
                "price": r.get("price"),
                "accrued": None,
                "accrued_pct": None,
            }
            for isin, r in admin_bonds.items() if r.get("par") is not None
        }

    # Fetch coupon + business_day_convention from bond_identity, filtered to only
    # the ISINs we care about. Without the filter, PostgREST caps the response at
    # 1000 rows (bond_identity has ~33k), so most HK/XS bonds get silently dropped
    # and fall through to "missing reference data" — which is exactly the bug
    # that caused CNY accrued diffs to persist after the ACT/365 fix was deployed.
    bbg_isins = list(bbg_bonds.keys())
    if bbg_isins:
        async with httpx.AsyncClient(timeout=15) as client:
            identity_resp = await client.get(
                f"{BOND_DATA_URL}/rest/v1/bond_identity",
                headers=_bond_data_headers(),
                params={
                    "isin": f"in.({','.join(bbg_isins)})",
                    "select": "isin,coupon,business_day_convention",
                },
            )
        if identity_resp.status_code == 200:
            for r in identity_resp.json():
                # Ensure ref entry exists even if local_bond_reference lacks this ISIN
                if r["isin"] not in ref_map:
                    ref_map[r["isin"]] = {"isin": r["isin"]}
                if r.get("coupon") is not None:
                    ref_map[r["isin"]]["coupon"] = float(r["coupon"])
                ref_map[r["isin"]]["bdc"] = r.get("business_day_convention") or "Unadjusted"

    trade_date = datetime.strptime(date, "%Y-%m-%d")

    def _static_hash(ref: dict) -> str:
        """Short hash of the static inputs that drive accrued calculation.
        If this differs from the stored value, recalc is forced even if numbers look fine."""
        parts = "|".join([
            str(ref.get("day_count") or ""),
            str(ref.get("frequency") or ""),
            str(ref.get("coupon") or ""),
            str(ref.get("maturity_date") or ""),
            str(ref.get("accrual_date") or ""),
            str(ref.get("bdc") or "Unadjusted"),
        ])
        return hashlib.md5(parts.encode()).hexdigest()[:16]

    # Detect bonds whose static convention data has changed since last recalc
    current_hashes = {isin: _static_hash(ref_map[isin]) for isin in bbg_bonds if isin in ref_map}
    hash_changed = {
        isin for isin, h in current_hashes.items()
        if athena_map.get(isin, {}).get("static_hash") != h
    }
    if hash_changed and not force:
        logger.info(
            "recalc_accrued: %d bond(s) have changed static data → forcing recalc: %s",
            len(hash_changed), sorted(hash_changed),
        )

    needs_recalc = list(bbg_bonds) if force else list({
        isin for isin, bbg in bbg_bonds.items()
        if (athena_map.get(isin, {}).get("accrued_c1") is None
            or bbg.get("accrued") is None
            or (bbg["accrued"] != 0
                and abs((athena_map[isin]["accrued_c1"] - bbg["accrued"]) / bbg["accrued"]) > 0.0001))
    } | hash_changed)

    if not needs_recalc:
        return {"recalculated": 0, "message": "All bonds already match BBG"}

    updated_rows, skipped = [], []
    for isin in needs_recalc:
        ref    = ref_map.get(isin)
        bbg    = bbg_bonds[isin]
        athena = athena_map.get(isin, {})

        if not ref or not ref.get("coupon") or not ref.get("maturity_date"):
            skipped.append({"isin": isin, "reason": "missing reference data"})
            continue

        coupon    = float(ref["coupon"])
        maturity  = bbg.get("maturity_date") or ref["maturity_date"]
        day_count = ref.get("day_count") or "30/360"
        par = (holdings_map.get(isin)
               or (float(bbg["par"]) if bbg.get("par") else None)
               or (float(athena["par"]) if athena.get("par") else None))
        if not par:
            skipped.append({"isin": isin, "reason": "no par amount"}); continue

        price = (float(bbg["price"]) if bbg.get("price")
                 else (float(athena["source_price"]) if athena.get("source_price") else None))
        if not price:
            skipped.append({"isin": isin, "reason": "no price"}); continue

        mat = datetime.strptime(maturity[:10], "%Y-%m-%d")
        freq_str = (ref.get("frequency") or "").lower()
        if freq_str in ("annual", "annually", "1"):
            freq, coup_months = 1, [mat.month]
        else:
            freq = 2
            coup_months = sorted(set([(mat.month - 1) % 12 + 1, ((mat.month + 5) % 12) + 1]))
        coup_day = mat.day

        accrual_date_str = ref.get("accrual_date")
        accrual_start = datetime.strptime(accrual_date_str[:10], "%Y-%m-%d") if accrual_date_str else None
        bdc = ref.get("bdc") or "Unadjusted"
        currency = ref.get("currency")

        args = (coupon, freq, mat, coup_months, coup_day, day_count, par)
        lc = _last_coupon_before(trade_date, coup_months, coup_day, bdc, currency)
        if lc and accrual_start and lc < accrual_start:
            lc = accrual_start
        if lc and accrual_start and lc >= accrual_start and (lc - accrual_start).days < 30:
            lc = accrual_start
        days_acc = (trade_date - lc).days if lc else None

        acc_t0 = _accrued_at(trade_date,                     *args, accrual_start=accrual_start, bdc=bdc, currency=currency)
        acc_c1 = _accrued_at(trade_date + timedelta(days=1), *args, accrual_start=accrual_start, bdc=bdc, currency=currency)
        acc_c2 = _accrued_at(trade_date + timedelta(days=2), *args, accrual_start=accrual_start, bdc=bdc, currency=currency)
        acc_c3 = _accrued_at(trade_date + timedelta(days=3), *args, accrual_start=accrual_start, bdc=bdc, currency=currency)

        # BBG reference accrued: prefer par × accrued_pct/100 (matches SQL view), fall back to stored accrued
        bbg_pct = bbg.get("accrued_pct")
        bbg_ref = (float(bbg_pct) * par / 100) if bbg_pct is not None else (float(bbg["accrued"]) if bbg.get("accrued") else None)

        # Find which offset is closest to BBG — for diagnostic display
        offset_candidates = [("T+0", acc_t0), ("C+1", acc_c1), ("C+2", acc_c2), ("C+3", acc_c3)]
        if bbg_ref and par:
            best_name, best_acc = min(
                [(n, v) for n, v in offset_candidates if v is not None],
                key=lambda x: abs(x[1] - bbg_ref), default=(None, None)
            )
            best_diff_per100 = abs(best_acc - bbg_ref) / par * 100 if best_acc is not None else None
        else:
            best_name, best_diff_per100 = None, None

        # Convention diagnosis: if best offset is still off by >0.01 per 100 face,
        # brute-force day_count × frequency × offset to find what would match BBG.
        conv_hypothesis = None
        conv_diff_per100 = None
        if bbg_ref and par and best_diff_per100 is not None and best_diff_per100 > 0.01:
            day_counts = ["30/360", "ACT/365", "ACT/ACT", "30E/360"]
            freqs      = [1, 2]
            best_hyp_diff = float("inf")
            for dc_try in day_counts:
                for freq_try in freqs:
                    # Recompute coup_months for this frequency
                    if freq_try == 1:
                        cm_try = [mat.month]
                    else:
                        cm_try = sorted(set([(mat.month - 1) % 12 + 1, ((mat.month + 5) % 12) + 1]))
                    freq_label = "Annual" if freq_try == 1 else "Semi"
                    args_try = (coupon, freq_try, mat, cm_try, coup_day, dc_try, par)
                    for offset in range(4):
                        settle_try = trade_date + timedelta(days=offset)
                        acc_try = _accrued_at(settle_try, *args_try, accrual_start=accrual_start, bdc=bdc, currency=currency)
                        diff_try = abs(acc_try - bbg_ref) / par * 100
                        if diff_try < best_hyp_diff:
                            best_hyp_diff = diff_try
                            offset_label = f"C+{offset}" if offset else "T+0"
                            conv_hypothesis = f"{dc_try} {freq_label} {offset_label}"
                            conv_diff_per100 = round(diff_try, 6)
            # If hypothesis matches the current convention, clear it (no new info)
            curr_dc  = "30/360" if "30" in day_count else day_count
            curr_lbl = "Annual" if freq == 1 else "Semi"
            if conv_hypothesis == f"{curr_dc} {curr_lbl} C+1":
                conv_hypothesis = None
            logger.debug(
                "recalc_accrued: %s bbg_ref=%.4f best_c1_diff=%.4f/100 → hypothesis=%s diff=%.4f/100",
                isin, bbg_ref, best_diff_per100, conv_hypothesis, conv_diff_per100 or 0,
            )

        updated_rows.append({
            "isin": isin, "par": par, "source_price": price,
            "day_count": day_count if "30" not in day_count else "30/360",
            "last_coupon_date": lc.date().isoformat() if lc else None,
            "days_accrued": days_acc,
            "accrued_t0": acc_t0,
            "accrued_c1": acc_c1,
            "accrued_t1": acc_c1,   # T+1 same as C+1 (BBG uses raw calendar, no business-day roll)
            "accrued_c2": acc_c2,
            "accrued_c3": acc_c3,
            "conv_hypothesis":  conv_hypothesis,
            "conv_diff_per100": conv_diff_per100,
            "static_hash": current_hashes.get(isin),
        })

    if updated_rows:
        await _upsert("athena_bbg",
                      [{"portfolio_id": portfolio_id, "date": date, **r} for r in updated_rows],
                      "portfolio_id,date,isin")

    n_hypothesis = sum(1 for r in updated_rows if r.get("conv_hypothesis"))
    logger.info(
        "recalc_accrued: %s/%s → %d updated, %d skipped, %d with convention hypothesis",
        portfolio_id, date, len(updated_rows), len(skipped), n_hypothesis,
    )
    return {
        "recalculated": len(updated_rows),
        "checked": len(needs_recalc),
        "skipped": skipped,
        "convention_hypotheses": n_hypothesis,
        "bonds": [
            {
                "isin": r["isin"],
                "accrued_c1": r["accrued_c1"],
                **({"conv_hypothesis": r["conv_hypothesis"], "conv_diff_per100": r["conv_diff_per100"]}
                   if r.get("conv_hypothesis") else {}),
            }
            for r in updated_rows
        ],
    }


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
    issue_date_bonds = bbg_result.get("issue_date_bonds", {})
    maturity_date_bonds = bbg_result.get("maturity_date_bonds", {})
    coupon_bonds = bbg_result.get("coupon_bonds", {})
    cpn_rate_bonds = bbg_result.get("cpn_rate_bonds", {})
    cpn_freq_bonds = bbg_result.get("cpn_freq_bonds", {})
    day_count_bonds = bbg_result.get("day_count_bonds", {})
    eff_maturity_bonds = bbg_result.get("eff_maturity_bonds", {})
    first_coupon_bonds = bbg_result.get("first_coupon_bonds", {})
    accrued_pct_bonds = bbg_result.get("accrued_pct_bonds", {})
    moodys_bonds = bbg_result.get("moodys_bonds", {})
    sp_bonds = bbg_result.get("sp_bonds", {})
    fitch_bonds = bbg_result.get("fitch_bonds", {})
    bb_comp_bonds = bbg_result.get("bb_comp_bonds", {})
    all_isins = list(set(list(price_bonds.keys()) + list(accrued_bonds.keys())))

    ref_by_isin = await lookup_bond_reference(all_isins)

    bbg_bonds = []
    for isin in all_isins:
        ref = ref_by_isin.get(isin, {})
        bbg_bonds.append({
            "isin": isin,
            "description": ref.get("description", ""),
            "currency": ref.get("currency", "USD"),
            "coupon": cpn_rate_bonds.get(isin) or coupon_bonds.get(isin) or ref.get("coupon"),
            "maturity_date": eff_maturity_bonds.get(isin) or maturity_date_bonds.get(isin) or ref.get("maturity_date") or None,
            "price": price_bonds.get(isin),
            "accrued": accrued_bonds.get(isin),
            "yield_to_worst": yield_bonds.get(isin),
            "duration": oad_bonds.get(isin),
            "mv": mv_bonds.get(isin),
            "par": position_bonds.get(isin),
            "issue_date": issue_date_bonds.get(isin),
            "coupon_freq": cpn_freq_bonds.get(isin),
            "day_count": day_count_bonds.get(isin),
            "first_coupon_date": first_coupon_bonds.get(isin),
            "accrued_pct": accrued_pct_bonds.get(isin),
            "moodys": moodys_bonds.get(isin),
            "sp": sp_bonds.get(isin),
            "fitch": fitch_bonds.get(isin),
            "bb_comp": bb_comp_bonds.get(isin),
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

    # Enrich local bond tables with BBG-parsed maturity/coupon for NULL fields.
    # Awaited so that local_bond_reference is up-to-date before the recalc fires.
    await enrich_bond_data_from_bbg(
        maturity_date_bonds,
        coupon_bonds,
        cpn_freq_bonds=cpn_freq_bonds,
        day_count_bonds=day_count_bonds,
        eff_maturity_bonds=eff_maturity_bonds,
        first_coupon_bonds=first_coupon_bonds,
    )

    # Fast accrued recalc — runs immediately after enrich so local_bond_reference is fresh
    asyncio.create_task(recalc_accrued(pid, bbg_date, force=True))

    # Sync bond data + Orca holdings for uploaded ISINs (fire-and-forget)
    asyncio.create_task(sync_bond_data(all_isins))
    asyncio.create_task(sync_orca_holdings(pid))

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

    # Write admin prices to bond_analytics_dated (bond-data Supabase)
    asyncio.create_task(_store_admin_prices_to_bond_data(admin_bonds, admin_date))

    # Sync bond data for uploaded ISINs
    admin_isins = [b["isin"] for b in admin_bonds if b.get("isin")]
    asyncio.create_task(sync_bond_data(admin_isins))

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
    maia_bonds, maia_meta = parse_maia_tsv(tsv)
    logger.info(f"Maia parsed: {len(maia_bonds)} bonds, date={maia_date}, pid={maia_pid}, meta={maia_meta}")

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

    fx_rate = maia_meta.get("fx_cnh_per_usd")
    await asyncio.gather(
        store_maia(maia_pid, maia_date, maia_bonds, uploaded_by, fx_cnh_per_usd=fx_rate),
        store_raw_upload(
            source="maia", portfolio_id=maia_pid, date=maia_date,
            file_bytes=file_bytes, filename=filename,
            uploaded_by=uploaded_by, bonds_parsed=len(maia_bonds),
        ),
    )

    # Sync bond data for uploaded ISINs
    maia_isins = [b["isin"] for b in maia_bonds if b.get("isin")]
    asyncio.create_task(sync_bond_data(maia_isins))

    asyncio.create_task(alert_upload_success("maia", maia_pid, maia_date, len(maia_bonds), filename))

    return {
        "status": "ok",
        "portfolio_id": maia_pid,
        "date": maia_date,
        "bonds_parsed": len(maia_bonds),
        "bonds": len(maia_bonds),
        "rows": len(tsv.strip().splitlines()),
    }
