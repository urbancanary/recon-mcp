"""Hash helpers for the calc-staleness detector.

Stamps recon_calcs rows with content-derived fingerprints of the inputs
used (static + price) and the engine layer ids that produced them. A view
(v_stale_calcs in sql/005_calc_staleness_hashes.sql) compares these against
current values to surface stale rows for the recalc cron.

Design doc: athena_html_v3 memory project_calc_staleness_detector.md
Backlog: codebase-mcp id=371 (Option A — promote engine_hash to GCP backend)
"""
import hashlib
import json
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GA10_PRICING_URL = os.environ.get("GA10_PRICING_URL", "")
GA10_GATEWAY_URL = os.environ.get(
    "GA10_GATEWAY_URL",
    "https://ga10-gateway.urbancanary.workers.dev",
)


def compute_static_hash(ref: Optional[dict], identity: Optional[dict]) -> Optional[str]:
    """Hash exactly the static fields recon-mcp sends to GA10.

    Whitelist — Tier A display fields (description, country, ticker) are
    deliberately NOT included so corrections to display data don't trip
    a recalc. Returns None when ref is missing entirely.
    """
    if not ref:
        return None
    fields = {
        "coupon":       _fmt_num(ref.get("coupon")),
        "maturity":     _fmt_str(ref.get("maturity_date")),
        "day_count":    _fmt_str(ref.get("day_count")),
        "frequency":    _fmt_str(ref.get("frequency")),
        "accrual_date": _fmt_str(ref.get("accrual_date")),
        "bdc":          _fmt_str((identity or {}).get("business_day_convention")),
    }
    blob = json.dumps(fields, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def compute_price_hash(price, price_date, source_table: str) -> Optional[str]:
    """Hash price + date + source table.

    source_table is the recon-mcp table the price came from
    (recon_bbg / recon_admin / recon_maia). When a fallback priority order
    later gets defined, encoding the source name here means a re-prioritize
    correctly invalidates the calcs that flipped source.
    """
    if price is None or price_date is None or not source_table:
        return None
    fields = {
        "price":      _fmt_num(price),
        "price_date": _fmt_str(price_date),
        "source":     source_table,
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()[:16]


async def fetch_engine_version() -> dict:
    """Fetch /engine/version from ga10-pricing and ga10-gateway.

    Returns: {"pricing": {...} | None, "gateway": {...} | None}
    Failures don't raise — they leave the failing layer as None, which the
    consuming code should treat as drift (better to over-recalc than
    silently keep stale rows).
    """
    async def _get(url: str) -> Optional[dict]:
        if not url:
            return None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{url.rstrip('/')}/engine/version")
                if resp.status_code == 200:
                    return resp.json()
                logger.warning("engine/version %s -> %s", url, resp.status_code)
        except Exception as e:
            logger.warning("engine/version fetch failed %s: %r", url, e)
        return None

    pricing = await _get(GA10_PRICING_URL)
    gateway = await _get(GA10_GATEWAY_URL)
    return {"pricing": pricing, "gateway": gateway}


def engine_ids_from(engine_version: dict) -> tuple[Optional[str], Optional[str]]:
    """Extract (pricing_id, gateway_id) from a fetch_engine_version() result."""
    pricing = (engine_version or {}).get("pricing") or {}
    gateway = (engine_version or {}).get("gateway") or {}
    return pricing.get("version_id"), gateway.get("version_id")


def _fmt_num(v) -> Optional[str]:
    if v is None:
        return None
    try:
        return f"{float(v):.6f}"
    except (TypeError, ValueError):
        return str(v)


def _fmt_str(v) -> Optional[str]:
    if v is None:
        return None
    return str(v)
