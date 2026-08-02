"""Human + service auth for recon-mcp's UI and AUM endpoints.

Same pattern as Athena (athena_html_v3/orca_session.py): login is owned by
orca — browsers carry the xt_session cookie (Domain=.x-trillion.com), which we
verify against orca's /auth/verify-session with a short-TTL cache and a
stale-grace fail-open for already-authenticated users.

NOTE the cookie's domain: it is only sent when this service is reached via an
*.x-trillion.com hostname (or via Athena's proxy, which does its own session
check and forwards with X-Service-Key). On the bare Railway hostname a browser
session can never authenticate — that is a DNS task, not a code path.

Escapes, in precedence order:
  HETZNER_NOAUTH=1      Guinness box: no-login identity (same env var and
                        semantics as Athena's xt_auth middleware).
  X-Service-Key         machine calls (Athena proxy, mail-ingest push):
                        HMAC token minted with ATHENA_SERVICE_KEY via
                        export_auth-style generate/verify below.
  xt_session cookie     humans.

The pre-existing endpoints (/upload/*, /recon/*, /sweep/*, /webhooks/*,
/recalc/*) keep their current open behaviour — ga10's cron and the Supabase
webhook call them unauthenticated today; gating them is a separate,
deliberate change.
"""

import hashlib
import hmac
import logging
import os
import time
from typing import Optional

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

logger = logging.getLogger("recon.auth")

COOKIE_NAME = "xt_session"
_CACHE_TTL = 300
_STALE_GRACE = 3600
_cache: dict[str, tuple[dict, float]] = {}

BOX_USER = {"email": "box-noauth@guinnessgi.com", "is_admin": False}
SERVICE_USER = {"email": "service@recon-mcp", "service": True}


def _hetzner_noauth() -> bool:
    return os.environ.get("HETZNER_NOAUTH", "").strip().lower() in {"1", "true", "yes", "on"}


# ── Service key (machine-to-machine) ───────────────────────────────────────

_service_key_cache: Optional[str] = None


def _service_key() -> str:
    global _service_key_cache
    if _service_key_cache is None:
        try:
            from auth_client import get_api_key
            _service_key_cache = get_api_key("ATHENA_SERVICE_KEY") or ""
        except Exception as e:
            logger.warning("ATHENA_SERVICE_KEY lookup failed: %s", e)
            _service_key_cache = ""
    return _service_key_cache


def verify_service_token(token: str) -> bool:
    """Constant-time check of an X-Service-Key HMAC: hex(hmac_sha256(key, 'recon-mcp'))."""
    key = _service_key()
    if not key or not token:
        return False
    expected = hmac.new(key.encode(), b"recon-mcp", hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, token)


# ── Orca session verification (humans) ─────────────────────────────────────

def _orca_url() -> str:
    env = os.environ.get("ORCA_MCP_URL", "").strip()
    if env:
        return env
    try:
        from auth_client import get_service_url
        return get_service_url("ORCA_MCP_URL") or ""
    except Exception as e:
        logger.warning("get_service_url(ORCA_MCP_URL) failed: %s", e)
        return ""


def login_url() -> str:
    """Where to send an unauthenticated browser (must be *.x-trillion.com so
    the cookie the flow sets is actually stored)."""
    try:
        from auth_client import get_service_url
        url = get_service_url("LOGIN_URL")
        if url:
            return url
    except Exception as e:
        logger.warning("get_service_url(LOGIN_URL) failed: %s", e)
    return os.environ.get("LOGIN_URL") or _orca_url()


async def verify_session(token: str) -> Optional[dict]:
    now = time.time()
    cached = _cache.get(token)
    if cached and now - cached[1] < _CACHE_TTL:
        return cached[0]
    url = _orca_url()
    if not url:
        logger.error("ORCA_MCP_URL unavailable — cannot verify session")
        return cached[0] if cached else None
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{url}/auth/verify-session", params={"token": token})
        if r.status_code == 200:
            user = r.json()
            _cache[token] = (user, now)
            return user
        if r.status_code == 401:
            _cache.pop(token, None)
            return None
        logger.warning("verify-session returned %s, keeping cached", r.status_code)
    except Exception as e:
        logger.warning("verify-session network error: %s", e)
    if cached and now - cached[1] < _STALE_GRACE:
        return cached[0]
    return None


# ── The dependency ──────────────────────────────────────────────────────────

async def current_user(request: Request) -> Optional[dict]:
    if _hetzner_noauth():
        return dict(BOX_USER)
    svc = request.headers.get("X-Service-Key")
    if svc and verify_service_token(svc):
        return dict(SERVICE_USER)
    token = request.cookies.get(COOKIE_NAME)
    if token:
        return await verify_session(token)
    return None


def unauthorized(request: Request):
    """401 JSON for API calls; login redirect for browser page loads."""
    accepts_html = "text/html" in (request.headers.get("accept") or "")
    if accepts_html and request.method == "GET":
        base = login_url()
        if base:
            proto = request.headers.get("x-forwarded-proto", "https")
            host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
            return_to = f"{proto}://{host}{request.url.path}"
            from urllib.parse import quote
            return RedirectResponse(
                f"{base}/auth/login?return_to={quote(return_to, safe='')}&app=recon",
                status_code=302)
    return JSONResponse({"error": "Unauthorized"}, status_code=401)
