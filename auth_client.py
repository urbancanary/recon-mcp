"""
Auth client for API key retrieval.
Routes through Orca gateway with fallback to environment variables.
Caches results in-memory so transient auth-mcp failures don't break the app.
"""

import hashlib
import logging
import os
import secrets
import time

import httpx

logger = logging.getLogger(__name__)

ORCA_URL = os.environ.get("ORCA_URL", "")
if not ORCA_URL:
    raise RuntimeError("Configuration missing")
_CACHE_TTL = 300  # 5 minutes
_cache: dict[str, tuple[str, float]] = {}  # key -> (value, expiry)


def generate_auth_token() -> str:
    """Generate a session token."""
    import hmac as _hmac
    random_part = secrets.token_hex(8)
    auth_secret_hash = os.environ.get("AUTH_SECRET_HASH", "")
    service_id = os.environ.get("RAILWAY_SERVICE_ID", "local-dev")

    if auth_secret_hash:
        payload = f"{random_part}|{service_id}"
        mac = _hmac.new(auth_secret_hash.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
        return f"{random_part}|{service_id}|{mac}"
    else:
        checksum = hashlib.sha256(random_part.encode()).hexdigest()[:8]
        return f"{random_part}-{checksum}"


def _fetch_from_auth(path: str, params: dict = None) -> str:
    """Fetch a value from the auth service, with caching."""
    now = time.time()
    cached = _cache.get(path)
    if cached and cached[1] > now:
        return cached[0]

    auth_token = os.environ.get("AUTH_MCP_TOKEN") or generate_auth_token()
    try:
        response = httpx.get(
            f"{ORCA_URL}{path}",
            params=params or {},
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=5.0,
        )
        if response.status_code == 200:
            value = response.json().get("value", "")
            if value:
                _cache[path] = (value, now + _CACHE_TTL)
            return value
    except Exception as e:
        logger.warning(f"Auth service failed for '{path}': {e}")
        # Return stale cache if available (better than nothing)
        if cached:
            logger.info(f"Using stale cached value for '{path}'")
            return cached[0]

    return ""


def get_api_key(key_name: str, requester: str = "") -> str:
    """Get API key from auth service. No env var fallback — fail loudly."""
    params = {"requester": requester} if requester else {}
    value = _fetch_from_auth(f"/auth/api/key/{key_name}", params)
    if not value:
        logger.error(f"auth-mcp returned no value for '{key_name}' (requester={requester})")
    return value


def get_service_url(service_name: str) -> str:
    """Get service URL from auth service. No env var fallback — fail loudly."""
    value = _fetch_from_auth(f"/auth/api/key/{service_name}")
    if not value:
        logger.error(f"auth-mcp returned no value for service URL '{service_name}'")
    return value
