"""Backlog 5414: the GA10 auth-mcp URL must not be resolved synchronously on
the event loop.

Startup primes the resolvers in a worker thread (``asyncio.to_thread``); after
that every async-path call is an in-process cache hit and never touches the
network from the loop.
"""
import asyncio
import threading

import app as app_module
import auth_client
import aum_orchestrator
import calc_hashes
import recon_engine


def test_prime_resolves_off_the_event_loop(monkeypatch):
    """The blocking auth-mcp lookup runs on a worker thread, not the loop."""
    seen = {}

    def fake_get_service_url(name):
        seen["name"] = name
        seen["thread"] = threading.get_ident()
        return "https://ga10.test"

    monkeypatch.setattr(auth_client, "get_service_url", fake_get_service_url)
    monkeypatch.setattr(aum_orchestrator, "_gae_url_cache", "")
    monkeypatch.setattr(calc_hashes, "_ga10_backend_url", "")
    monkeypatch.setattr(recon_engine, "_ga10_pricing_url", "")
    monkeypatch.delenv("GA10_PRICING_URL", raising=False)
    monkeypatch.delenv("GA10_BACKEND_URL", raising=False)

    main_thread = threading.get_ident()
    asyncio.run(app_module._prime_service_urls())

    assert seen.get("thread") is not None, "prime never called auth-mcp"
    assert seen["thread"] != main_thread, "auth-mcp resolved on the loop thread"


def test_async_path_is_a_cache_hit_after_prime(monkeypatch):
    """Once primed, the resolvers never call the synchronous network function."""
    monkeypatch.setattr(aum_orchestrator, "_gae_url_cache", "https://ga10.test")
    monkeypatch.setattr(calc_hashes, "_ga10_backend_url", "https://ga10.test")

    calls = []
    monkeypatch.setattr(auth_client, "get_service_url",
                        lambda name: calls.append(name) or "")

    assert aum_orchestrator._gae_url() == "https://ga10.test"
    assert calc_hashes.ga10_backend_url() == "https://ga10.test"
    assert calls == []
