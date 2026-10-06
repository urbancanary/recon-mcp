# recon-mcp 5414 — re-land handoff-3061 with the auth-mcp URL off the event loop

Verdict: **FIX**.

## What the tree has right now

Handoff-3061 (lane `auto-recon-mcp-handoff-3061-09211012`, committed `55906e7`,
reviewed `8249aa6`, approved by Andy "yes to 47") was landed as `294be51` and
reverted 11 minutes later as `4b42be3` — after the Railway deploy the service
answered `/health` once (uptime 7s) and then timed out for 10+ minutes
(05:51–05:59Z). The revert message names the suspicion verbatim: *"lazy
`get_service_url` (sync requests) called inside async paths."*

So the tree today is the **pre-3061** state. The retired host is still a literal
in four modules:

- `aum_orchestrator.py:149` — `_GAE_URL = "https://future-footing-414610.uc.r.appspot.com"`
- `calc_hashes.py:29-32` — same host as the `GA10_BACKEND_URL` **default**
- `static_validation.py:63` — same host, module-level
- `recon_engine.py:261,756` — function-local literals in the GA10 call sites

This is not already fixed, and it is not a decision — the approval stands, the
revert was on availability grounds only, and the item tells us the shape of the
acceptable fix.

## The defect 5414 adds on top of 3061

`auth_client.get_service_url()` is **synchronous**: `_fetch_from_auth` calls
`httpx.get(...)` on the calling thread (`auth_client.py:51`). In 3061 it is
reached from `async` code — `aum_orchestrator._gae_url()` inside `_ga10_batch`,
`calc_hashes.ga10_backend_url()` inside `fetch_engine_version`, and the startup
backfill's recalc path. On a slow or hanging auth-mcp lookup that call blocks the
event loop, and every request queues behind it — which is exactly the observed
signature: one early `/health` 200, then nothing.

The fix that closes it, per the item: **resolve the URL once at startup in a
thread (`asyncio.to_thread`)** so that every later resolver call is an in-process
cache hit and never touches the network from the loop.

## What I am changing (one change, 5414)

1. **Re-land the approved 3061 bytes** (`git revert 4b42be3`): the four modules
   resolve the host *env override → `get_service_url("GA10_PRICING_URL")` →
   fail/blank loudly*, replacing the appspot literals. This part is byte-identical
   to what Andy approved; the revert was red, not a rejection.
2. **Add a startup prime** (`app.py`) that runs the auth-mcp resolution in a
   worker thread via `asyncio.to_thread`, populating the resolver caches
   (`recon_engine._ga10_pricing_url`, `calc_hashes._ga10_backend_url`,
   `aum_orchestrator._gae_url_cache`, plus `auth_client._cache`) before any
   request or the backfill can reach them. Failure is logged, not raised — the
   resolvers still retry lazily; the prime only removes the blocking fast-path.
3. **A test** asserting the async path does not call the synchronous
   `get_service_url` after priming (cache hit), and that the prime itself runs
   off the event-loop thread.

## Remaining asks in this item (not closed here)

- **Hold live `/health` for 10 min after a deploy.** This lane cannot deploy
  (hard limit) and cannot observe production. It needs the deploy lane / merge
  desk to hold the check after re-landing.
- **`docs/DEPLOY_REGISTRY.md` is missing recon-mcp.** A separate doc ask
  (`recon.x-trillion.com` follows main; `recon-mcp-production.up.railway.app`
  has not redeployed in 11 days). Left for its own change — this lane is the
  availability fix.
- **Re-review.** The 8249aa6 approval covers the original bytes only; this branch
  adds the startup prime, so it needs a fresh review before landing (item says
  so explicitly).

<!-- lane-result
FIXED: none
ALREADY_FIXED: none
DECISION: none
-->
