# recon-mcp — package handoff:3061

Lane `auto-recon-mcp-handoff-3061-09211012`. Branch
`proposal/recon-mcp-handoff-3061`, commit `55906e7`.

## The one defect, and why it is one defect

Every symptom in item 3061 is the same line of code wearing five filenames:
**recon-mcp still names the retired GAE host `future-footing-414610.uc.r.appspot.com`
as a literal, instead of asking auth-mcp for `GA10_PRICING_URL`.** Backlog 2551
fixed that in the four etf-scraper tools and stopped there; the grep was never
widened to the other repos, so recon-mcp kept the old host in four modules.

There is no second cause to group. Five sites, in three shapes:

| shape | sites | why it was left |
|---|---|---|
| function-local literal | `recon_engine.py` 309, 756 | each function used the file's *other* loader (`_ga10_pricing()`, line 35) everywhere else and hardcoded the host only for the GA10 call |
| module-level literal | `static_validation.py` 63, `aum_orchestrator.py` 149 | an import-time URL, so it could not use auth-mcp without making the module unimportable when auth-mcp is unreachable |
| literal in an `os.environ` **default** | `calc_hashes.py` 31 (`GA10_BACKEND_URL`) | looks like a config fallback, so a grep for "hardcoded URL" reads past it |

One of the three is materially worse than the other two, and it is the one the
item text lists last:

- `calc_hashes.ga10_backend_url()` feeds `fetch_engine_version()`, whose
  `hash` is stamped on every `recon_calcs` row and is exactly what the staleness
  cron compares against for engine-drift detection. A hardcoded default there is
  not just an old address — **if auth-mcp ever repoints `GA10_PRICING_URL`, the
  literal keeps reporting the OLD engine as "current" and engine-drift stops
  firing, silently.** A missed recalc and a missing alarm at the same time. That
  is the producer-side reason to fix this at the URL, not per call site.

## What changed

All four modules now resolve the host the way the rest of the estate does:
**env override → `get_service_url("GA10_PRICING_URL")` from auth-mcp → fail
loudly.** No `GA10_API_URL` (that key does not exist — confirmed absent from
auth-mcp, so the app ticket's suggested name would have resolved to nothing).

- `recon_engine.py` — `_calc_admin_analytics(prices, price_date, gae_url)` now
  takes the URL as an argument; both call sites pass `_ga10_pricing()`, the lazy
  loader already in the file. `recalc_with_bbg_prices` resolves it once per call
  into `GAE_URL`, so the ~26-request per-bond fan-out at line ~900 still reads
  the same local name. The item says `_ga10_pricing()` was unused — it was: this
  is now its first caller.
- `static_validation.py`, `aum_orchestrator.py` — module-level `_GAE_URL`
  replaced by a lazy `_gae_url()`, same shape as `recon_engine._ga10_pricing`
  including the cache-on-first-success global, so an auth-mcp blip does not
  poison the value for the process lifetime. Resolved per call; `get_service_url`
  caches internally so the cost after the first call is a dict lookup. Both are
  only ever reached from `async` code (`static_validation.validate` via app.py,
  `_ga10_batch` in the AUM path) — no sync caller that could not afford it.
- `calc_hashes.py` — the appspot **default** is gone. `GA10_BACKEND_URL` survives
  as an explicit env override (a deploy that sets it still wins); otherwise
  auth-mcp. Returns `""` when neither is set, which `fetch_engine_version` already
  treats as "skip this layer" — a missing engine hash is not a missing price and
  must not raise.

**Same host before and after.** Verified in this clone: `GA10_PRICING_URL` from
auth-mcp resolves to `https://hopper.x-trillion.com`, i.e. the current GA10
backend, so no GA10 call changes destination. **No client-facing number moves** —
no price, yield, spread, duration, NAV, cash or P&L changes value or column.
No table, no migration, no SQL file. No new endpoint, so nothing went near
`server.py`; no cache, lock or client was copied between modules.

## Ids

- **3061 — FIXED.** All recon-mcp sites the item names:
  `recon_engine.py` (286, 687, 800 in the item's numbering / 309, 756, ~900 here),
  `aum_orchestrator.py:149`, `static_validation.py:63`, `calc_hashes.py:31`.
  `grep -rn "appspot.com\|future-footing" --include=*.py .` now returns only the
  three explanatory comments and nothing executable.
- No other item id is in this package — the package is 1 open item and my slice
  is that item. Nothing was deliberately left unfixed inside recon-mcp.

## Deliberately not in this branch

- **Other repos.** The item's own grep found live appspot literals outside
  recon-mcp: `ga10-perf-mcp/server.py:33`,
  `ga10-perf-mcp/calculate_ytd_performance_with_accrued.py:24`,
  `ga10-perf-mcp/tools/performance_analytics.py:26`,
  `google_sheets_mcp/sheena_xtrillion_api.py:29`. A branch in recon-mcp cannot
  fix them; handed off below. They carry **no item id of their own** — they are
  bodies of text inside 3061, not items, so nothing can be closed by fixing them,
  which is why they are handoffs and not `FIXED`.
- **`james_mcp/tools/ga10_portfolio.py:22`** — `GA10_API_BASE_APPENGINE`, an
  explicitly-named last-resort fallback. The item says leave it; I verified it is
  named as such and left it.
- **`fixed_income_mcp/test_*.py`, `debug_cashflow_response.py`** — test/debug
  scripts, ranked lower by the item. Not touched.
- **BigQuery project ids** that contain `appspot` — not service URLs.

## Tests

`python3 -m pytest tests/ -q` → **69 passed**.

- `tests/test_admin_analytics_columns.py` was updated: its `_calc()` helper called
  `recon_engine._calc_admin_analytics(prices, date)` and now passes the third
  argument explicitly (`"https://ga10.test"`) rather than resolving through
  auth-mcp, so the test asserts the FLDS column mapping without depending on the
  estate's auth service. No assertion changed.
- `py_compile` + import of all four modules.
- Both resolution legs exercised manually: with `GA10_PRICING_URL` set, the env
  override wins; with it unset, all three accessors (`calc_hashes.ga10_backend_url`,
  `static_validation._gae_url`, `aum_orchestrator._gae_url`) return the auth-mcp
  value — verified live against auth-mcp in this environment.

## Needs a human

Nothing blocking. One fact worth recording, because it is now load-bearing: the
engine-drift cron's correctness depends on auth-mcp's `GA10_PRICING_URL` being
current. It was already the value `recon_engine` and `recon_db` used for the
price path, so the estate has a single source for this host now rather than two —
but if that key is ever stale, engine-drift detection follows it. That is the
correct behaviour (one source of truth) and is why the literal had to go, not a
new risk introduced here.

<!-- lane-result
FIXED: 3061
ALREADY_FIXED: none
DECISION: none
-->

<!-- lane-handoffs
item: 3061
repo: ga10-perf-mcp
change: Replace the appspot literals with get_service_url('GA10_PRICING_URL') (not GA10_API_URL — that auth-mcp key does not exist): server.py:33, tools/performance_analytics.py:26 (default param), calculate_ytd_performance_with_accrued.py:24. Use a lazy accessor rather than an import-time constant, per tools/verify_bond.py in etf-scraper.
-->

<!-- lane-handoffs
item: 3061
repo: google_sheets_mcp
change: Replace the appspot default param at sheena_xtrillion_api.py:29 with get_service_url('GA10_PRICING_URL'); the appspot host is no longer the GA10 backend, so the default silently points at a retired service.
-->
