# recon-mcp — package `theme:analytics`

Branch: `proposal/recon-mcp-theme-analytics-970`
Commit: `0252cf4`

## The screening, confirmed

`recon_engine.py:813` did say exactly:

```python
results = await asyncio.gather(*[_ytw_with_call(i, d) for i, d in call_dates.items()])
```

It was the tail of a **callables-only** fan-out: for each bond with a `call_date`
in `bond_reference`, one `/api/v1/bond/analysis` with `overrides={call_date,
call_price: 100}`, and only `analytics.ytw` was kept. Non-callable bonds were
never fanned out at all. So the endpoint item 970 wants is already being called
from this function — just for the wrong subset, keeping one field of ten.

## Grouping: one defect, not one item

The whole package is **one defect**: `recalc_with_bbg_prices` reads a single
batched FLDS endpoint and treats it as if it returned everything. It does not.
It returns `accrued_interest`, `yield` (one, convention-aware), `duration`,
`spread`, `day_count` — no split ytm/ytw, no convexity, no pvbp, no
dirty_price, no accrued_days, no ytal, no conventions block. Everything the
package complains about is a field that is *absent from the response shape
being read*, not a field that is wrong.

The corollary matters more than the fix: because `ga10_yield_worst` is fed the
FLDS convention-aware `yield`, and the recon badge compares exactly that
against BBG `yield_to_worst`, the recon's "Diff (bps)" column is a comparison of
**two different yield conventions** for any bond where GA10's convention-aware
pick is not YTW. That is not something a lane may silently correct — it changes
a yield a client reads (`v_athena_bbg_yield.athena_ytconv`, `diff_bps_ytconv`,
`convention`). **The fix is to supply the missing field and record the gap, not
to swap the comparator.**

## What I changed

`recon-mcp/recon_engine.py` — `recalc_with_bbg_prices`

- Replaced the callables-only fan-out with a **whole-book fan-out**: one
  `/api/v1/bond/analysis` per bond, `settlement_date = settle_t0`, no overrides,
  except callables which still get `{call_date, call_price: 100}`. Same number
  of round-trips as before plus the 22 non-callable bonds (26 total per the
  item's own count), one `asyncio.gather`, bounded by one 30s timeout.
  Side benefit: the 4 callable bonds that previously came back unenriched now
  get the full analytics dict.
- New `recon_calcs` columns: `ga10_ytw_explicit` (GA10's own `analytics.ytw`),
  `ga10_amortizing_yield` (GA10's convention-aware pick, labelled from
  `is_amortizing`), `ga10_dirty_price`, `ga10_accrued_days`,
  `ga10_is_amortizing`, `ga10_ytw_delta_bps` (comparator − explicit, bps),
  `convention_used_detail` (the whole conventions block).
- New `athena_bbg` columns: `dirty_price`, `dirty_price_c1` — **per 100, not
  par-scaled**, stated in the code and the SQL because every other money column
  on that table is par-scaled dollars and mixing the two is how a price starts
  looking like cash.
- `recon_db.py` — `store_calcs` and `store_athena_bbg` forward the new fields.
  `store_athena_bbg`'s docstring and a new `COMMENT ON COLUMN` pin down what
  `ga10_yield_worst` means and what it is not.

`recon-mcp/sql/007_recon_diagnostic_analytics.sql` — **prepared, not applied.**
Adds the columns above and one new view, `v_athena_bbg_yield_diag`, which
carries both yields side by side, a `comparable` boolean (|delta| < 0.01bp) and
`accrued_days_delta`. It deliberately does **not** touch `v_athena_bbg_yield` —
that view feeds the recon badge and its columns are client-visible.

**Nothing client-facing moved.** No new endpoint, so no `routes/` module; the
existing `/recalc/portfolio` path in `app.py` is untouched and is unaffected by
the new columns (PostgREST ignores keys it is not given).

## Item ids

**FIXED: 970.** The fan-out exists, stores the specified fields, and the call
override is preserved and extended to the whole book. Tests pass.

**NOT in this package, and why.** The merged job text on 970 also carries
[#980] — GA10 `wal_date = 2040-02-13` vs BBG Average Life `2039-02-10` for
ADCOP — and sibling amortiser-analytics symptoms. **980 is not fixed here, and
I am deliberately not claiming it:** the 1-year-3-day disagreement is a *value*
produced inside GA10's WAL/YTAL calculation, and no amount of fanning out
changes a number the engine computed. It needs a re-derivation against BBG's
Average Life definition on the GA10 side. I did not have the full #980 text
(it is truncated in my brief and lives in codebase-mcp), so I am not filing a
card for it either — filing an id I cannot write a correct card for files
nothing, and the next lane will have the item in front of it.

## Tests run

```
python3 -m pytest tests/ -q   →  60 passed, 5 warnings in 1.15s
```

Warnings are pre-existing (`datetime.utcnow()` deprecation, FastAPI `on_event`).
No test in the repo covers `recalc_with_bbg_prices` (it is network-bound to GAE),
so the change is covered by: full suite green, `py_compile` clean on both files,
and the two pre-existing pyflakes findings in these files unchanged.

## Needs a human — do not let a lane close this

**REVIEW REQUIRED (client-facing number).** `ga10_yield_worst` is the comparator
the recon badge diffs against BBG `yield_to_worst`. It is fed GA10's FLDS
convention-aware `yield`, while GA10's per-bond `analytics.ytw` is available and
for some bonds will differ. Which one is the *correct* BBG comparator is a
convention decision on a yield a client reads — Andy's standing rule, so I have
left the existing behaviour exactly as it was and recorded the gap in
`ga10_ytw_delta_bps` / `v_athena_bbg_yield_diag` for independent review. I did
not raise a `DECISION:` card: I have not measured the gap, so I cannot write a
card with an honest `recommend` or `default`, and the queue refuses a card whose
default proceeds when money a client reads is in play.

Suggested next step for a Claude review session: apply `sql/007`, run
`POST /recalc/portfolio?portfolio_id=wnbf&date=<d>`, then read
`v_athena_bbg_yield_diag WHERE comparable IS FALSE` — that is the population
where the comparator and GA10's explicit ytw disagree, sized in bps.

**Also for review:** `ga10_accrued_days` (GA10's own ACT count) vs
`athena_bbg.days_accrued` (the BBG-style ACT count the recon stores, computed in
`recon_engine.py` from `bond_cashflow_schedule`). `accrued_days_delta` in the
new view makes the known stub-period disagreement per-bond instead of
hypothetical. It confirms a suspicion; it does not resolve it.

<!-- lane-result
FIXED: 970
ALREADY_FIXED: none
DECISION: none
-->
