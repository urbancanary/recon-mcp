# Stored bond analytics history — root cause (Part B) then remediation (Part A)

Investigation date: **2026-08-12**. Scope: `bond_analytics_dated` in the bond-data Supabase
project `xdgicslrdudsqlsudsgv`, measured against the GDBFT (`gdbft`) bond universe from
orca `/api/transactions`.

Nothing in this document has been executed. No database was modified, no engine code changed.

**Read before acting:** `/Users/andyseaman/.claude/CLAUDE.md` and
`/Users/andyseaman/Notebooks/mcp_central/CLAUDE.md`. The rules that bind every step below are
called out inline: never override QuantLib; the four-tier static model and the frozen-calc
rule; auth-mcp for every key/URL/model id; check the Database Register before creating a
table; back up before any destructive change; commit what you change.

---

## Executive summary — what actually turned out to be true

The brief's headline numbers were measured correctly but three of them do not mean what they
appear to mean. Two are artifacts, and the one genuine defect is narrower and more fixable
than assumed.

| Brief's finding | Verdict | What it actually is |
|---|---|---|
| "Only 34 of 59 bonds have any row; 25 have none" | **Artifact** | The 25 are 24 share-class ISINs + a CASH plug. They are not bonds. Real coverage is **34 of 34 = 100%**. |
| "1,456 rows, 2024-10-31..2026-08-11, 453 distinct dates" | **Artifact** | 452 of the 453 dates belong to **one** bond (US71654QDD16). The other 33 start 2026-07-09 or later. The fund's first trade is 2026-07-13. There is no deep history to repair — max depth is 18 business days. |
| "`calc_engine_versions` NULL on 66% — schema evolution?" | **No — writer asymmetry, and it is by design** | NULLs are *recent*, not old. Stamping is a hopper-only behaviour; the NULL is deliberately used as the "needs recompute" signal. |
| "The GA10 calc path ignores stored conventions, so stored rows carry wrong numbers" | **Half true — and the important half is good news** | The defect is real but is **confined to one endpoint**. The endpoint that writes `bond_analytics_dated` is correct. **Stored history is not contaminated.** |
| "provisional_static concentrated in AA Bond Co" | **Confirmed, cause established** | Point-in-time stamp of `bond_identity.day_count_validated` at write time. |

The single most consequential finding is the fourth. It is set out in **B4** and it changes the
shape of Part A completely: the recompute population is small and the urgency is on the
*validation* path, not on the stored data.

---

# PART B — Why the gaps exist

## B1. The "25 missing bonds" are not bonds

**Claim:** 34 of 59 coverage is a measurement artifact. Actual coverage of the traded bond
universe is 34/34.

**Evidence.** `GET {ORCA_MCP_URL}/api/transactions?portfolio_id=gdbft` returns 211 rows:
124 `FX_FORWARD`, 46 `BUY`, 25 `SUBSCRIPTION`, 14 `SELL`, 1 `COUPON`, 1 `PLUG`.
Excluding `FX-` prefixed ISINs leaves 59 distinct ISIN strings — the brief's 59.

Of the 25 with no analytics row:

- **24 are Irish share-class ISINs** appearing *only* on `SUBSCRIPTION` rows, with tickers that
  name them outright: `IE0009UOUDD4` "Class C USD Acc", `IE000PWKC9L5` "Class Y USD Acc",
  `IE0004EKQAB5` "Class F GBP Hedged Dist", `IE0002REEQV4` "Class C EUR Hedged Dist", and 20
  more of the same form. These are units in the fund, not holdings of the fund.
- **1 is `CASH`**, a single `PLUG` row, description "CASH PLUG — subscriptions / FX fwds / fees
  (unattributed)".

Restricting to ISINs carrying a `BUY` or `SELL` gives exactly **34** ISINs, and the set of
ISINs present in `bond_analytics_dated` is **exactly equal** to that set (verified by set
equality, not by count).

**Root cause:** the universe query counted every non-FX ISIN on the transaction table rather
than every ISIN on a *trade*. Share classes and the cash plug ride the same column.

**Consequence for Part A:** there is no "25 never-enrolled bonds" backfill to do. Any plan
built on that premise would have spent its budget enrolling share classes into a bond pricing
pipeline.

**Forward fix:** the universe helper should filter on `transaction_type IN ('BUY','SELL')`
rather than on `NOT isin LIKE 'FX-%'`. Worth noting that `gdbf` (without the `t`) returns
`count=0` *silently* — an easy way to conclude "no data" when the id is simply wrong.

## B2. There is almost no history, because the fund is four weeks old

**Claim:** the 2024-10-31 start date and 453 distinct dates describe one bond, not the book.

**Evidence.**

- GDBFT `BUY`/`SELL` transaction dates run **2026-07-13 .. 2026-08-06**; settlement
  **2026-07-14 .. 2026-08-06**. The fund began trading on 2026-07-13.
- `US71654QDD16` (PEMEX 7.69% 2050) holds **554 rows across 452 distinct price_dates**,
  2024-10-31 .. 2026-08-11 — i.e. **452 of the 453 dates in the whole table for this universe**.
  It has that history because it is an ETF-universe bond that was already being priced
  (`bbg_bdh` 380 rows, `etf_cron`, `carried_forward`) long before GDBFT existed.
- Every other bond's first row is 2026-07-02 or later; **28 of 34 start on 2026-07-17**.
- Per-bond depth after first appearance, against the 18 business days 2026-07-17..2026-08-11:

| Bond | first row | missing business days after first row |
|---|---|---|
| 24 bonds | 2026-07-09..2026-08-04 | **0** |
| `FR001400SMR0`, `GB00BBQ33664`, `US912810UU06`, `USF1067PAK24`, `USF8500RAF94`, `XS2393498204`, `XS2485268150`, `XS2731297235`, `XS3191631079`, `XS3221827911`, `XS3404445663` | 2026-07-17 / 07-31 | **1** — all the same day, **2026-08-03** |
| `FR001400L834`, `IT0005560948` | 2026-07-09 | 12 (see B6) |
| `US91282CJK80` | 2026-07-17 | 9 (see B6) |

- **Zero weekend price_dates** across all 34 bonds. The weekend guard
  (`ga10-pricing-mcp/src/index.js:10233`) works.
- `US71654QDD16` has exactly **one** gap >4 days in 22 months: 2026-07-08 → 2026-07-16.

**Root cause:** history begins at fund inception. This is correct behaviour, not a gap.

**Consequence for Part A:** the maximum possible backfill for GDBFT is 34 bonds × ~18 business
days. Any "recompute the history" project is hours of engine time at most, not days — and most
of it is already present.

## B3. `calc_engine_versions` NULL is writer asymmetry, and the NULL is a deliberate signal

**Claim:** the field was not added late. It is stamped by one family of writers and not by
another, the unstamped family is the *newer, higher-volume* one, and the hopper treats a NULL
stamp as its own trigger to recompute.

**Evidence — the NULLs are recent, not old.** Distribution by `price_date` month:

```
2024-10 .. 2026-04   ga11:37cc06 only        (~20-44 rows/month, zero NULL)
2026-05              ga11:37cc06 14, ga10 1, NULL 5
2026-06              NULL 24
2026-07              NULL 482, ga11:93f0ab 13
2026-08              NULL 448, ga11:93f0ab  9
```

A column added late would produce the opposite: NULL in the old rows, stamped in the new.

**Evidence — NULL tracks the writer.** By `source`:

| source | rows | stamped | NULL | created_at |
|---|---|---|---|---|
| `bbg_bdh` | 380 | 380 | 0 | 2026-04 |
| `scheduled_job` | 371 | 103 | 268 | 2026-04 .. 2026-08 |
| `admin` | 476 | 4 | **472** | 2026-08 only |
| `NAV_REPORT` | 213 | 4 | **209** | 2026-07 .. 2026-08 |
| `bbg_index_file` | 6 | 1 | 5 | 2026-07 |
| `etf_cron` | 4 | 0 | 4 | 2026-05..06 |
| `carried_forward` | 5 | 4 | 1 | 2026-05 |

**Evidence — from the code (all paths verified by reading, citations from the writer sweep):**

*Writers that stamp* (all hopper / UI recompute paths, all GA11):
- `ga10-pricing-mcp/src/hopper.js:3147` — `stageCalc`, blob built at `:3109`
- `ga10-pricing-mcp/src/hopper.js:3430` — `stageHistoryFill`, blob at `:3360`
- `ga10-pricing-mcp/src/index.js:3952` — `/hopper/fill-gaps`

*Writers that do not stamp* (every first-write path):
- `ga10-pricing-mcp/src/index.js:10305` — `syncAnalyticsToSupabase` datedRow (`:10305-10367`),
  **the main daily cron**; covers `scheduled_job` (via `processSingleBond` `:6968`, label
  `:6987`, through `storeBondAnalytics` `:9544`), `etf_cron` (via `runEtfAnalytics` `:6671`,
  label `:6760`) and the `bbg_*` sources. Upsert at `:10369`.
- `ga10-pricing-mcp/src/index.js:7776` — `backfillExtraCbondsPrices`
- `ga10-pricing-mcp/src/index.js:9330` — `recalcHistory` PATCH payload
- `ga10-pricing-mcp/scripts/backfill_v2/batch_backfill.py:656` (`bbg_bdh`, local QuantLib job)
- `ga10-pricing-mcp/scripts/backfill_bbg_analytics.py:36` (`bbg_bdh`, via `/prices/calculate`
  → `handleCalculateAnalytics` `src/index.js:11183`)
- **`recon-mcp/recon_engine.py:341`** `_store_admin_prices_to_bond_data`, POST at `:411`,
  called at `:1443` — this is *our* writer, and it is the source of the `admin` rows
- `boerse_frankfurt_mcp/src/writer.py:41` (source `deutsche_boerse`, not in this universe)

Two source labels have **no writer anywhere in `mcp_central`**:

- **`NAV_REPORT` (213 rows).** Swept recon-mcp, athena_mcp, athena_portfolios, bond_data_mcp,
  orca_mcp and ga10-pricing-mcp — the literal appears only as a *source-priority rank* in view
  SQL (`migrations/004:240-245`, `007:178`, `008:146`, `009:167-171`, `010:190-194`, where it
  ranks **above** `scheduled_job`) and a comment in `024:25`. Given `created_at` 2026-07/08
  alongside the `admin` rows, an ad-hoc or out-of-tree recon-mcp/Maia NAV import is likeliest —
  **but this is not established**, and it matters for A8a: we cannot add a stamp to a writer we
  cannot find.
- **`bbg_index_file` (6) and `BBG` (1).** Almost certainly the `/prices/calculate` path, which
  passes the caller's `source` straight through (`src/index.js:11372` names "BBG sheet upload,
  bbg_bdh backfill" as its two clients). Which client sent them is not established.

`carried_forward` (5 rows, all 2026-05) has **no live writer** — the write was retired
(`src/index.js:6991-7007`, "no fresh CBonds price, skipping write … See backlog id=480");
remaining code only reads or DELETEs it. Those rows are residue.

**Evidence — the NULL is intentional.** `ga10-pricing-mcp/src/hopper.js:3214-3235`:
`stageHistoryFill` switches to `fullRecalcMode` when any price-bearing row has
`calc_engine_versions IS NULL`. The comment at `:3214` dates the decision to 2026-05-23.

**Root cause:** rows are born unstamped by design and become stamped only once the hopper has
recomputed them. So "66% NULL" does not mean "66% untrustworthy" — it means **"66% of these
rows have not yet been through a hopper recompute"**, i.e. it is a measure of *backlog depth*.
GDBFT is worse than the estate (66% vs 25% universe-wide, see B7) purely because its rows are
all a few weeks old and were written by the two paths that never stamp.

**Caveat I could not settle:** no migration in `ga10-pricing-mcp/migrations/` (001-036) adds
`calc_engine_versions` to `bond_analytics_dated`. The only DDL for that column name is
`migrations/024_bond_analytics_per_source.sql:47` and that is a **different table**
(`bond_analytics_per_source`). The column appears to have been added by ad-hoc SQL applied
straight to Supabase. **What would settle it:** `SELECT attname, attnum FROM pg_attribute`
ordering, or the Supabase migration history, for `bond_analytics_dated`.

## B4. The engine defect is real, unconditional — and confined to one endpoint. Stored rows are clean.

This is the finding that reshapes the plan, so it is evidenced in three independent ways.

### B4a. The defect is real and cannot be worked around by passing conventions

`bond_identity` for Vodafone `XS2630493570` holds correct, locked, cbonds-sourced static:
`coupon 8`, `day_count ActualActual.ISMA`, `frequency 1`, `maturity 2086-08-30`,
`day_count_validated true`, `locked true`.

Calling `POST {GAE}/api/v1/bond/analysis/flexible` three ways:

| call | accrued | days | coupon | day_count | frequency |
|---|---|---|---|---|---|
| bare `{isin, price, settlement_date}` | (non-JSON error) | — | — | — | — |
| **+ explicit `coupon/day_count/frequency/maturity_date` at top level** | **3.044444** | 137 | None | Thirty360 | 2 |
| **+ same fields nested under `overrides`** | **3.044444** | 137 | None | Thirty360 | 2 |

Passing the correct conventions explicitly changes **nothing**. The endpoint returns
`Thirty360` / `frequency 2` / `coupon None` regardless. Trade confirm for the same settlement
(2026-07-15) is **6.991781** = 8.000 × 319/365. So the defect is unconditional on this
endpoint, not a caller error and not a missing-parameter problem.

`/api/v3/...` shares the handler (`google_analysis10_api.py:4767` routes v3 into
`bond_analyze_flexible()`), so it inherits the defect.

### B4b. The sibling endpoint is correct to six decimal places

`POST {GAE}/api/v1/portfolio/analysis`, one bond per call, against the settled-trade confirms:

| ISIN | settle | portfolio/analysis accrued | trade confirm | |
|---|---|---|---|---|
| `XS2630493570` Vodafone | 2026-07-15 | 6.9917808219178035 | 6.991781 | **match** |
| `XS2731297235` SW Finance | 2026-07-15 | 4.344178082191785 | 4.344177 | **match** |
| `XS2819228664` PIC/Athora | 2026-07-15 | 4.55821917808219 | 4.558219 | **match** |
| `DE0001030757` Bund | 2026-07-15 | 1.647123287671226 | 1.647123 | **match** |

Same engine host, same bonds, same dates. **The discriminator is the endpoint, not the static
and not the engine.** This is exactly the pairing the brief describes as impossible to detect
by hash — and it is detectable, just not by hash.

### B4c. The stored rows carry the *correct* numbers

Two independent checks.

*Direct read, Vodafone.* Stored `bond_analytics_dated` accrued: 2026-07-17 → 7.03562
(= 8 × 321/365 = 7.035616), 2026-07-24 → 7.18904 (= 8 × 328/365 = 7.189041). The day-over-day
increment is 0.02192 = 8/365 exactly. **ACT/365 annual is being applied** — the correct
convention, not the buggy Thirty360/semi. This holds for `admin`, `NAV_REPORT` *and*
`scheduled_job` rows alike.

*Systematic, all 34 bonds.* For each bond, the median empirical daily accrual increment from
consecutive stored rows, versus its own `bond_identity` conventions. Every bond is consistent
with its own stored static:

- 5 annual ACT/365 bonds land on coupon/365 to 1e-5 (`XS2630493570` 0.021920 vs 0.021918;
  `XS2731297235` 0.020207 vs 0.020205; `XS2819228664` 0.018837 vs 0.018836; `FR0014015MU5`,
  `XS3191631079`)
- 7 US 30/360 bonds land on coupon/360 (`US71654QDD16` 0.021360 vs 0.021361;
  `US29273VBH24` 0.018750 vs 0.018750; `US674599DH56`, `US458140CK47`, `US71647NAA72`,
  `US87927VAV09`, and the two BNP/SocGen perps)
- The rows that fall on neither are all **semi-annual ActualActual**, where the correct daily
  rate is coupon/2 ÷ actual days in the period, not coupon/365. Spot-checked:
  `XS2580220171` 8.45 semi ActAct → 4.225/184 = 0.0229619; stored **0.022962**.
  `XS2059770409` 8.125 semi → 4.0625/183 = 0.02220; stored **0.022200**. Correct.

**Root cause:** `bond_analytics_dated` is written by paths that use the *portfolio* endpoint or
the GA10 gateway, never by `/api/v1/bond/analysis/flexible`. Endpoint usage across the tree:

| endpoint | callers |
|---|---|
| `/api/v3/bond/analysis` (gateway) | `fetchGA10Analytics` (`src/index.js:8578,8583,8622,8630`) — `scheduled_job`, `etf_cron`, `/prices/calculate` |
| `/api/v1/portfolio/analysis` | `recalcHistory` (`src/index.js:9087-9105`); **recon-mcp `recon_engine.py:309`, `:707`**; `aum_orchestrator._ga10_batch:169` |
| GA11 `/api/v3/bond/history` | hopper stageCalc / stageHistoryFill / fill-gaps, GA10 `history-analysis` fallback at `hopper.js:3298-3305` |
| `/api/v4/bond/analysis` | boerse_frankfurt `src/enricher.py:31,64` |
| **`/api/v1/bond/analysis/flexible`** | **exactly one caller in the entire `mcp_central` tree, and it is not a writer: `recon-mcp/static_validation.py:119-133`, `_ga10_accrued()`** — the validator itself |

That last row was verified by a full-tree sweep, not just this repo. **No writer of
`bond_analytics_dated` touches the defective endpoint.**

**Consequence for Part A, stated plainly:** the 51 findings that `static_validation.validate('gdbft')`
returns are **mostly an artifact of the validator calling the broken endpoint**, not evidence
that stored analytics are wrong. The bug to fix is in the *validator's* engine call. There is
**no large recompute** implied by B4. Fixing the endpoint (or repointing the validator) is a
code change whose effect is to make the regression test meaningful — it does not, by itself,
require touching a single stored row.

**Caveat I could not settle:** the daily cron reaches GA10 through the gateway Worker at
`/api/v3/bond/analysis` (`ga10-pricing-mcp/src/index.js:8578-8630`, `fetchGA10Analytics`), and
`/api/v3` is said to share the defective handler. Yet the `scheduled_job` rows measure as *correct* (B4c). Either the gateway
rewrites to a different upstream route, or the App Engine `/api/v3` differs from the one the
brief cites. **What would settle it:** call the gateway URL directly for `XS2630493570` at
2026-07-15 and compare to 6.991781; and read the gateway Worker's route map. Until settled,
treat `scheduled_job` rows as *verified correct empirically but not yet explained*. This is the
single largest open question in this document and A2 is written to resolve it before anything
else.

## B5. `provisional_static` is a point-in-time stamp of `day_count_validated`

**Claim:** confirmed, with the mechanism identified but the trigger itself unverifiable from
the repo.

**Evidence.** 200 provisional rows across 22 ISINs in the GDBFT universe. Cross-referencing
`bond_identity.day_count_validated` as it stands today:

- `day_count_validated = false` for exactly 7 GDBFT bonds: `DE0001030757`, `GB00BBQ33664`,
  `GB00BPCJD997`, `IT0005534141`, `US912810UU06`, `US91282CJK80`, `US91282CKJ98`.
- Those bonds are **100% provisional**: `DE0001030757` 26/26, `GB00BBQ33664` 17/17.
- `XS2580220171` (AA Bond Co) is 28/30 provisional but reads `day_count_validated = true`
  *today*. So it flipped to validated after most of its rows were written. Same for
  `XS2731297235` (22/26 provisional, now validated).

That asymmetry — a bond whose rows are provisional but whose identity is now validated — is
what proves the field is stamped **at write time from the then-current
`bond_identity.day_count_validated`**, not maintained. The brief's AA Bond Co concentration is
therefore expected: it was unvalidated for most of the window and has since been validated by
the hopper.

`day_count_validated` is written by the hopper: set `true` on a genuine `validate_static` pass
(`ga10-pricing-mcp/src/hopper.js:6748`), `true` + `locked` on reaching `complete` (`:6768`),
and deliberately **not** stamped on the unverified advance (`:6710`).

**Caveat I could not settle:** the trigger `trg_stamp_provisional_static` **does not exist
anywhere in `/Users/andyseaman/Notebooks/mcp_central`** (swept `.sql`/`.py`/`.js`/`.ts`/`.md`,
excluding `node_modules`/`.git`). The only literal `provisional_static` in any repo is
`boerse_frankfurt_mcp/src/writer.py:71`, which hardcodes `False`. Likewise **no writer sets
`calc_hash` on `bond_analytics_dated`** — every code path only *reads* it — yet 1,334 of 1,456
rows have one. Both columns are therefore almost certainly populated by database-side
triggers/generated columns that exist only in the live Supabase instance and are **not in
version control**. **What would settle it:** `SELECT tgname, pg_get_triggerdef(oid) FROM
pg_trigger WHERE tgrelid = 'bond_analytics_dated'::regclass` and the matching
`pg_get_functiondef`. This is itself a finding — see A7.

## B6. Other gap types, counted

Everything below was measured; none of it was in the brief's list.

**a) Duplicate `(isin, price_date)` — 374 keys, 483 excess rows. Not a defect.**
The real key is `(isin, price_date, source, provider_detail)` — confirmed both by the upsert
`on_conflict=isin,price_date,source,provider_detail`
(`ga10-pricing-mcp/src/index.js:10369`) and empirically: **zero** duplicates on
`(isin, price_date, source)`. Per-source rows are the design
(`migrations/024_bond_analytics_per_source.sql`). Any query that assumes `(isin, price_date)`
is unique will silently double-count — 973 logical (isin,date) keys are represented by 1,456
rows.

**b) `has_null_analytics` — 398 rows, 27% of the universe.** Overwhelmingly `admin`
(395 rows); 3 are `bbg_index_file`. These are recon-mcp's price-only rows: when GA10 returns no
analytics, `recon_engine.py:384-407` logs an error, fires `alert_data_quality(...)`, and stores
the price anyway. Overall **1,075 of 1,456 rows carry both ytm and duration; 381 do not**
(378 `admin` + 3 `bbg_index_file`).

This is the most consequential real gap, because of a documented failure mode: a
price-with-NULL-accrued row wins the `v_holdings_enriched` lateral and **silently zeroes
accrued** (the `GB00BBQ33664` incident, 2026-08-11).

**c) Analytics coverage by bond.** Dates with a row vs dates with full analytics:

| Bond | dates | with ytm+duration |
|---|---|---|
| `USF1067PAK24`, `XS2485268150` | 17 | **7** |
| `FR001400SMR0`, `USF8500RAF94`, `XS2393498204`, `XS3221827911`, `XS3404445663`, `XS2731297235` | 17 | **8** |
| `XS3191631079` | 17 | 9 |
| `US91282CJK80` | 9 | 6 |
| 24 others | — | full |

The eight worst are all in the poison-bond class (B6f).

**d) 14 of 34 bonds are not enrolled in the daily pricing job at all.** Bonds with zero
`scheduled_job` rows: `DE000BU2Z072`, `FR001400SMR0`, `GB00BBQ33664`, `US912810UU06`,
`US91282CJK80`, `US91282CKJ98`, `USF1067PAK24`, `USF8500RAF94`, `XS2393498204`, `XS2485268150`,
`XS2731297235`, `XS3191631079`, `XS3221827911`, `XS3404445663`.

The cause is structural. Daily pricing enrolment is the **D1 `watchlist_cache` / `etf_watchlist`
tables**, not the hopper (`fetchWatchlist()`, `ga10-pricing-mcp/src/index.js:7243`:
`WHERE wc.is_active = 1 AND ba.isin IS NULL AND NOT EXISTS (pricing_skip_log …)`; per-bond
CBonds calls additionally gated by `isActiveCbondsWatchlistMember()` at `:7913`, which **fails
closed**). Hopper enrolment is a *different* universe — everything in
`bond_reference ∪ bond_identity` (`migrations/034_enrol_all_known_bonds.sql:18`). A bond can be
fully hopper-enrolled and still never get a daily price row, and the hopper's recompute stages
are PATCH-only — they cannot create a row that does not exist. **These 14 bonds depend entirely
on the admin/NAV file arriving.** Also note `WATCHLIST_NAMING.md`: the CBonds spend gate and the
D1 table have drifted; do not assume patching one moves the other.

There are **three silent zero-row outcomes**, which is why this never surfaced as an error:
not being on `watchlist_cache` at all (ETF-sourced bonds live in `etf_watchlist` and get
`etf_cron` instead, `runEtfAnalytics:6677`); `no_fresh_price` (`:6999-7007`); and
`perpetual_no_maturity` (`:7028-7035`). The latter two park the bond via `parkBond` (`:4533`).
**Diagnostic for the 14:** check `watchlist_cache.is_active`, then `pricing_skip_log.reason`.

**e) One missing day, 2026-08-03, across 11 bonds.** A single admin/NAV file that did not
arrive or was not ingested. Trivially backfillable.

**f) Six bonds cannot be priced by GA10 at all — and they are all perpetuals.**
Calling `/api/v1/portfolio/analysis` one bond at a time for all 34 (28.5s total, 0.84s/call):
**28 priceable, 6 fail** with HTTP 500 `"Portfolio processing error: argument of type 'NoneType'
is not iterable"` — the poison-bond signature `aum_orchestrator._ga10_batch` was written to
survive (`recon-mcp/aum_orchestrator.py:152-196`).

| ISIN | name | `bond_identity` missing |
|---|---|---|
| `FR001400SMR0` | EDF 7.375% PERP | maturity_date |
| `USF1067PAK24` | BNP PARIBAS 7.2% PERP | frequency, day_count, maturity_date |
| `USF8500RAF94` | SOCIETE GENERALE 7.125% PERP | frequency, day_count, maturity_date |
| `XS2393498204` | ROTHESAY LIFE 5% PERP | maturity_date |
| `XS2485268150` | AVIVA PLC 6.875% PERP | maturity_date |
| `XS3404445663` | LEGAL & GENERAL 7.125% PERP | maturity_date |

Six for six: **every bond missing `maturity_date` in `bond_identity` is a poison bond, and
every poison bond is a perpetual** (all six descriptions carry "PERP"). The control case
supports `maturity_date` as the discriminator rather than the missing conventions:
`XS3191631079` (ALDERMORE GROUP 6% 10/01/2035) is **also** missing `frequency` and `day_count`
but **has** a maturity date — and it prices without error. This is consistent with the cron's documented
"perpetual/undated bond" silent skip (`ga10-pricing-mcp/src/index.js:7028-7035`). It also
explains (c) and (d) — these bonds have the thinnest analytics coverage and no `scheduled_job`
rows because the engine cannot price them.

Not fully explained: `GB00BBQ33664` (NATIONWIDE 10.25% PERP) is also missing frequency,
day_count and maturity_date, yet **does** price. Its 17 rows carry a ytm but
`accrued_interest` is **NULL on every one**. Worth a look; do not assume it behaves like the
other six.

**g) `bond_identity` completeness.** `US91282CJK80` (US TREASURY 4.625% 11/15/2026) has **no
`bond_identity` row at all** — 33 of 34 bonds are present — yet it has 15 analytics rows. Nine
bonds are missing at least one of coupon/frequency/day_count/maturity_date (listed above plus
`XS3191631079`, `GB00BBQ33664`).

**h) Price staleness / `carried_forward`.** Only 5 `carried_forward` rows in this universe, all
2026-05, all on `US71654QDD16`. Not a live problem for GDBFT.

## B7. `calc_hash` drift is two different phenomena, and one of them is a bug

14 of 34 bonds show more than one `calc_hash`, as the brief measured. They split cleanly.

**Pattern 1 — genuine static corrections (5 bonds).** A one-way transition, and they cluster on
a single date, **2026-08-10**: `XS2630493570` `937d80d4`→`f2dceca9`, plus `XS2731297235`,
`XS2815887372`, `XS2819228664`, `GB00BBQ33664`. This is a real static-correction sweep and it
is corroborated by `bond_identity` itself: Vodafone's `prior_calc_hash` is
`937d80d4b392985e7e123f76549feb68` and its current `calc_hash` is
`f2dceca9704da24363df1ce153cfa810` — exactly the observed transition. These are the GBP
annual-frequency fixes described in `static_validation.py`'s docstring. **Correct behaviour.**

**Pattern 2 — flapping (6 bonds), which is a bug.** `DE0001030757` goes
`0e30d8e5`→`37ee8fe0`→`0e30d8e5`; `FR0014015MU5` `a26c1297`→`f24b6be7`→`a26c1297`; same shape
for `XS2580220171`, `IT0005534141`, `US29273VBH24`, `GB00BPCJD997`. Static does not un-correct
itself.

Decomposing by source resolves it: on **18 of 973** `(isin, price_date)` keys, two sources
disagree about the hash on the same day. Every flap is exactly one row:

```
('DE0001030757','2026-07-24')  scheduled_job 37ee8fe0  |  admin 0e30d8e5
('FR0014015MU5','2026-07-24')  scheduled_job f24b6be7  |  admin a26c1297
('US29273VBH24','2026-07-24')  scheduled_job 877814f3  |  admin da95ec00
('XS2580220171','2026-07-24')  scheduled_job b57e30b7  |  admin 67369380
('GB00BPCJD997','2026-07-27')  scheduled_job b849dddb  |  admin a4907ce6
('IT0005534141','2026-07-27')  scheduled_job 24e3b7e7  |  admin 6b427464
```

And `bond_identity` for `DE0001030757` holds `calc_hash = 0e30d8e5…`,
`prior_calc_hash = 37ee8fe0…`. **The two writers stamped different generations of the same
static on the same day** — one read `bond_identity` before a hash update, the other after. A
mid-day static edit on 2026-07-24 and 2026-07-27 raced the two writers.

There is also a clean example of correct convergence: on `('XS2630493570','2026-08-10')` the
`admin` row carries the old `937d80d4` while `NAV_REPORT` and `scheduled_job` carry the new
`f2dceca9` — the same race, on the day of the real correction.

**Root cause:** `calc_hash` is stamped per row at write time with no read-consistency against
`bond_identity`, and two writers with different schedules both stamp the same logical day.

**This directly validates the brief's warning, from the other direction.** Hash-matching is not
merely blind to engine misapplication — it also **produces false positives** from writer races.
14 bonds "changed static"; only 5 actually did. Scoping a recompute by "rows whose hash changed"
would have recomputed 6 bonds for nothing and, per B4, still missed nothing real.

## B8. Estate-wide context

`bond_analytics_dated` as a whole (exact counts via PostgREST `count=exact`):

| metric | rows | share |
|---|---|---|
| total | **197,970** | — |
| `calc_engine_versions IS NULL` | 49,119 | 25% |
| `provisional_static = true` | 65,962 | 33% |
| `has_null_analytics = true` | 24,001 | 12% |
| `calc_hash IS NULL` | 9,006 | 5% |
| `source = 'scheduled_job'` | 32,119 | 16% |
| `source = 'admin'` | 4,077 | 2% |

GDBFT is **66% unstamped against an estate average of 25%** — consistent with B3: its rows are
recent and written by the two paths that never stamp. GDBFT is 1,456 rows, **0.7% of the
table**. Any GDBFT-scoped remediation is small; any estate-wide one is 135× larger and should
be a separate decision.

---

# PART A — Remediation plan

## A0. Scope, stated plainly

Given B4, **the recompute population is not "rows affected by the engine bug", because there
are none in `bond_analytics_dated`.** The stored rows reproduce their own conventions and, where
a trade exists, the trade confirm. The brief's trap — scoping by static-hash drift — is avoided
not by choosing a better hash filter but by establishing that **no hash-based scope is needed**.

What is actually broken, in priority order:

1. **The validator calls the broken endpoint** (B4). It reports 51 findings across 26 bonds that
   are largely artifacts of its own engine call. Highest urgency, smallest change, zero data risk.
2. **381 rows carry a price and no analytics** (B6b/c), 395 of them `admin`, and a NULL-accrued
   row can silently zero accrued downstream.
3. **6 perpetuals cannot be priced at all** (B6f) because they lack `maturity_date` in
   `bond_identity`. This is a **static** problem, so per `mcp_central/CLAUDE.md` the fix is the
   static, never a patch to QuantLib's output.
4. **`calc_hash` races between writers** (B7) make drift detection unreliable in both directions.
5. **`calc_engine_versions` is unstamped on the two paths we own** (B3), so backlog depth is
   invisible.
6. **Two production DB objects are not in version control** (B5) — a trigger and whatever
   populates `calc_hash`.

Explicitly **out of scope**: any recomputation of settled trade values. Settled accrued, settled
cash and realised P&L are **Tier C, frozen** (`mcp_central/CLAUDE.md`). Everything below touches
**Tier D valuation history only**. `static_validation.py`'s own docstring already states this
rule; the plan does not relax it.

## A1. Hard dependency ordering

```
A2 (settle the gateway question)
      │
      ├──> A3 (fix/repoint the validator)  ──> A4 (re-baseline) ──┐
      │                                                            │
      └──> A5 (perp static)  ──> A6 (fill missing analytics) ──────┴──> A8 (forward fixes)
                                        ▲
                                A7 (capture DB objects) — do before ANY write
```

**The engine question must be settled before any backfill.** If A2 shows the gateway *is*
affected, then `scheduled_job` rows need re-deriving and A6 grows; if it shows the gateway is
clean, A6 stays small. Backfilling before A2 risks writing 1,456 wrong rows at scale. This is
the dependency the brief asks to be named, and it is A2 → A6, not "engine fix → backfill".

## A2. Settle the gateway question — **do this first, it is read-only**

The one thing that could still invalidate B4. Three read-only checks:

1. Call the gateway (`ga10-pricing-mcp/src/index.js:8607`, `env.GA10_GATEWAY` /
   `GA10_API_URL`, default `…/api/v3`) for `XS2630493570` at settlement 2026-07-15. Correct
   answer is **6.991781**; the defective handler returns **3.044444**. Unambiguous either way.
2. Read the gateway Worker's route map to see what `/api/v3` proxies to upstream.
3. Confirm `google_analysis10_api.py:4767` really routes v3 into `bond_analyze_flexible()` in
   the *currently deployed* revision, not just in the repo — per the house rule that repo files
   drift from prod.

**Cost:** minutes. **Risk:** none. **Decision gate:** if the gateway returns 3.044444, stop and
re-plan — `scheduled_job` rows come under suspicion and A6 becomes a real backfill.

## A3. Fix the calc path — validator first

Two options; recommend both, in this order.

**A3a (immediate, recon-mcp).** Repoint `static_validation._ga10_accrued()`
(`recon-mcp/static_validation.py:119-133`) from `/api/v1/bond/analysis/flexible` to
`/api/v1/portfolio/analysis`, the endpoint proven correct in B4b and already used by
`aum_orchestrator._ga10_batch`. Reuse `_ga10_batch`'s poison-bond halving rather than writing a
second retry loop.

- Small, in a repo we own, no DB writes, immediately reversible.
- Note the 6 perps will still 500 — they must surface as **"cannot price"**, not as a static
  finding. Conflating the two is how the current output gets to 51.

**A3b (proper, GA10).** Fix `bond_analyze_flexible()` so it loads stored conventions. B4a shows
it ignores conventions even when passed explicitly, so this is a real handler fix, not a caller
fix. Out of this repo; log it and hand it to the GA10 owner.

**Constraint:** the fix is to make the engine *receive* the right static — never to post-process
its output. `mcp_central/CLAUDE.md`, non-negotiable.

## A4. Re-baseline the regression test

After A3a, re-run `static_validation.validate('gdbft')` and record the result in this file.

Current baseline, measured 2026-08-12: **58 trades checked, 2 unchecked, 7 agree, 51 findings,
26 bonds affected, 0 excepted.** Every finding carries `coupon: null` and many carry
`our_accrued_per100: 0.0` — the endpoint is not loading the bond at all, which is why the
`ratio` is 0.0 rather than 0.5 or 1.014. That signature is itself evidence for B4.

**Expected after A3a:** findings drop sharply. It will **not** reach zero — the 6 perps cannot be
priced, and any residual genuine static defect should remain. **The acceptance criterion is
therefore: every remaining finding is either a priceable bond with a real static defect, or a
perp reported as "unpriceable".** "Goes to zero" is the wrong target while perps 500.

Findings that survive A3a are **real** and route to A5 / the hopper's `validate_static`.

## A5. Fix the perpetual static — static, not code

For the 6 bonds in B6f: source `maturity_date` (and for BNP/SocGen also `frequency` and
`day_count`) from CBonds/BBG/prospectus and write it to `bond_identity`. For a genuine perp the
right answer may be a far-dated sentinel or an explicit perp flag the engine understands —
that is a GA10 modelling question, not something to guess here. `validate-watchlist-static`
carries a perp/sinker pass; use it rather than improvising.

Also in this step: create the missing `bond_identity` row for `US91282CJK80`, and investigate
`GB00BBQ33664` (prices but accrued NULL on all 17 rows).

`bond_identity` is reference data (Tier A/B source). Correcting it flows into **today's**
valuation and display. It must **not** re-derive any settled trade value.

## A6. Fill missing Tier D analytics — the actual backfill

**Population, stated plainly:** rows in `bond_analytics_dated` for the 34 GDBFT bonds that carry
a price but no analytics — **381 rows** (378 `admin`, 3 `bbg_index_file`), plus the single
missing day 2026-08-03 across 11 bonds. **Not** scoped by hash (B7 shows hash is unreliable in
both directions). **Not** scoped by `calc_engine_versions` either, though the stamp is a useful
*ordering* heuristic since the hopper already treats NULL as its recalc trigger (B3).

**Preferred mechanism: let the hopper do it.** `stageHistoryFill`
(`ga10-pricing-mcp/src/hopper.js:3210`) already exists, already PATCHes historical rows, already
stamps `calc_engine_versions`, and already enters `fullRecalcMode` on NULL stamps (`:3229`).
Writing a bespoke backfill script would duplicate it and skip the stamping. Nudge these 34 ISINs
up the hopper queue rather than building a parallel path.

**Cost.** 28 of 34 bonds priceable. Measured **0.84s per single-bond `/api/v1/portfolio/analysis`
call**; batches are faster but the 6 perps force `_ga10_batch`'s recursive halving, so assume
worst case. Per-bond history calls (GA11 `/api/v3/bond/history`) are ~34 calls, not 1,456.

- Bounded worst case, one call per row: 381 × 0.84s ≈ **5.5 minutes**.
- Realistic via per-bond history: ~34 calls, **under 2 minutes**.
- Estate-wide equivalent would be 135× (B8) and is **not** proposed here.

The engine can take this comfortably. Two known constraints: `_ga10_batch`'s sequential halving
is deliberate — parallel fan-out overloaded GAE and dropped coverage from 21/28 to 11/28
(`aum_orchestrator.py:189-191`); and the daily cron swallows dated-upsert failures
(`ga10-pricing-mcp/src/index.js:10384`, "Don't throw"), so a silent partial failure will not
raise on its own. **Verify by re-count, not by absence of error.**

**Backups and verification — `mcp_central/CLAUDE.md`, mandatory.**

1. **Before any write**, back up the affected slice:
   `CREATE TABLE backup_bond_analytics_dated_20260812 AS SELECT * FROM bond_analytics_dated
   WHERE isin IN (<the 34>);` Expect **1,456** rows. Verify the count before proceeding.
2. Check the **Database Register** (`codebase-mcp recall("database register")`) before creating
   the backup table, and update the register after.
3. **Stepwise, not batched**: one bond first, verify, then 5, then the rest.
4. **Verification after each step**, all of which are counts we already have baselines for:
   - total rows for the 34 unchanged at **1,456** (this is a PATCH, not an insert — row count
     must not move except for the 2026-08-03 fill)
   - `has_null_analytics = true` falls from 398
   - rows with ytm+duration rises from 1,075 toward 1,456 minus the perps
   - **accrued increments still match each bond's conventions** — re-run the B4c check; this is
     the real regression guard, because it is the check that would catch a wrong-convention
     recompute
   - `static_validation.validate('gdbft')` no worse than the A4 baseline
5. **Revert path:** the PATCH is column-level on existing rows, so revert is
   `UPDATE bond_analytics_dated t SET (…analytics cols…) = (SELECT … FROM
   backup_bond_analytics_dated_20260812 b WHERE b.id = t.id)` keyed on `id`. Keep the backup
   until the next NAV cycle reconciles clean, then drop it and update the register.
6. **Never** touch `orca` transactions or any settled value. Nothing in A6 writes outside
   `bond_analytics_dated`.

## A7. Capture the undocumented DB objects — before any write

B5 establishes that `trg_stamp_provisional_static` and whatever populates `calc_hash` exist in
production but in **no repo**. Per `mcp_central/CLAUDE.md` ("git is the source of truth"), a
production object that exists nowhere in git is unreviewable and unrevertable — and A6 will fire
those triggers.

1. Dump `pg_get_triggerdef` / `pg_get_functiondef` for every trigger on
   `bond_analytics_dated`, plus the full `information_schema.columns` DDL.
2. Commit them as a numbered migration in `ga10-pricing-mcp/migrations/`, marked
   "captured from live, not applied".
3. Confirm whether `calc_hash` is a generated column or trigger-populated — this determines
   whether A6's PATCH will re-stamp it, and therefore whether A6 can *cause* new B7-style flaps.

**Read-only. Do this before A6.**

## A8. Forward fixes — so this is detectable next time

**a) Stamp `calc_engine_versions` on every row we write.** The two paths in our control:

- `recon-mcp/recon_engine.py:341` `_store_admin_prices_to_bond_data` (476 GDBFT rows, 472
  unstamped) — capture the engine hashes from the `/api/v1/portfolio/analysis` response at
  `:309` and add them to the row dict at `:361-373`.
- `ga10-pricing-mcp/src/index.js:10305` `syncAnalyticsToSupabase` (the main daily cron) — the
  pattern already exists in the same file at `:247` for `bond_analytics_per_source`; copy it.

**Caution 1:** the hopper *uses* NULL as its recalc trigger (`hopper.js:3214-3235`). Stamping
these paths removes that trigger, so the `fullRecalcMode` condition must change in the same PR
— replace "stamp is NULL" with a ga10-vs-ga11 drift rule (`src/index.js:726-739`) — or rows
will silently stop being recomputed. Do not do one without the other.

**Caution 2:** this cannot cover `NAV_REPORT` (213 GDBFT rows, 209 unstamped) because **its
writer has not been located** (B3). Finding it is a prerequisite, not a detail.

**b) Record the conventions actually applied, beside the static hash.** This is the brief's core
ask and B4 is the proof it is needed: `calc_hash` recorded correct static while
`/flexible` applied Thirty360/semi, and nothing in the row would have shown it. Add
`applied_day_count` / `applied_frequency` (and ideally `applied_accrued_days`) populated from the
engine's *response*, not from the static we sent. Then the misapplication is a one-line query
(`applied_day_count <> bond_identity.day_count`) instead of an investigation. Requires a Database
Register check and a migration.

**c) Make `calc_hash` stamping read-consistent.** B7's 18 disagreeing keys are two writers
reading `bond_identity` either side of an update. Either stamp from a single writer, or have the
DB derive it (see the existing `BACKLOG_calc_hash_postgres_generated.md`), or carry the
`bond_identity.updated_at` alongside so a race is distinguishable from a correction.

**d) Fix the universe query.** Filter on `transaction_type IN ('BUY','SELL')` (B1). Also worth a
guard on the silent `count=0` for an unknown `portfolio_id`.

**e) Ops probes** (per the house Control Room rule), on `GET /ops/probes`:
- `analytics_rows_without_analytics` — amber above a threshold, catching the NULL-accrued
  regression that caused the `GB00BBQ33664` incident
- `unpriceable_bonds` — the poison-bond count, currently **6**
- `static_validation_findings` — the A4 baseline, so a regression shows up before a demo
- `analytics_stale_days` — max days since last analytics row per held bond

**f) Do not stop swallowing the dated-upsert failure without adding an alert**
(`ga10-pricing-mcp/src/index.js:10384`). Today a failed write is a `console.warn`.

## A9. Backlog items to log now

Per the cross-project continuity rule, log these to codebase-mcp with tag `backlog` in the same
session, regardless of which steps Andy approves:

1. `/api/v1/bond/analysis/flexible` ignores stored conventions even when passed explicitly —
   returns Thirty360/freq 2/coupon None (project `google_analysis10`; evidence in B4a)
2. `trg_stamp_provisional_static` and the `calc_hash` populator exist in production but in no
   repo (project `ga10-pricing-mcp`; B5)
3. `calc_hash` writer race produces false static-drift signals on 18 keys (project
   `ga10-pricing-mcp`; B7)
4. 6 GDBFT perpetuals unpriceable by GA10 for want of `maturity_date` (project
   `ga10-pricing-mcp`; B6f)
5. 14 of 34 GDBFT bonds absent from `watchlist_cache`, so they depend entirely on the admin file
   (project `ga10-pricing-mcp`; B6d — cross-check `WATCHLIST_NAMING.md` before touching)
6. `US91282CJK80` has analytics rows but no `bond_identity` row (B6g)
7. **Unidentified writer** producing `source='NAV_REPORT'` rows in `bond_analytics_dated` —
   ranks above `scheduled_job` in every view's source priority yet exists in no repo
   (project `ga10-pricing-mcp`; B3, blocks A8a)
8. `carried_forward` rows persist with no live writer since the retirement at
   `src/index.js:6991-7007` (backlog id=480) — decide whether to purge the residue

---

## Decision list

| # | Step | Writes? | Risk | Effort | Recommend |
|---|---|---|---|---|---|
| A2 | Settle the gateway question | No | None | Minutes | **Yes — do first** |
| A7 | Capture prod triggers into git | No | None | ~1h | **Yes — before A6** |
| A3a | Repoint validator to `/portfolio/analysis` | No (code) | Low, revertible | ~1h | **Yes** |
| A3b | Fix `bond_analyze_flexible()` in GA10 | No (code) | Medium, other repo | ? | Yes, but hand off |
| A4 | Re-baseline validator | No | None | Minutes | **Yes** |
| A5 | Source perp `maturity_date` into `bond_identity` | Yes (Tier A/B ref) | Low | ~half day | **Yes** |
| A6 | Backfill 381 Tier D rows via the hopper | **Yes** | Medium — needs backup + stepwise | <10 min run | Yes, **after A2/A5/A7** |
| A8a | Stamp `calc_engine_versions` + move hopper trigger | No (code) | Medium — must be one PR | ~half day | Yes |
| A8b | Record applied conventions on the row | Yes (schema) | Low | ~1 day | **Yes — this is the real forward fix** |
| A8c | Read-consistent `calc_hash` | No/schema | Low | ~half day | Yes |
| A8d-f | Universe query, ops probes, upsert alert | No | None | ~half day | Yes |
| — | Estate-wide recompute (197,970 rows) | Yes | High | Large | **No — not justified by this evidence** |

---

## What I could not establish

Stated explicitly rather than guessed, each with the check that would settle it.

1. **Why `scheduled_job` rows are correct if the cron goes through `/api/v3`.** Empirically
   correct (B4c) but unexplained (B4). → A2.
2. **The definition of `trg_stamp_provisional_static`, and what populates `calc_hash`.** Neither
   exists in any repo; both are populated in production. → `pg_trigger` / `pg_get_functiondef`
   (A7).
3. **When `calc_engine_versions` was added to `bond_analytics_dated`.** No migration adds it.
   Code comments date the semantics to 2026-05-23 (`hopper.js:3214`) and the GA11 switchover to
   2026-06-02 (`hopper.js:3081`). → Supabase migration history.
4. **How GA11 computes `engine_hashes`.** The GA11 service is not in `mcp_central`; it is App
   Engine `ga11-dot-future-footing-414610`. So the two ga11 `core` hashes (`37cc067f`, 2024-10..
   2026-05, 474 rows; `93f0ab1e`, 2026-07..08, 22 rows) are **sequential versions of one
   engine**, and the single `{"engine":"ga10","global":"ad0fda13"}` row is the
   `USE_GA10_HISTORY` fallback (`hopper.js:3285`) — but I cannot confirm what changed between
   the two ga11 versions. → `/engine/version` on the GA11 service, or its changelog.
5. **Attribution of the 959 NULL-engine rows to an engine.** Not possible from the field. It
   *is* possible by `source` + `created_at`: `admin`/`NAV_REPORT` → recon-mcp GA10
   `/portfolio/analysis`; `scheduled_job` → GA10 gateway `/api/v3`. Confidence in that mapping
   depends on A2.
6. **Why `GB00BBQ33664` prices despite missing the same fields as the 6 poison bonds, yet has
   `accrued_interest` NULL on all 17 rows.** → A5.
7. **What writes the 213 `NAV_REPORT` rows.** A full-tree sweep of recon-mcp, athena_mcp,
   athena_portfolios, bond_data_mcp, orca_mcp and ga10-pricing-mcp found the literal only in
   view-SQL source-priority ranks, never in a writer. Blocks A8a for those rows. → grep the
   Supabase request logs for the inserting key, or check for an out-of-tree/ad-hoc importer.
8. **Which client sent the 6 `bbg_index_file` and 1 `BBG` rows.** Both almost certainly came
   through `/prices/calculate`, which passes the caller's `source` through unchanged
   (`src/index.js:11372`). Low stakes — 7 rows.

---

# DRY RUN — 2026-08-12, nothing written

Read-only execution of the plan's own steps. **Every baseline in Part B has moved**; the
conclusions hold but the numbers must be re-taken before any write.

## A2 — decision gate: **PASS**

The gate was "if the gateway returns 3.044444, stop and re-plan". It does not.

| Path | accrued | day_count | freq |
|---|---|---|---|
| Gateway `/api/v3/bond/analysis` (what the hopper calls) | **6.9917808** | ActualActual.ISMA | 1 |
| GAE direct `/api/v3/bond/analysis` | 0.0 | 30/360 | Semiannual |
| GAE direct `/api/v1/bond/analysis` | 0.0 | 30/360 | Semiannual |

Confirm is 6.991781, `bond_identity` holds ActualActual.ISMA / frequency 1. **The gateway path is
correct**, so `scheduled_job` rows are not under suspicion and A6 stays a fill, not a backfill.

**Why**, and it refines B4: `fetchGA10Analytics` (`ga10-pricing-mcp/src/index.js:8607`) calls
`resolveConventions()` and passes `overrides`. The bare GAE bond/analysis handler does not resolve
conventions itself. So the defect is "bond/analysis does not look up static", and the gateway
compensates by supplying it. `/api/v1/portfolio/analysis` resolves correctly on its own.

## A3a / A4 — done

Validator repointed to `/api/v1/portfolio/analysis` with the poison-bond halving (recon-mcp
`23da02e`). **New baseline: 54 checked, 49 agree, 5 findings, 6 unchecked** (was 58/7/51/2).

The 5 survivors are real and route to A5:

| ISIN | settle | trade | ours | diff | class |
|---|---|---|---|---|---|
| GB00BBQ33664 | 2026-07-15 | 0.700135 | 0.645890 | −0.054245 | period_anchor |
| GB00BBQ33664 | 2026-08-05 | 1.288250 | 1.235616 | −0.052634 | period_anchor |
| XS2783792307 | 2026-07-15 | 1.808218 | 1.793478 | −0.014740 | day_count |
| XS3221827911 | 2026-07-15 | 0.900000 | 0.896739 | −0.003261 | day_count |
| XS2580220171 | 2026-08-06 | 0.000000 | 0.137772 | +0.137772 | indeterminate — AA redemption, confirm carries nil accrued by construction; likely not a defect |

## A5 — **7 bonds missing maturity, not 6**

| ISIN | name | freq | day_count | locked |
|---|---|---|---|---|
| USF1067PAK24 | BNP 7.2% | — | — | false |
| USF8500RAF94 | SOCGEN 7.125% | — | — | false |
| FR001400SMR0 | EDF 7.375% | 2 | ActualActual.ISMA | **true** |
| XS2393498204 | ROTHESAY 5% | 2 | ActualActual.ISMA | **true** |
| GB00BBQ33664 | NBS1USD 10.25% | — | — | false |
| XS2485268150 | AVLN 6.875% | 2 | ActualActual.Bond | false |
| XS3404445663 | LEGAL 7.125% | 2 | ActualActual.ISMA | **true** |

All perpetual-type (AT1/RT1/hybrid), consistent with B6f. Note **three are `locked = true`**, so the
write path must respect the lock rule rather than overwrite. Separately, 4 bonds lack
day_count/frequency: the three above plus **XS3191631079** (Aldermore — has maturity, prices fine;
the plan's control case). **GB00BBQ33664 appears in both lists and in the A4 findings** — it is the
per-unit CCDS and should be treated first. `US91282CJK80` has no `bond_identity` row at all.

## A6 — population re-measured, **all baselines moved**

| Metric | Plan | Dry run | Δ |
|---|---:|---:|---:|
| Universe (BUY/SELL ISINs) | 34 | **34** | ✓ |
| Total rows | 1,456 | **1,511** | +55 |
| Price but no analytics — **would change** | 381 | **408** | +27 |
| `has_null_analytics` | 398 | **425** | +27 |
| Rows with ytm+duration | 1,075 | **1,103** | +28 |

Spread over **30 of 34 bonds**, evenly (top 18–19 rows each) — systemic fill gap, not a few broken
bonds.

**Two corrections to the plan before anyone writes:**

1. The source label is **`BBG_INDEX`**, not `bbg_index_file`. A script filtering the documented
   string silently misses those 3 rows.
2. **2026-08-03 is now present** (18 rows). That part of A6 is already done; do not re-fill it.

**The gap is accruing ~27 rows/day** — the table now runs to 2026-08-12. This is not a static
backlog, so A8a (stamp on write) matters more than the one-off fill.

Backup would capture **1,511** rows, not 1,456. Row count must not move (PATCH, not INSERT).

## A7 — **cannot be dry-run from here**

No SQL introspection path: PostgREST exposes no `exec_sql`/`execute_sql` RPC, and the Supabase MCP
is not connected in this session. Confirming which triggers/functions exist in prod but in no repo
needs DB admin access (Supabase MCP, dashboard, or psql). **A7 remains a genuine blocker on A6** as
the plan sequences it — the backfill would fire those objects blind.

## Recommendation

A2 passed, so the plan stands. Order unchanged: **A7 (needs DB access) → A5 → A6**. A6 is the only
step that writes, and it should not run until A7 is settled and the 1,511-row backup is taken.
