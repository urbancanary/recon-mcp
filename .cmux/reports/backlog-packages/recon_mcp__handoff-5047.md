# recon-mcp — package `handoff:5047`

Lane branch: `proposal/recon-no-local-calc-copy-5047`
Commit: `6671362` — *5047: recon stops keeping a local copy of the calc static*

## The item said

Handed over by `auto-bond-data-mcp-handoff-5047-09220500`:

> Stop keeping a local calc copy. Read coupon / maturity_date / day_count /
> frequency from bond_data_mcp's new `POST /tools/static_corrections` (which
> serves them from `v_bond_static`) at calc time, and delete the calc fields
> from `local_bond_reference`, or never write them. In `recon_db.py`,
> `sync_bond_data` / `enrich_bond_data_from_bbg` should stop overwriting
> `maturity_date`, `frequency` and `day_count` (and filling `coupon`) on local
> rows: send what Bloomberg told you as `findings` on that endpoint instead,
> and apply a correction only from its `corrections` list.

The handoff named two functions. I checked both against the tree and against
the producer's own contract before writing anything, because the two functions
had *different* defects and only one of them was the overwrite.

## What the endpoint actually is (read from the producer, not assumed)

`/opt/work/bond_data_mcp/server.py:510` and `normalizer.py:725`
(`build_static_corrections`). The contract, as the code states it:

- `POST /tools/static_corrections` takes `{findings, isins, sync_observed}`,
  reads **v_bond_static**, and **writes nothing** — `"applied": 0`, and its
  own note says *"corrections are offered, never applied"*.
- A calc field on a row moves **only** by the guarded UPDATE in
  `migrations/005_PREPARED_recon_static_corrections.sql`. That file's own
  header says it "**DOES move numbers**" and "**no lane may**" apply it.
- If the view cannot be read the endpoint answers `status: "unavailable"` with
  the instruction: *"do not fall back to a local copy"*.
- `corrections` is already the restricted set: values that disagree with the
  view (`normalizer.py:786`) *and* are not contradicted by the producer's own
  store via `sync_observed` (`normalizer.py:800-806`). `CALC_FIELDS` is
  exactly `coupon, maturity_date, day_count, frequency`.

So "read the calc static at calc time" is satisfied by the `current_static` the
endpoint returns, and `sync_bond_data` (T-88, re-landed at `0e7360b`) already
does exactly that from the same view. What was left was the write side.

## Grouping — the underlying defects

Both in `recon_db.py`, both the same root cause (recon holding and writing its
own copy of the calc static), but they are separable and one of them is the one
the handoff actually names.

**D1 — `enrich_bond_data_from_bbg` overwrote the calc fields on local rows.**
This is the defect named in the handoff, and the code was as described:
`ref_patch` set `maturity_date` / `frequency` / `day_count` on every *unlocked*
row unconditionally ("ALWAYS overwrite … (authoritative)"), and `id_patch`
filled a NULL `coupon`. Nothing anywhere told bond_data_mcp this had happened,
so the two sources could disagree with no record of it. That is the drift the
producer's migration header describes verbatim: *"an enrichment service
(recon-mcp) parses Bloomberg, keeps its own mirror of the static fields, and
overwrites them locally — including on rows the bond-data side considers
locked."*

**D2 — `overlay_canonical_static` left the base table's value in the mirror
when the view carried none.** Added by T-88 in the same file, and easy to miss:
it wrote `out[f] = s.get(f)`, so a field `v_bond_static` holds as NULL was
*overwritten with None* in the payload — but more importantly, the T-88
`select` never asked the view for a value that was absent, so the mirrored
base-table coupon stayed in `local_bond_reference`. `sql/003_create_recon_views.sql`
COALESCEs `local_bond_reference`'s calc fields against the bond-data side
(`:71`, `:106`, `:178`, `:217`, `:280`), so a leftover mirrored coupon there is
**still priced on**. The handoff's first sentence — *"Stop keeping a local calc
copy … or never write them"* — is this defect, and it is the half that a reader
of the handoff would most easily think was already done by T-88. It was not.

One change, because they are the same decision (the mirror holds no calc value
the view did not give it) and both live in the same two functions.

## What I changed

`recon_db.py` only. No new endpoint, no new route module, nothing in
`server.py` — there is no `server.py` in this repo; the app is `app.py` and I
did not touch it. No cache, lock or client copied. No migration prepared or
applied. No dependency added.

### D1 — findings out, corrections back

`enrich_bond_data_from_bbg` no longer writes to `local_bond_identity` or
`local_bond_reference` at all. It converts the BBG-parsed values into
`findings` — `{isin, field, value, source, observed_at, evidence}`, blanks
omitted, because the producer's `_BLANK_VALUES` rule says a blank is not
evidence — and posts them through a new helper:

- `offer_static_corrections(findings)` — posts to
  `{bond_data_service_url}/tools/static_corrections`, returns the producer's
  answer unchanged. On timeout, non-200, or `status != "ok"` it returns
  `status="unavailable"` with **`corrections: []`**. That is the producer's own
  instruction and it is the safe direction: no local fallback, nothing written,
  the status logged.
- `_apply_offered_corrections(corrections)` — writes back only rows and fields
  the canonical path itself named, only values it returned, and only fields in
  `REFERENCE_CALC_FIELDS`. A local value now moves *only* when bond_data_mcp's
  restricted `corrections` list says so. (I chose to mirror it back rather than
  write nothing, so the local row stops contradicting the view; the guarded
  UPDATE that moves the row for every engine remains bond_data_mcp's 005, which
  I did not touch.)
- `_bond_data_service_url()` — resolved lazily, env override → auth-mcp
  `get_service_url("bond_data_mcp")`. No hardcoded URL; the fallback is empty,
  which routes to `unavailable` rather than to a baked-in host.
- `_patch` was hoisted out of the function to module level so both paths share
  one implementation instead of my adding a second copy.

`enriched` in the return dict is retained (it counts corrections actually
applied) because `recon_engine.py:1513` and `app.py:1101` call this and
`app.py` returns the dict to an HTTP caller; nothing outside the file reads the
old `enriched_identity` / `enriched_reference` keys — I grepped, and no file
does.

### D2 — a calc field the view does not carry is dropped, not mirrored

`overlay_canonical_static` now pops the key when the view's value is None
rather than copying the base table's value through. A field the view *does*
carry is still taken from the view; a bond the view does not return at all
still keeps its row, so a transient read gap does not blank the mirror.

## Item ids

- **5047 — addressed.** Both halves above are its text: stop overwriting (D1),
  stop keeping the copy (D2), offer findings instead of writing them, apply
  only from `corrections`.
- No other id is in this package (slice was 1 item).

## Tests I ran

- `tests/test_5047_no_local_calc_copy.py` — new, 10 tests: view value replaces
  the base value; a calc field the view lacks is dropped; a bond the view lacks
  keeps its row; BBG values go out as findings with source and date; **nothing
  is patched when the canonical path offers nothing** (the regression guard for
  D1); only the canonical path's own corrections are applied and off-contract
  fields are ignored; a blank is not offered; an unreachable path and an
  `unavailable` answer both yield no corrections; the service URL is not
  hardcoded.
- `tests/test_t88_static_reads.py` — 3 passed, unchanged.
- Full suite: **82 passed** (`python3 -m pytest tests/ -q`).
- `pyflakes recon_db.py tests/test_5047_no_local_calc_copy.py` — clean.

## Not done, and why

- **`migrations/005_PREPARED_recon_static_corrections.sql` in bond_data_mcp is
  not applied and no lane may apply it.** Its step 1 creates `v_bond_static`;
  its step 4 is a guarded UPDATE of calc fields. Step 4 moves accrued, duration
  and yield on whatever prices the bond — a client-facing financial number —
  and the file's own header reserves it for independent review. **If
  `v_bond_static` does not already exist in the bond-data database, this
  change and T-88 both read an empty set and recon falls back to the local
  mirror** (which, after this change, is only what `sync_bond_data` puts
  there). Step 1 is a read-only view and moves no number, so it is the piece
  that should be confirmed and run first.
- **`local_bond_reference` and `local_bond_identity` still physically carry
  `coupon` / `maturity_date` / `day_count` / `frequency` columns.** The handoff
  offers "delete the calc fields … or never write them"; I took the second arm.
  Dropping columns is a schema change on a table the recon views join, and the
  house rules say never drop or alter a table. `sql/003_create_recon_views.sql`
  would need rewriting in the same breath, so it is a separate, reviewed change.
- **`current_static_hash` / `v_stale_calcs` still hash the mirror, not the
  view.** `compute_static_hash` runs over `local_bond_reference` rows
  (`recon_db.py:291`), so after this change the hash describes what recon
  *holds* rather than what it *prices on*. T-88's overlay makes them agree
  whenever the view is readable, so I did not change the hash — changing it
  would move a `stale` flag that the recon view surfaces. Worth a follow-up.

## Needs a human

One thing, and it is not a code change: **confirm `v_bond_static` exists in the
bond-data Supabase project, and run migration 005 step 1 if it does not.**
Nothing in either repo shows the view existing — the migration header says so
itself — while every surface that prices a bond is documented as reading it.
If it is missing, the calc-time read this package and T-88 both install returns
nothing and the local mirror remains the de facto source.

<!-- lane-result
FIXED: 5047
ALREADY_FIXED: none
DECISION: none
-->
