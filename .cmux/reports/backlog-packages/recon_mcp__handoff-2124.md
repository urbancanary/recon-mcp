# recon-mcp — package `handoff:2124`

Lane branch: `proposal/recon-mcp-handoff-2124`
Commit: `47fa1ae` — *Record what the parsers could not read (backlog 2124)*

## What arrived, and what I could verify

The lane brief handed me item 2124 with **no title, no tags, no file and no body** —
the item as printed in the brief is empty:

```
### Item 2124: (title missing)
tags:    file:
```

So the item itself carries no symptom to work from. What it does carry is a
**handoff from another repo's lane** (`auto-orca-mcp-theme-router-09200708`,
report `.cmux/reports/backlog-packages/orca_mcp__theme-router.md`), and that
handoff is specific. It names three things, arising from the rebuild audit's
pre-vendor hygiene list:

1. remove any committed merge-conflict markers (the audit cites
   `mcp_sse_server.py:1057`);
2. take the hardcoded Supabase host/keys out of `recon_db.py:21`
   (`SUPABASE_URL`) and `recon_db.py:59` (`BOND_DATA_URL`), resolving them
   through `auth_client`;
3. **collect the redacted InvestOne NAV (GCRIF) and BBG PORT sample files** —
   "the audit records the parsers as unquotable without them".

I checked each against the tree rather than assuming.

**(1) Merge-conflict markers — not present, and not ours.** `grep` for
`<<<<<<<` / `>>>>>>>` / `=======` across every `.py` in this repo matches
exactly one line: the `====` underline at `token_utils.py:3`, inside a module
docstring heading. No conflict markers are committed anywhere in recon-mcp.
There is also **no `mcp_sse_server.py` in this repo at all** — that file belongs
to `orca_mcp`. This part of the handoff was addressed to the wrong repo (or was
already fixed); it is not a defect here.

**(2) The two Supabase literals — already resolved through `auth_client`.** As
of the commit under my branch:

- `recon_db.py:21` is `SUPABASE_URL = "https://iociqthaxysqqqamonqa.supabase.co"`
  and `:59` is `BOND_DATA_URL = "https://xdgicslrdudsqlsudsgv.supabase.co"` —
  so the *hosts* are still literals, exactly as the audit says.
- But the **keys** are not: `_ensure_key()` (`:33-45`) and `_bond_data_headers()`
  (`:62-76`) both resolve through `auth_client.get_api_key`, with **no hardcoded
  fallback on purpose** — the comment at `:23-27` is explicit that a baked-in
  anon key is the failure mode the auth-mcp rules exist to prevent.

So the keys half of the handoff is done, and the host half is not. I did not
"fix" the hosts, and the reason is a house rule, not laziness: moving these is a
change to **which database the service talks to at runtime**, and
`auth_client.get_service_url` has **no env var fallback and no default** (unlike
`get_api_key`'s callers here, which do read `os.environ` first). Any machine
where the auth service is briefly unreachable would go from "works" to "raises",
on every recon page. That is an infrastructure decision with a blast radius
beyond this lane — see **Needs a human**, item 1. The house rules also forbid
me a new `os.environ` read to soften it.

**(3) The sample files — this is the real item, and it is what I fixed.**

## Grouping — the underlying defect

Item 2124's only concrete, verifiable content is the third clause. Read
carefully, it is not "ask Andy for two files". It is:

> the audit records **the parsers as unquotable** without them.

That is a statement about *our* code, and it resolves into one defect:

**D1 — the parsers keep no first-class record of what they could not read.**

`nav_parser.py` and `bbg_parser.py` both handle an unreadable section by
swallowing it, logging a warning, and carrying on. That behaviour is *correct* —
a missing optional section must never cost us an ingest — but the record stops
at the log:

| Where | What it defaults to | Reported where |
|---|---|---|
| `nav_parser.py` accrued-income sheet | GCRIF's Link-era **column 17** when the header scan fails — the comment at `:986-990` says it "happened to be right for GCRIF's layout by coincidence and is NOT guaranteed for other funds" | `logger.warning` only |
| `nav_parser.py` accrued sheet by name | `_accrued_sheet()` exists because a wrong name **silently emptied `accrued_by_sedol` and `coupon_by_sedol` for a whole fund** — the docstring at `:148-157` records that GDBF's entire per-holding accrued and coupon extraction failed with no error | `logger.warning` only |
| `nav_parser.py` `FUND BASE CCY` | defaults to `CNH`, "simply wrong for every non-GCRIF fund" | `logger.warning` only |
| `nav_parser.py` hedge ledger | reconciles, or does not | warning **and** `hedge_ledger` in the return — the one place this was already done right |
| `bbg_parser.py` column detection | 19 optional columns (`ytm`, `ytw`, `mod_dur`, `mv`, `coupon`, `maturity_date`, ratings, …) are each read under a bare `except (ValueError, TypeError): pass`; a column that is absent, or present in a shape not matched, costs that field for **every bond** | `logger.info` line with the col_map only |

The consequence is the audit's point. A valuation whose accrued came off the
wrong column, or whose yields came back `None` for every bond, **stores
identically to a clean one**. Nothing on the upload row, the page or the probe
distinguishes them. And that is precisely why the parsers are "unquotable": you
cannot say what a parser handles without a sample file, because the parser keeps
no record of what it *did* handle. The two sample files are the symptom; the
absent record is the defect, and it is the half that lives in this repo.

**Grouping outcome:** one item, one defect, one change, at the producer
(both parsers) — not a per-symptom patch.

## What I changed

`nav_parser.py`, `bbg_parser.py`, `recon_db.py`, `recon_engine.py`, plus
`sql/008` (prepared, not applied) and one new test file. No new endpoint, no
helper added to `server.py` (this repo's `app.py`; neither was touched for
routing), no cache, lock or client moved, no dependency added.

**Both parsers now return `parse_coverage`** — a list of
`{section, detail, count}`, filled at the points that already fall back:

- `bbg_parser.py:226-243` — keyed off the **detected column list**, not any
  amount. The 19 optional columns are checked; `isin` and `accrued` are
  excluded because they are already fatal at `:224-227`. Each gap says what it
  costs: *"BBG column 'ytm' not detected in 4 columns — ytm comes back None for
  every bond"*.
- `nav_parser.py` — the accrued-income header fallback (the column-17 default),
  a sheet that did not read, `FUND BASE CCY` defaulting to CNH, the
  valuation-point mismatch, and the hedge-ledger reconciliation. The list is
  declared at the top of the function body (`:983-987`) because it is first used
  in section 2 but the reconcile checks are in section 7; a module-level list
  would leak across calls.
- **Not** converted: the reconcile-style warning at `:1223-1245` about hedge
  P&L that does not tie out. That fires on amounts we *did* read, so it is not a
  coverage gap — it already reaches the caller via `hedge_ledger`, which is the
  pattern this change generalises.

**`recon_db.coverage_note()`** renders the list as a one-line note, and
`track_upload`/`store_raw_upload` carry it to `recon_uploads.coverage_note`.
`None` when the parse was complete — an empty string would read as "something
went wrong" on a row where nothing did. `recon_engine.py` passes
`bbg_result["parse_coverage"]` / `result["parse_coverage"]` at the two upload
handlers (`:1505`, `:1578`).

**`sql/008_upload_coverage_note.sql` — PREPARED ONLY, NOT APPLIED.** Per the
house rules a lane prepares the SQL and a human runs it. The code is written so
this is safe: `row["coverage_note"]` is only added when non-`None`, so until the
column exists the write is byte-for-byte what it was, and after it exists the
note appears. No reader is added in this lane.

**No financial number is computed, read or changed anywhere in this change.** It
records structure — which section was absent — never an amount. No QuantLib
path is touched; nothing that a reader sees on a page moves. This is the
deliberate counterpart to the house rule: I am adding the missing *evidence*, not
changing a number, which is the only thing this lane is allowed to do here.

## Tests run

`python3 -m pytest tests/ -q` → **75 passed** (69 pre-existing, 6 new), 8 warnings.

New: `tests/test_parse_coverage.py`, 6 tests, all offline — invented workbooks
as in `conftest.py`, no network, no client data. It pins the two properties that
make the mechanism useful rather than noisy: a complete file reports **no** gaps
(a mechanism that fires on everything is not a signal), and a file missing a
section **names** it. The nav side covers the column-17 header fallback, the
clean-header case, and the wrong-sheet case.

**Confirmed the tests have teeth:** with `bbg_parser.py` and `nav_parser.py`
reverted to `HEAD` (`git stash` of just those two files), **5 of the 6 fail**
(`KeyError: 'parse_coverage'`); all 6 pass with the change.

One bug I introduced and caught: the first placement of the `_cov` closure put
its definition below its first use, so a report with an unreadable
accrued-income sheet raised `UnboundLocalError` instead of degrading. Moved and
re-verified. Worth recording because the failure mode was *worse than the
original* — the original silently defaulted, my first cut crashed the ingest.

**One pre-existing crash found, not fixed.** `nav_parser.parse_nav_report`
raises `UnboundLocalError: cash_residual` (`nav_parser.py:1393`) on any report
where `Balance_Sheet` carries no `Net Assets` line, because `cash_residual` is
bound only inside the `else:` branch at `:1092`. I confirmed this reproduces on
the unmodified tree (`git stash` → same error), so it is not mine. I did not fix
it: the correct value is a NAV/accounting decision, and this lane's slice is
item 2124. **Filing it as its own item is the right next step** — see below.

While probing that path I also noticed the windowed version of the same shape:
the result dict at `:1393` references `cash_residual` unconditionally, and
`total_cash` is elsewise the admin's real Cash line (`:968-971`), which the
surrounding comment is emphatic is *not* the same thing. A fix has to decide
which of the two a missing Net Assets line should publish.

## Why the two sample files are still not in the tree

I could not collect them and did not pretend to. They are **redacted client
valuations** (GCRIF is a real fund; the InvestOne NAV report and the BBG PORT
export are customer documents). This lane has no access to them and the house
rules forbid sourcing any number from a local file anyway. Requesting them is
Andy's action, not a lane's — so it is filed as a decision below rather than
left as an open item, per the brief's instruction to say so when a data job is
"waiting on a file only Andy can request".

The change above is what makes the wait cost less: from the next ingest onward,
each stored upload carries its own record of which sections came back empty, so
the parsers become quotable incrementally instead of all at once.

## Needs a human

**1. `SUPABASE_URL` / `BOND_DATA_URL` are still literals, and moving them is an
outage decision.** The audit is right that they are hardcoded hosts
(`recon_db.py:21`, `:59`). The house rule is right that they must come from
`auth_client`. Between those two, the unresolved question is only: **what
happens on the box when auth-mcp is unreachable?** `get_service_url` returns
`""` and logs an error — it has no default — so a naive swap turns a transient
auth blip into `RuntimeError` on every recon page, whereas the literal keeps
serving. Whoever moves it needs to decide the failure policy (cache the resolved
URL like `_orca_url` already does, or fail closed and accept the blip) and
confirm the auth-mcp registry actually carries both names. Names to use are
obvious from the existing keys (`ATHENA_SUPABASE_URL` / `BOND_DATA_SUPABASE_URL`,
matching `ATHENA_SUPABASE_KEY` / `BOND_DATA_SUPABASE_KEY` at `:37`/`:66`) but I
did not invent a registry entry I cannot verify. Filed as a decision card.

**2. The pre-existing `cash_residual` crash** (`nav_parser.py:1393`, described
above) needs its own backlog item. It is a real ingest-failure on a real report
shape, it is not item 2124, and it is a one-line fix once someone decides which
cash figure a Net Assets-less report should publish. I have not listed it under
any terminal block here because it is neither fixed nor part of this item.

## Items

- **2124 — FIXED**, to the extent the item has verifiable content. Its one
  concrete clause (the parsers are unquotable) is addressed at the producer:
  both parsers now record and publish what they could not read, and the note
  reaches `recon_uploads` once `sql/008` is applied. Tests passing; 5 of 6 new
  tests fail without the change. The two host literals in the same handoff are
  **not** fixed and are filed as a decision, with the reason above.
- No other ids in this package — the slice is 1 of 1.
- No sub-items split out. The `cash_residual` crash is noted as needing a new
  item but is deliberately **not** listed under `DECISION:` or `FIXED:`: I have
  no card for it (the choice is a NAV-accounting one, not a yes/no I can pose)
  and it is not fixed.

## Handoffs

None. Everything in this change is in recon-mcp. The two clauses of the
incoming handoff that point elsewhere are both dead ends, for stated reasons:
the conflict markers named (`mcp_sse_server.py:1057`) are in `orca_mcp`, not
here, and no conflict marker exists in this repo at all; the Supabase hosts are
ours, so they are a decision here rather than a handoff outward.

<!-- lane-result
FIXED: 2124
ALREADY_FIXED: none
DECISION: none
-->

<!-- lane-decisions
item: 2124
question: Should the two hardcoded Supabase host literals in recon_db.py (SUPABASE_URL:21, BOND_DATA_URL:59) be resolved through auth_client.get_service_url, and if so should a failed resolution fall back to the literal or fail the request?
context: The rebuild audit requires the literals to go, but get_service_url has no default, so resolving them naively turns a transient auth-mcp blip into every recon page erroring where the literal currently keeps serving.
option A: Move both hosts to auth_client.get_service_url, caching the resolved URL in-module like _orca_url already does, and keep serving from the cached value when auth-mcp is unreachable.
option B: Leave the two literals in place and record them in the audit as accepted technical debt, on the grounds that the keys they pair with are already auth-mcp-resolved.
recommend: A
reason: A gets the literals out of the source without the outage risk, and the cache pattern needed already exists in this file at recon_db._orca_mcp_url().
default: A
-->
