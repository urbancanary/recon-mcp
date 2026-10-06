# recon-mcp — item 5636

`Restore raw upload retention for BBG uploads (2670)`

Branch: `proposal/mini-5636-1006230443`

## What the item asks, and what the tree does

> Raw upload retention was dead before 2026-08-02; pre-August sources are
> unrecoverable and the BBG path is unproven.
>
> Wanted result: A BBG upload lands in `recon-uploads/bbg/` with a
> `recon_uploads` row, verified by one live upload.

Traced the BBG upload path end to end:

- `app.py:319` `POST /upload/bbg` → reads bytes → `process_bbg_upload(...)`.
- `recon_engine.py:1420` `process_bbg_upload` → at `recon_engine.py:1502-1509`
  it already stores the raw file, in parallel with the parsed rows:

  ```python
  await asyncio.gather(
      store_bbg(pid, bbg_date, bbg_bonds, uploaded_by),
      store_raw_upload(
          source="bbg", portfolio_id=pid, date=bbg_date,
          file_bytes=file_bytes, filename=filename,
          uploaded_by=uploaded_by, bonds_parsed=len(bbg_bonds),
      ),
  )
  ```

- `recon_db.py:545` `store_raw_upload` → `upload_to_storage` (`recon_db.py:476`)
  writes to bucket `STORAGE_BUCKET = "recon-uploads"` (`recon_db.py:467`) at path
  `{source}/{portfolio_id}/{date}{ext}` → `bbg/<pid>/<date>.xlsx`, i.e. exactly
  the `recon-uploads/bbg/` location the item names — then `track_upload`
  (`recon_db.py:523`) upserts the `recon_uploads` row with the file path, name,
  size, SHA256 hash and uploader.

Both halves of the wanted result are therefore already wired: the raw bytes land
under `recon-uploads/bbg/`, and a `recon_uploads` row is written keyed on
`(portfolio_id, source, date)` — which matches the table's primary key
(`sql/001_create_tables.sql:143`), so the upsert conflict target is valid.

History agrees this is not a recent regression that should be "restored":
`git log -S store_raw_upload -- recon_engine.py` attributes the BBG call to
`5e0370d` (2026-05-05), an ancestor of HEAD, and `git blame -L 1501,1509
recon_engine.py` shows those lines unchanged since. The admin (`1577`) and Maia
(`1649`) paths carry the same call, so retention is uniform across the three
sources. No feature flag or env gate disables it — `grep -rni retention` finds
nothing, and the source strings are explicit (`source="bbg"`).

The "dead before 2026-08-02" note lines up with `bb55359` (2026-08-02,
*Remove hard-coded Supabase keys — auth-mcp only, fail loud*): before that
commit `_ensure_key()` silently fell back to a baked-in anon JWT, after it the
key must come from `ATHENA_SUPABASE_KEY` / auth-mcp and raises if absent. That
is a key-provisioning/ops property, not a missing retention code path — when the
key resolves, the BBG write above runs.

## Why this is a verdict and not a change

Verdict: **ALREADY_FIXED.** The tree already performs the wanted write; there is
no missing symbol, table, bucket constant or call site to add. Building a second
retention call in `process_bbg_upload` would be duplicate work.

One caveat, stated plainly: the third clause of the wanted result — *verified by
one live upload* — was **not** performed here. That needs credentials
(`ATHENA_SUPABASE_KEY` via auth-mcp), a running service and a real BBG file, and
this lane is barred from deploying; a live verification is a production/ops step
for whoever holds the key, not a repo change.

## What would confirm it live (for the operator, no code change)

1. Upload any `.xlsx` BBG export to `POST /upload/bbg`.
2. Confirm the object at `recon-uploads/bbg/<portfolio_id>/<date>.xlsx`.
3. Confirm one `recon_uploads` row with `source='bbg'`, `portfolio_id`, `date`,
   non-null `file_path`/`file_hash`, `parse_status='ok'`.

Note (not part of this item): `process_bbg_upload` discards the return of
`store_raw_upload`, so a storage/registry failure logs an error but does not
fail the upload. That is a robustness observation, not a fix this item asks for.

<!-- lane-result
FIXED: none
ALREADY_FIXED: 5636
DECISION: none
-->
