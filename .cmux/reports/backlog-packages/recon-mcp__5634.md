# Backlog item 5634 — bbg_parser: capture Mod Dur and OAD separately, stop coalescing

**Project:** recon-mcp · **Branch:** proposal/mini-5634-1006230440 · **Date:** 2026-10-07

## Verdict: FIX (bbg_parser.py)

### What the tree does today

`bbg_parser.py` *detects* the two duration columns separately — `col_map['mod_dur']`
lines 134–139, `col_map['oad']` lines 141–147 — but then **coalesces them at
`bbg_parser.py:236`**:

```python
oad_col = col_map.get('mod_dur') or col_map.get('oad')  # Prefer mod dur over OAD
```

Only that one column is read (loop at `bbg_parser.py:326–332`) and it lands in a single
`oad_bonds` dict (`bbg_parser.py:246`, returned at line 578). `recon_engine.py:1455`
reads `oad_bonds` and `recon_engine.py:1487` writes it to `recon_bbg.duration` with no
record of *which* measure it was. When a month's file switches layout (OAD present in
March, Mod Dur present in April), the stored `duration` silently changes measure with
nothing downstream able to tell — the ADCOP `XS1709535097` 9.18 → 12.771 jump in the
evidence.

### Change made

In `bbg_parser.py` only:

- Extract **both** columns independently: `mod_dur_bonds` from the Mod Dur column and
  `oad_bonds` from the OAD column (each kept only when the column is present).
- Keep the derived primary duration **explicitly** as mod-dur-preferred, per ISIN
  (`duration_bonds[isin] = mod_dur if present else oad`). This is the same value the old
  coalesced line produced, so **no client-facing duration number moves** and
  `recon_engine.py`/`recon_db.py` need no change.
- Expose provenance in the returned dict: file-level `duration_measure`
  (`'mod_dur'` | `'oad'`) and per-ISIN `duration_measure_bonds`.
- Preserve the existing `oad_bonds` key (now the derived primary) for backward
  compatibility with `recon_engine.py:1487`, plus add `mod_dur_bonds` and `oad_only_bonds`.

Test: new `tests/test_bbg_parser_duration.py` builds BBG-shaped workbooks for both
layouts (Mod Dur only, OAD only, and both present) and asserts each measure is captured
separately and the primary stays mod-dur-preferred.

### Remaining (bundled) ask — NOT done here

"recon_bbg gets `duration_measure`" (the DB column) is left to a follow-up: it needs a
`recon_bbg` schema migration, a `store_bbg` row field, and the `recon_engine` row
builder to pass the new fields through. Deliberately out of scope for this one-file
change so the stored `duration` value is provably unchanged.

<!-- lane-result
FIXED: 5634
ALREADY_FIXED: none
DECISION: none
-->
