#!/usr/bin/env python3
"""Fail (exit 1) if recon-mcp's synced copies have drifted from their
canonical athena_html_v3 sources.

The copies carry a 3-line provenance header; everything after it must be
byte-identical to the athena file. Run this in the promote checklist (and CI
when it exists). If it fails, the fix is: edit the ATHENA copy, re-copy here,
never patch the copy in place.

Usage:  python3 scripts/check_parser_sync.py [athena_repo_path]
"""
import hashlib
import pathlib
import sys

HEADER_LINES = 3  # provenance header prepended to each synced copy
SYNCED = ["nav_parser.py", "fund_config.py"]

here = pathlib.Path(__file__).resolve().parent.parent
athena = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else \
    here.parent / "athena_html_v3"

if not athena.is_dir():
    print(f"SKIP: athena repo not found at {athena} — cannot check sync")
    sys.exit(0)  # not an error on machines without the athena checkout

failed = False
for name in SYNCED:
    src = (athena / name).read_bytes()
    copy = (here / name).read_text().split("\n", HEADER_LINES)[HEADER_LINES]
    a, b = hashlib.sha256(src).hexdigest(), \
        hashlib.sha256(copy.encode()).hexdigest()
    status = "ok" if a == b else "DRIFT"
    print(f"{name}: {status}")
    failed |= a != b

if failed:
    print("\nDrift detected. Re-copy from athena_html_v3 (keep the 3-line "
          "provenance header, update the commit sha in it).")
    sys.exit(1)
