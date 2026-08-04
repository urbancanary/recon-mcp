#!/usr/bin/env python3
"""Watch the Guinness OneDrive MAIA folders and feed recon-mcp.

Runs on Andy's Mac (cron, every 15 min) against the locally-synced OneDrive
dirs — the sync client has already done the transport, so no Graph API is
needed. Every new spreadsheet is POSTed to recon-mcp /aum/upload/auto, which
identifies the TYPE from the CONTENTS (maia_classify) — filenames in these
folders carry no signal — and routes it: admin packs → admin pipeline, Maia
views → the AUM recon, AUM summaries / compliance dumps → raw-stored.

Idempotent three ways: a local sha256 state file skips already-pushed bytes,
recon-mcp dedupes the registry by hash, and storage upserts by (source,
fund, date).

Files-On-Demand: placeholders aren't readable until materialised. We ask
macOS to download via `brctl download` and skip the file until the next run
if it hasn't landed — never half-read.

Install (writes the crontab line):  python3 scripts/onedrive_maia_watch.py --install
"""
import argparse
import hashlib
import hmac
import json
import pathlib
import subprocess
import sys
import time

import httpx

HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

FOLDERS = [
    pathlib.Path("/Users/andyseaman/Library/CloudStorage/"
                 "OneDrive-GuinnessAssetManagementLimited(2)/MAIA"),
    pathlib.Path("/Users/andyseaman/Library/CloudStorage/"
                 "OneDrive-GuinnessAssetManagementLimited/MAIA"),
]
STATE = pathlib.Path.home() / ".recon_maia_watch.json"
RECON_URL = "https://recon.x-trillion.com"
FUND = "gdbf"  # every file seen in these folders so far is Dynamic Bond
EXTS = {".xlsx", ".xls"}


def _service_token() -> str:
    from auth_client import get_api_key
    key = get_api_key("ATHENA_SERVICE_KEY", requester="onedrive-maia-watch")
    return hmac.new(key.encode(), b"recon-mcp", hashlib.sha256).hexdigest()


def _read_or_materialise(p: pathlib.Path) -> bytes | None:
    try:
        data = p.read_bytes()
        if data[:2] in (b"PK", b"\xd0\xcf"):  # zip (xlsx) or OLE2 (xls)
            return data
    except OSError:
        pass
    # Files-On-Demand placeholder: request download, pick it up next run.
    subprocess.run(["brctl", "download", str(p)], capture_output=True)
    print(f"  placeholder, download requested: {p.name}")
    return None


def run() -> int:
    state = json.loads(STATE.read_text()) if STATE.exists() else {"pushed": {}}
    token = _service_token()
    pushed = failed = 0
    for folder in FOLDERS:
        if not folder.is_dir():
            continue
        for p in sorted(folder.iterdir()):
            if p.suffix.lower() not in EXTS or p.name.startswith("~$"):
                continue
            data = _read_or_materialise(p)
            if data is None:
                continue
            digest = hashlib.sha256(data).hexdigest()
            if state["pushed"].get(digest):
                continue
            try:
                r = httpx.post(
                    f"{RECON_URL}/aum/upload/auto",
                    params={"fund": FUND},
                    files={"file": (p.name, data, "application/octet-stream")},
                    headers={"X-Service-Key": token,
                             "X-User-Email": "onedrive-watch@mac"},
                    timeout=120,
                )
                body = r.json() if r.headers.get("content-type", "").startswith(
                    "application/json") else {}
                if r.status_code == 200:
                    print(f"  {p.name}: {body.get('detected_type')} "
                          f"→ {body.get('date') or body.get('status')}")
                    state["pushed"][digest] = {
                        "file": p.name, "at": time.strftime("%F %T"),
                        "type": body.get("detected_type")}
                    pushed += 1
                elif r.status_code == 422:
                    # Understood-but-unusable (wrong shape, no date). Remember
                    # it so we don't re-push the identical bytes every run.
                    print(f"  {p.name}: rejected — {body.get('detail')}")
                    state["pushed"][digest] = {
                        "file": p.name, "at": time.strftime("%F %T"),
                        "rejected": body.get("detail")}
                else:
                    print(f"  {p.name}: HTTP {r.status_code} — will retry")
                    failed += 1
            except Exception as e:
                print(f"  {p.name}: FAILED {e} — will retry")
                failed += 1
    STATE.write_text(json.dumps(state, indent=1))
    print(f"done: {pushed} pushed, {failed} to retry")
    return 1 if failed else 0


def install_cron():
    line = (f"*/15 * * * * cd {HERE} && /usr/bin/env python3 "
            f"scripts/onedrive_maia_watch.py >> /tmp/maia_watch.log 2>&1")
    cur = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = cur.stdout if cur.returncode == 0 else ""
    if "onedrive_maia_watch" in existing:
        print("cron entry already present")
        return
    new = existing.rstrip("\n") + ("\n" if existing.strip() else "") + line + "\n"
    subprocess.run(["crontab", "-"], input=new, text=True, check=True)
    print(f"installed: {line}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--install", action="store_true", help="install the cron entry")
    args = ap.parse_args()
    if args.install:
        install_cron()
    else:
        sys.exit(run())
