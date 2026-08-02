"""Fund registry — adding a fund to the AUM recon is an entry here, not code.

Deliberately a Python module, not a DB table: fund identity (id, aliases,
admin format) is reference data that is the same on every host, exactly like
fund_config's valuation points. See CLAUDE.md "No Railway Env Vars for Static
Values" — the same argument applies to registry-in-DB, which would just make
"which funds exist" invisible and unversioned.

`sections` lists which parts of the AUM comparison a fund can honestly
populate — the UI hides what a fund cannot answer instead of showing empty
tables (same principle as Athena's nav gating).
"""

FUNDS = {
    "gdbf": {
        "name": "Guinness Global Dynamic Bond Fund",
        # gdf = portfolio_optimizer_db id; DYN = Maia extract prefix. "Bond"
        # is sometimes dropped from the name in admin mails.
        "aliases": ["gdf", "dyn", "gdbf", "global dynamic"],
        "admin_format": "waystone_investone",
        "base_currency": "USD",
        "maia": True,
        "sections": ["aum", "attribution", "bonds", "prices", "accrued",
                     "fx", "currency", "evidence"],
    },
    "gcrif": {
        "name": "Guinness China RMB Income Fund",
        "aliases": ["gcrif", "china rmb", "rmb income"],
        "admin_format": "waystone_investone",
        "base_currency": "USD",
        "maia": False,   # no Maia position view ingested for GCRIF yet
        "sections": ["aum", "currency"],
    },
    "wnbf": {
        "name": "Wealthy Nations Bond Fund",
        "aliases": ["wnbf", "wealthy nations"],
        "admin_format": None,   # transactions-primary; no admin NAV packs here
        "base_currency": "USD",
        "maia": False,
        "sections": [],  # bond-level recon only (existing /recon/* pages)
    },
}


def resolve(name_or_alias: str) -> str | None:
    """Fund id for any known alias (case-insensitive), else None."""
    s = (name_or_alias or "").strip().lower()
    if s in FUNDS:
        return s
    for pid, f in FUNDS.items():
        if s in f["aliases"]:
            return pid
    return None


def aum_funds() -> list[str]:
    """Funds the AUM recon can run for (admin packs expected)."""
    return [pid for pid, f in FUNDS.items() if f["admin_format"]]
