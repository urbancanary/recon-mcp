"""
NAV Report Parser for Guinness China RMB Income Fund

Parses the daily XLS NAV report from InvestOne and returns structured JSON
matching Athena's API format. Used by the upload endpoint in server.py.

Sheets used:
  - Balance_Sheet: valuation date, cash, total NAV
  - Detailed_Security_Valuation: holdings with prices, MV, P&L
  - LFAI_Accrued_Income_Recon: per-bond accrued income
  - OpenCurrency: FX forwards and spot rates
  - Share_Class_Price_Report: share class NAVs
"""

import math
import re
import logging
from datetime import datetime, date
from io import BytesIO
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ─── SEDOL → metadata lookup ────────────────────────────────────────────────
# These bonds are the known GCRIF universe. New bonds appearing in the XLS
# will be mapped using description-based heuristics as a fallback.
BOND_METADATA = {
    "BS4DCD7": {"ticker": "AGRBK", "country": "China", "name": "Agricultural Bank of China", "is_cgb": False, "maturity": "2027-07-31"},
    "BN6LY72": {"ticker": "HKMC", "country": "Hong Kong", "name": "Hong Kong Mortgage Corp", "is_cgb": False, "maturity": "2026-09-12"},
    "BQ1JSR0": {"ticker": "GACI", "country": "Saudi Arabia", "name": "GACI First Investment", "is_cgb": False, "maturity": "2027-10-13"},
    "BBLZGN5": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2028-06-27"},
    "BPSNN73": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2028-08-04"},
    "BQ845Y4": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2026-06-16"},
    "BSLK7P8": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2029-03-15"},
    "BSLK7Q9": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2027-03-15"},
    "BV4GCM6": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2028-02-21"},
    "BV4GD01": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2027-02-21"},
    "BYSVGD7": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2026-07-04"},
    "BRF4G96": {"ticker": "CGB", "country": "China", "name": "China Government Bond", "is_cgb": True, "maturity": "2029-06-10"},
    "B7SBXC6": {"ticker": "CHEXIM", "country": "China", "name": "Export-Import Bank of China", "is_cgb": False, "maturity": "2027-06-18"},
    "BQZC6B5": {"ticker": "KFW", "country": "Germany", "name": "KfW", "is_cgb": False, "maturity": "2026-02-24"},
    "BN0Z7F9": {"ticker": "ADGB", "country": "UAE", "name": "Abu Dhabi Government", "is_cgb": False, "maturity": "2028-06-02"},
}

# Description-based issuer lookup — works for any SEDOL (new bonds auto-match)
ISSUER_PATTERNS = [
    (r"China Government Bond", {"ticker": "CGB", "country": "China", "is_cgb": True, "asset_type": "Government"}),
    (r"Export.Import Bank.*China|EXPORT-IMPORT BANK CHINA", {"ticker": "CHEXIM", "country": "China", "is_cgb": False, "asset_type": "Government"}),
    (r"KFW|Kreditanstalt", {"ticker": "KFW", "country": "Germany", "is_cgb": False, "asset_type": "Government"}),
    (r"Abu Dhabi Gov|ABU DHABI GOVT", {"ticker": "ADGB", "country": "UAE", "is_cgb": False, "asset_type": "Government"}),
    (r"GACI|GACI FIRST INVESTM", {"ticker": "GACI", "country": "Saudi Arabia", "is_cgb": False, "asset_type": "Corporate"}),
    (r"Agricultural Bank.*China|AGRICULTURAL BK CHINA", {"ticker": "AGRBK", "country": "China", "is_cgb": False, "asset_type": "Corporate"}),
    (r"Hong Kong Mortgage|HONG KONG MORTGAGE", {"ticker": "HKMC", "country": "Hong Kong", "is_cgb": False, "asset_type": "Corporate"}),
]

# Country-only fallback if issuer pattern doesn't match
COUNTRY_PATTERNS = [
    (r"CHINA|HONG KONG|AGRI.*BANK.*CHINA", "China"),
    (r"ABU DHABI", "UAE"),
    (r"KFW|KREDITANSTALT", "Germany"),
    (r"GACI|SAUDI", "Saudi Arabia"),
    (r"EXPORT.IMPORT.*CHINA", "China"),
]


def _match_issuer(description: str) -> dict | None:
    """Match description against known issuer patterns."""
    for pattern, meta in ISSUER_PATTERNS:
        if re.search(pattern, description, re.IGNORECASE):
            return meta
    return None


def _guess_country(description: str) -> str:
    desc_upper = description.upper()
    for pattern, country in COUNTRY_PATTERNS:
        if re.search(pattern, desc_upper):
            return country
    return "Other"


def _parse_coupon(description: str) -> float:
    if not description:
        return 0.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", description)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+\.\d{2,})\s+\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", description)
    if m:
        return float(m.group(1))
    return 0.0


def _parse_maturity(description: str) -> str | None:
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", description)
    if not m:
        return None
    a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if c < 100:
        c += 2000
    # Disambiguate MM/DD/YYYY vs DD/MM/YYYY: if a > 12 it must be DD
    if a > 12:
        return f"{c:04d}-{b:02d}-{a:02d}"
    if b > 12:
        return f"{c:04d}-{a:02d}-{b:02d}"
    # Default to MM/DD/YYYY (US format, matches InvestOne output)
    return f"{c:04d}-{a:02d}-{b:02d}"


def _clean_description(desc: str, coupon: float, maturity: str | None) -> str:
    """Build a clean short description like 'Agricultural Bank of China 2.80% 07/2027'."""
    # Strip the raw InvestOne description noise
    clean = desc.strip()
    # Remove trailing "USD200000" or similar
    clean = re.sub(r"\s+(USD|CNY|CNH|EUR|GBP)\d+\s*$", "", clean, flags=re.I)
    # Remove long decimal coupons like "2.800000" (we'll add our own)
    clean = re.sub(r"\s+\d+\.\d{4,}\s+\d{1,2}/\d{1,2}/\d{2,4}", "", clean)
    # Remove "GTD SNR EMTN" type noise
    clean = re.sub(r"\s+(GTD\s+)?SNR\s+EMTN\b", "", clean, flags=re.I)
    # Remove "Ltd/Hong Kong", "Ltd/The" suffixes
    clean = re.sub(r"\s+Ltd(/\w+)?", "", clean, flags=re.I)
    clean = clean.strip()
    # Add coupon + maturity suffix
    suffix_parts = []
    if coupon:
        suffix_parts.append(f"{coupon:.2f}%")
    if maturity:
        suffix_parts.append(maturity[:7].replace("-", "/"))  # YYYY/MM
    if suffix_parts:
        # Don't duplicate if already present
        if f"{coupon:.2f}%" not in clean:
            clean = clean + " " + " ".join(suffix_parts)
    return clean


# Waystone fund name (report header) → (portfolio_id, display name).
# Module-level: the server's mail-ingest dedupe reconstructs per-fund
# filename prefixes from this map. Add new funds HERE (one line) and the
# whole pipeline — zip ingest, storage routing, Excel — picks them up.
FUND_MAP = {
    "GUINNESS CHINA RMB INCOME FUND": ("gcrif", "Guinness China RMB Income Fund"),
    "GUINNESS GLOBAL DYNAMIC BOND FUND": ("gdbf", "Guinness Global Dynamic Bond Fund"),
}


def _accrued_sheet(source) -> str:
    """Name of the accrued-income recon sheet in this workbook.

    It was hardcoded to "LFAI_Accrued_Income_Recon", which is GCRIF's Link-era
    name. GDBF's is "WFAI_Accrued_Income_Recon", and because the only caller sits
    inside a bare try/except, GDBF's entire per-holding accrued-income and coupon
    extraction failed SILENTLY — accrued_by_sedol and coupon_by_sedol just came
    back empty with no error.

    The administrator also rebranded Link -> Waystone between 2026-07-16 and
    2026-07-21, so GCRIF's own history spans both prefixes. Match on the stable
    suffix rather than either literal.
    """
    try:
        if isinstance(source, BytesIO):
            source.seek(0)
        names = pd.ExcelFile(source).sheet_names
        for n in names:
            if str(n).upper().endswith("_ACCRUED_INCOME_RECON"):
                return n
    except Exception:
        pass
    finally:
        if isinstance(source, BytesIO):
            source.seek(0)
    return "LFAI_Accrued_Income_Recon"   # legacy default; caller handles absence


def _norm_class_name(name) -> str:
    """Normalise a share-class name for cross-sheet joins.

    OpenCurrency block headers ("Class C EUR Hedged Dist CLASS") and
    Share_Class_Price_Report["Share Class"] ("CLASS C EUR HEDGED DIST" or,
    on one fund, "CLASS C EUR  ACCUMULATION" with a double space) must match
    byte-for-byte after case-folding and whitespace collapse — see
    docstring on _parse_open_currency.
    """
    return re.sub(r"\s+", " ", str(name).strip()).upper()


def _parse_share_classes(source) -> list[dict]:
    """Parse Share_Class_Price_Report (header row 16, data from 17).

    Columns (0-indexed): 0 Share Class, 1 ISIN, 2 Class Currency, 3 FX Rate,
    4 Shares, 5 NAV Price, 6 NAV Price (Prev), 7 Difference, 8 % Diff,
    9 Net Assets (Local), 10 Net Assets (Base), 11 % of Fund, 13 Account
    Class. Col 3 FX Rate's direction (base-per-class-ccy vs class-ccy-per-
    base) is NOT consistent across funds — it is returned as-is, never
    inverted or assumed.

    `hedged` is left None here — it is filled in by the caller from
    OpenCurrency block membership, which is the AUTHORITATIVE signal. The
    "HEDGED" substring in the class name is only ever used as a cross-check.
    """
    share_classes = []
    try:
        if isinstance(source, BytesIO):
            source.seek(0)
        df_sc = pd.read_excel(source, sheet_name="Share_Class_Price_Report", header=None)
        header_row = None
        for i, row in df_sc.iterrows():
            for val in row:
                if pd.notna(val) and str(val).strip().upper() == "SHARE CLASS":
                    header_row = i
                    break
            if header_row is not None:
                break
        if header_row is None:
            logger.warning("Share_Class_Price_Report: no 'Share Class' header row found")
            return share_classes

        header_row_vals = df_sc.iloc[header_row]
        colmap = {}
        for idx, val in enumerate(header_row_vals):
            if pd.notna(val) and str(val).strip():
                colmap.setdefault(str(val).strip(), idx)

        def _get(row, key):
            idx = colmap.get(key)
            if idx is None or idx >= len(row):
                return None
            v = row.iloc[idx]
            return v if pd.notna(v) else None

        def _num(row, key):
            v = _get(row, key)
            if v is None:
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        data = df_sc.iloc[header_row + 1:]
        for _, row in data.iterrows():
            name_val = _get(row, "Share Class")
            isin_val = _get(row, "ISIN")
            if name_val is None or isin_val is None:
                continue  # blank separator / totals row
            name = str(name_val).strip()
            isin = str(isin_val).strip()
            if not name or len(isin) != 12:
                continue
            ccy_val = _get(row, "Class Currency")
            share_classes.append({
                "share_class": name,
                "isin": isin,
                "class_currency": str(ccy_val).strip() if ccy_val is not None else None,
                "fx_rate": _num(row, "FX Rate"),
                "shares": _num(row, "Shares"),
                "nav_price": _num(row, "NAV Price"),
                "nav_price_prev": _num(row, "NAV Price (Prev)"),
                "difference": _num(row, "Difference"),
                "pct_diff": _num(row, "% Diff"),
                "net_assets_local": _num(row, "Net Assets (Local)"),
                "net_assets_base": _num(row, "Net Assets (Base)"),
                "pct_of_fund": _num(row, "% of Fund"),
                "account_class": _num(row, "Account Class"),
                "hedged": None,             # filled from OpenCurrency (authoritative)
                "hedged_name_hint": "HEDGED" in name.upper(),  # cross-check only
            })
    except Exception as e:
        logger.warning("Could not parse share classes: %s", e)
    return share_classes


def _parse_open_currency(source, base_currency: str, share_class_index: dict) -> tuple[list, dict, dict]:
    """Stateful row scan over the OpenCurrency sheet.

    OpenCurrency is NOT a flat table — it is a sequence of section/owner
    marker rows (all in column 0, other columns NaN) interleaved with data
    rows and subtotal rows. A column-filter approach (the previous
    implementation) cannot tell a fund-level forward from a share-class
    forward, and hardcoded the currency allow-list to ("USD", "CNH") which
    silently dropped EUR/GBP legs on every non-GCRIF fund.

    Grammar (col indices refer to the 12-col OpenCurrency layout: Currency,
    Broker, Trans No, Effective Date, Trade Date, Settlement Date, Value of
    Trade Local, Contract Rate, Valuation Rate, Contract Value Base, Market
    Value Base, Unrealised P/L Base):
      col0 in {"FORWARD CONTRACTS - HEDGE", "SPOT CONTRACTS"} -> section marker
      col0 == "MAIN ACCOUNT"                    -> owner = fund-level
      col0 endswith " CLASS" and not startswith "TOTAL " -> owner = share class
      col0 startswith "TOTAL "                  -> block subtotal (col11 = P/L); clear owner
      col0 is a 3-letter code, cols1-11 all NaN -> leg-currency sub-heading
      col0 is a 3-letter code, col1 non-numeric -> DATA ROW (one leg of a forward)
      col0 NaN, col5 startswith "Total: "       -> per-currency subtotal, skip
      col7 contains "Total Unrealised P/L:"     -> section grand total
      col0 startswith "EXCHANGE RATES"          -> stop the trade scan; spot +
                                                    forward-point grid follows
                                                    (note: some funds mis-stamp
                                                    this "EXCHANGE RATES1", and
                                                    a handful say "No Exchange
                                                    Rates available..." instead
                                                    of a real grid)

    Each forward trade is TWO rows sharing a Trans No within the same owner
    block (one leg in the fund base currency, Contract Rate == Valuation
    Rate == 1 and zero P/L; the other leg carries the real rates and the
    entire Unrealised P/L Base). They are paired here by (owner, Trans No).

    Returns (fx_forwards, hedge_ledger, exchange_rates).
    """
    fx_forwards: list = []
    exchange_rates: dict = {}
    hedge_ledger = {
        "fund_level_pl": 0.0,
        "share_class_pl": 0.0,
        "by_share_class": [],
        "total_pl": 0.0,
        "reconciles": None,
        "balance_sheet_pl": None,
        "difference": None,
        "grand_total_from_sheet": None,
    }

    try:
        if isinstance(source, BytesIO):
            source.seek(0)
        df = pd.read_excel(source, sheet_name="OpenCurrency", header=None)
    except Exception as e:
        logger.warning("Could not read OpenCurrency sheet: %s", e)
        return fx_forwards, hedge_ledger, exchange_rates

    if df.shape[1] < 12 or df.shape[0] == 0:
        return fx_forwards, hedge_ledger, exchange_rates

    def _cell(row, j):
        v = row.get(j) if hasattr(row, "get") else None
        return v if pd.notna(v) else None

    def _f(row, j):
        v = _cell(row, j)
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    def _dt(row, j):
        v = _cell(row, j)
        if v is None:
            return None
        try:
            return pd.to_datetime(v).strftime("%Y-%m-%d")
        except Exception:
            return None

    # Find the "Currency | Broker | ..." header row so we start the scan
    # after it regardless of which row index it lands on this fund.
    start = 0
    for i in range(min(30, len(df))):
        v0 = _cell(df.iloc[i], 0)
        v1 = _cell(df.iloc[i], 1)
        if v0 is not None and str(v0).strip() == "Currency" and v1 is not None and str(v1).strip() == "Broker":
            start = i + 1
            break

    section = None
    owner_type = None       # "fund" | "share_class" | None
    owner_name = None
    owner_isin = None
    owner_account_class = None

    trades_by_key: dict = {}   # (owner_type, owner_name, trans_no) -> {legs: [...]}
    block_totals: list = []    # one entry per "TOTAL ..." row encountered
    hedged_class_names: set = set()
    exchange_grid_start = None
    n_rows = len(df)

    for i in range(start, n_rows):
        row = df.iloc[i]
        c0_raw = _cell(row, 0)
        c0 = str(c0_raw).strip() if c0_raw is not None else ""
        rest_nan = all(_cell(row, j) is None for j in range(1, 12))

        if c0.startswith("EXCHANGE RATES"):
            exchange_grid_start = i
            break

        if c0 in ("FORWARD CONTRACTS - HEDGE", "SPOT CONTRACTS") and rest_nan:
            section = c0
            owner_type = owner_name = owner_isin = owner_account_class = None
            continue

        if c0 == "MAIN ACCOUNT" and rest_nan:
            owner_type, owner_name = "fund", "MAIN ACCOUNT"
            owner_isin = owner_account_class = None
            continue

        if c0.endswith(" CLASS") and not c0.startswith("TOTAL ") and rest_nan:
            cls_name = c0[: -len(" CLASS")].strip()
            owner_type, owner_name = "share_class", cls_name
            sc = share_class_index.get(_norm_class_name(cls_name))
            if sc:
                owner_isin = sc.get("isin")
                owner_account_class = sc.get("account_class")
            else:
                owner_isin = owner_account_class = None
                logger.warning(
                    "OpenCurrency class block %r has no matching row in "
                    "Share_Class_Price_Report", cls_name,
                )
            hedged_class_names.add(cls_name)
            continue

        if c0.startswith("TOTAL "):
            pl = _f(row, 11) or 0.0
            label = c0[len("TOTAL "):].strip()
            is_fund = (owner_type == "fund") or (label == "MAIN ACCOUNT")
            block_totals.append({
                "owner_type": "fund" if is_fund else "share_class",
                "owner_name": owner_name or label,
                "isin": owner_isin,
                "account_class": owner_account_class,
                "pl": pl,
                "section": section,
            })
            owner_type = owner_name = owner_isin = owner_account_class = None
            continue

        if c0 == "" and _cell(row, 5) is not None and str(_cell(row, 5)).strip().startswith("Total: "):
            continue  # per-currency subtotal — informational only, not a ledger line

        if c0 == "" and _cell(row, 7) is not None and "Total Unrealised P/L:" in str(_cell(row, 7)):
            gt = _f(row, 11)
            if gt is not None:
                hedge_ledger["grand_total_from_sheet"] = (
                    gt if hedge_ledger["grand_total_from_sheet"] is None
                    else hedge_ledger["grand_total_from_sheet"] + gt
                )
            continue

        if len(c0) == 3 and c0.isalpha() and rest_nan:
            continue  # leg-currency sub-heading

        if len(c0) == 3 and c0.isalpha() and _cell(row, 1) is not None:
            broker_val = str(_cell(row, 1)).strip()
            is_broker = bool(broker_val) and not broker_val.replace(".", "").replace("-", "").isdigit()
            if not is_broker:
                continue
            trans_no = str(_cell(row, 2)).strip() if _cell(row, 2) is not None else ""
            contract_rate = _f(row, 7)
            val_rate = _f(row, 8)
            leg = {
                "currency": c0,
                "broker": broker_val,
                "trans_no": trans_no,
                "effective_date": _dt(row, 3),
                "trade_date": _dt(row, 4),
                "settlement_date": _dt(row, 5),
                "value_of_trade_local": _f(row, 6) or 0.0,
                "contract_rate": contract_rate,
                "valuation_rate": val_rate,
                "contract_value_base": _f(row, 9) or 0.0,
                "market_value_base": _f(row, 10) or 0.0,
                "unrealised_pl_base": _f(row, 11) or 0.0,
                "is_base_leg": (contract_rate == 1 and val_rate == 1),
            }
            key = (owner_type, owner_name, trans_no)
            entry = trades_by_key.setdefault(key, {
                "owner_type": owner_type,
                "owner_name": owner_name,
                "share_class_isin": owner_isin,
                "account_class": owner_account_class,
                "section": section,
                "trans_no": trans_no,
                "legs": [],
            })
            entry["legs"].append(leg)
            continue
        # blank separator rows, stray text — ignored

    # ─── Reassemble trades from their legs ─────────────────────────────
    for entry in trades_by_key.values():
        legs = entry["legs"]
        base_leg = next((l for l in legs if l["is_base_leg"]), None)
        non_base_legs = [l for l in legs if not l["is_base_leg"]]
        settlement_date = next((l["settlement_date"] for l in legs if l["settlement_date"]), None)
        pnl_legs = non_base_legs or legs
        unrealised_pl = sum(l["unrealised_pl_base"] for l in pnl_legs)
        fx_forwards.append({
            "owner_type": entry["owner_type"],
            "owner_name": entry["owner_name"],
            "share_class_isin": entry["share_class_isin"],
            "account_class": entry["account_class"],
            "section": entry["section"],
            "trans_no": entry["trans_no"],
            "settlement_date": settlement_date,
            "unrealised_pl_base": unrealised_pl,
            "base_leg": base_leg,
            "non_base_legs": non_base_legs,
            "legs": legs,
        })

    # ─── Ledger: fund-level vs share-class P/L, from the TOTAL rows ────
    # Only "FORWARD CONTRACTS - HEDGE" blocks feed the ledger — the
    # reconciliation anchor is Balance_Sheet's "Unrealized Gain/Loss on
    # FORWARD Contracts", which does NOT include SPOT CONTRACTS (funds that
    # also settle spot FX for trade purposes, e.g. equity funds buying
    # foreign securities, carry a separate MAIN ACCOUNT total under a
    # "SPOT CONTRACTS" section that must not be commingled with hedge P/L).
    by_class: dict = {}
    fund_pl = 0.0
    class_pl = 0.0
    for b in block_totals:
        if b["section"] != "FORWARD CONTRACTS - HEDGE":
            continue
        if b["owner_type"] == "fund":
            fund_pl += b["pl"]
        else:
            class_pl += b["pl"]
            rec = by_class.setdefault(b["owner_name"], {
                "name": b["owner_name"], "isin": b["isin"],
                "account_class": b["account_class"], "pl": 0.0, "currencies": set(),
            })
            rec["pl"] += b["pl"]
            if rec["isin"] is None:
                rec["isin"] = b["isin"]
            if rec["account_class"] is None:
                rec["account_class"] = b["account_class"]

    for fwd in fx_forwards:
        if fwd["owner_type"] == "share_class" and fwd["owner_name"] in by_class:
            for l in fwd["legs"]:
                by_class[fwd["owner_name"]]["currencies"].add(l["currency"])

    by_share_class_list = []
    for rec in by_class.values():
        rec["currencies"] = sorted(rec["currencies"])
        by_share_class_list.append(rec)
    by_share_class_list.sort(key=lambda r: r["name"])

    hedge_ledger["fund_level_pl"] = fund_pl
    hedge_ledger["share_class_pl"] = class_pl
    hedge_ledger["by_share_class"] = by_share_class_list
    hedge_ledger["total_pl"] = fund_pl + class_pl
    hedge_ledger["_hedged_class_names"] = hedged_class_names  # consumed by caller, stripped after

    # ─── Exchange rate grid (spot + forward points) ────────────────────
    if exchange_grid_start is not None and exchange_grid_start + 1 < n_rows:
        hdr = df.iloc[exchange_grid_start + 1]
        hdr0 = _cell(hdr, 0)
        if hdr0 is not None and str(hdr0).strip() == "Currency":
            for i in range(exchange_grid_start + 2, n_rows):
                row = df.iloc[i]
                ccy = str(_cell(row, 0)).strip() if _cell(row, 0) is not None else ""
                if not ccy or len(ccy) != 3 or not ccy.isalpha():
                    continue
                exchange_rates[ccy] = {
                    "spot": _f(row, 1),
                    "30d": _f(row, 2),
                    "60d": _f(row, 3),
                    "90d": _f(row, 4),
                    "180d": _f(row, 5),
                    # Waystone leaves this 0/unpopulated for non-base currencies
                    "365d": _f(row, 6),
                }
        # else: "No Exchange Rates available..." placeholder — leave grid empty

    return fx_forwards, hedge_ledger, exchange_rates


_BS_DETAIL_LINES = (
    "Capital Shares Sold",
    "Capital Shares Redeemed",
    "Investment Securities Purchased",
    "Investment Securities Sold",
    "Accrued Expenses",
)


def _issuer_key_from_description(description: str) -> str:
    """Issuer name for grouping, stripping the trailing coupon/maturity tokens
    off a bond description, e.g. 'Electricite de France SA 7.375000 7.38%'
    -> 'Electricite de France SA'. Cuts at the first number-leading token, so
    it degrades to the SEDOL fallback (never silently mis-groups) for the
    rare issuer name that itself starts with a digit."""
    m = re.match(r'^(.*?)\s+[\d.]', description.strip())
    return (m.group(1) if m else description).strip()


def _group_by_issuer(holdings: list[dict]) -> dict[str, list[dict]]:
    """Group holdings by issuer. Shared by compute_issuer_exposure and
    compute_compliance so the two never drift on what counts as one issuer.

    `ticker` falls back to the raw SEDOL in parse_nav_report whenever the
    bond isn't in the (GCRIF-specific) BOND_METADATA/regex table — true for
    essentially every GDBF holding. Grouping by a per-bond SEDOL isn't
    grouping at all: two tranches of the same issuer would each look like a
    separate sub-5% issuer instead of summing toward the limit. Fall back to
    an issuer name pulled from the description instead of the SEDOL."""
    groups: dict[str, list[dict]] = {}
    for h in holdings:
        ticker = h.get("ticker") or ""
        if not ticker or ticker == h.get("sedol"):
            key = _issuer_key_from_description(h.get("description") or "") \
                or h.get("isin") or h.get("sedol") or "?"
        else:
            key = ticker
        groups.setdefault(key, []).append(h)
    return groups


def _is_sovereign_group(group: list[dict]) -> bool:
    """See compute_issuer_exposure's docstring for the accuracy caveat."""
    return any(g.get("is_cgb") or g.get("asset_type") == "Government" for g in group)


def compute_compliance(holdings: list[dict], net_cash: float, total_nav: float) -> dict:
    """UCITS-style compliance check for a stored NAV-payload fund (GCRIF,
    GDBF, ...), mirroring 6 of athena_mcp's 7 check_compliance() rules (max
    single issuer, sum-over-5%, cash overdrawn, cash level, max country,
    diversification) but computed from the same holdings weights
    compute_issuer_exposure already uses instead of transaction-derived data
    — see that function's docstring for the sovereign-exemption caveat,
    which applies here too.

    Deliberately NOT implemented: the NFA 3*+ country-eligibility rule. It's
    a specific fund's investment policy (WNBF), not a general UCITS rule, so
    it doesn't apply to every NAV-only fund by default — and the real
    implementation lives in athena_mcp/tools/compliance.py, which this
    module has no import path to (separate repo). A fund whose policy does
    require it should get NFA wired deliberately, not a reimplementation
    guessed at here.
    """
    empty = {
        "is_compliant": True,
        "summary": {"hard_pass": 0, "hard_total": 0, "soft_pass": 0, "soft_total": 0},
        "rules": [], "country_chart": [],
        "violations": {"issuers_over_5": [], "nfa_violations": []},
        "issuer_weights": {}, "metrics": {},
    }
    if not holdings or not total_nav:
        return empty

    groups = _group_by_issuer(holdings)
    issuer_weights = {}
    max_position, max_position_ticker = 0.0, "N/A"
    issuers_over_5_details = []
    sum_over_5_pct, num_issuers_over_5 = 0.0, 0
    for ticker, group in groups.items():
        total_value = sum(g.get("market_value", 0) or 0 for g in group)
        pct = (total_value / total_nav * 100) if total_nav else 0
        issuer_weights[ticker] = pct
        is_sovereign = _is_sovereign_group(group)
        if is_sovereign:
            continue
        if pct > max_position:
            max_position, max_position_ticker = pct, ticker
        if pct > 5:
            num_issuers_over_5 += 1
            sum_over_5_pct += pct
            issuers_over_5_details.append({
                "ticker": ticker, "pct_nav": round(pct, 2),
                "country": group[0].get("country") or "",
                "total_market_value": round(total_value, 0),
                "bonds": [{"isin": g.get("isin"), "description": g.get("description"),
                          "market_value": g.get("market_value"), "par_amount": g.get("par_amount")}
                         for g in group],
            })
    issuers_over_5_details.sort(key=lambda i: i["pct_nav"], reverse=True)

    is_overdrawn = net_cash < 0
    cash_pct = (net_cash / total_nav * 100) if total_nav else 0

    country_totals: dict[str, float] = {}
    for h in holdings:
        c = h.get("country") or "Unknown"
        country_totals[c] = country_totals.get(c, 0) + (h.get("market_value") or 0)
    country_pcts = {c: (v / total_nav * 100) for c, v in country_totals.items()}
    max_country = max(country_pcts, key=country_pcts.get) if country_pcts else "Unknown"
    max_country_pct = country_pcts.get(max_country, 0)

    num_holdings = len(holdings)

    rules = [
        {"type": "Hard", "name": "Max Single Issuer (5/10/40)", "limit": "10%",
         "current": f"{max_position:.1f}%", "status": "pass" if max_position <= 10 else "fail",
         "details": max_position_ticker},
        {"type": "Hard", "name": "Sum Issuers >5% (5/10/40)", "limit": "40%",
         "current": f"{sum_over_5_pct:.1f}%", "status": "pass" if sum_over_5_pct <= 40 else "fail",
         "details": f"{num_issuers_over_5} issuers"},
        {"type": "Hard", "name": "Cash Overdrawn", "limit": ">= $0",
         "current": f"${net_cash:,.0f}", "status": "fail" if is_overdrawn else "pass",
         "details": "OVERDRAWN!" if is_overdrawn else "OK"},
        {"type": "Soft", "name": "Cash Level", "limit": "0-5%",
         "current": f"{cash_pct:.1f}%", "status": "pass" if 0 <= cash_pct <= 5 else "warning",
         "details": f"${net_cash:,.0f}"},
        {"type": "Soft", "name": "Max Country", "limit": "20%",
         "current": f"{max_country_pct:.1f}%", "status": "pass" if max_country_pct <= 20 else "warning",
         "details": max_country},
        {"type": "Soft", "name": "Diversification", "limit": "10+",
         "current": str(num_holdings), "status": "pass" if num_holdings >= 10 else "warning",
         "details": f"{num_holdings} bonds"},
    ]

    hard = [r for r in rules if r["type"] == "Hard"]
    soft = [r for r in rules if r["type"] == "Soft"]
    hard_pass = sum(1 for r in hard if r["status"] == "pass")
    soft_pass = sum(1 for r in soft if r["status"] == "pass")

    country_chart = [{"country": c, "pct": round(p, 1), "over_limit": p > 20}
                     for c, p in sorted(country_pcts.items(), key=lambda kv: kv[1], reverse=True)]

    return {
        "is_compliant": hard_pass == len(hard),
        "summary": {"hard_pass": hard_pass, "hard_total": len(hard),
                    "soft_pass": soft_pass, "soft_total": len(soft)},
        "rules": rules,
        "country_chart": country_chart,
        "violations": {"issuers_over_5": issuers_over_5_details, "nfa_violations": []},
        "issuer_weights": issuer_weights,
        "metrics": {
            "max_position": round(max_position, 2), "max_position_ticker": max_position_ticker,
            "max_country_pct": round(max_country_pct, 2), "max_country": max_country,
            "cash_pct": round(cash_pct, 2), "num_holdings": num_holdings,
            "sovereign_tickers": sorted(t for t, g in groups.items() if _is_sovereign_group(g)),
        },
    }


def compute_issuer_exposure(holdings: list[dict], total_nav: float) -> dict:
    """5/10/40 issuer-concentration aggregation for a stored NAV-payload fund
    (GCRIF, GDBF, ...). Same output shape as athena_mcp's transaction-derived
    get_issuer_exposure() so the existing issuer-page widgets (issuer-metrics,
    issuer-concentration-bar, issuer-detail-table) work unmodified.

    Sovereign exemption here is a simplified proxy — `is_cgb` (only ever set
    for GCRIF's hardcoded China-govvy table) OR `asset_type == "Government"`
    (the admin report's own Asset Grp "G" flag, fund-agnostic). That is
    coarser than athena_mcp's ISIN-first sovereign/quasi-sovereign/corporate
    classification (it exempts development banks like KFW/CHEXIM alongside
    true sovereigns), so treat the result as directional for these funds
    until they get real issuer-type classification, not audit-grade.
    """
    if not holdings or not total_nav:
        return {"issuers": [], "summary": {
            "issuers_over_5": 0, "total_over_5_pct": 0, "max_issuer_pct": 0,
            "unassessed_issuer_count": 0, "unassessed_issuer_tickers": [],
            "unassessed_pct_nav": 0, "rule_5_10_40_pass": True,
        }}

    groups = _group_by_issuer(holdings)

    issuers = []
    issuers_over_5 = 0
    total_over_5_pct = 0.0
    max_issuer_pct = 0.0
    for ticker, group in groups.items():
        total_value = sum(g.get("market_value", 0) or 0 for g in group)
        pct = (total_value / total_nav * 100) if total_nav else 0
        is_sovereign = _is_sovereign_group(group)
        if not is_sovereign:
            max_issuer_pct = max(max_issuer_pct, pct)
        over_5 = pct > 5 and not is_sovereign
        over_10 = pct > 10 and not is_sovereign
        if over_5:
            issuers_over_5 += 1
            total_over_5_pct += pct
        issuers.append({
            "ticker": ticker,
            "country": (group[0].get("country") or ""),
            "total_pct": round(pct, 2),
            "total_value": round(total_value, 0),
            "bond_count": len(group),
            "over_5": over_5, "over_10": over_10,
            "is_sovereign": is_sovereign, "is_unassessed": False,
        })

    issuers.sort(key=lambda i: i["total_pct"], reverse=True)
    return {
        "issuers": issuers,
        "summary": {
            "issuers_over_5": issuers_over_5,
            "total_over_5_pct": round(total_over_5_pct, 1),
            "max_issuer_pct": round(max_issuer_pct, 1),
            "unassessed_issuer_count": 0,
            "unassessed_issuer_tickers": [],
            "unassessed_pct_nav": 0,
            "rule_5_10_40_pass": (max_issuer_pct <= 10) and (total_over_5_pct <= 40),
        },
    }


def parse_nav_report(file_path_or_bytes) -> dict:
    """
    Parse a Guinness China RMB Income Fund NAV report XLS.

    Args:
        file_path_or_bytes: Path to XLS file, or bytes/BytesIO of the file

    Returns:
        dict matching the Athena GCRIF data format with keys:
        valuation_date, holdings, summary, countries, fx_rates, fx_forwards, share_classes
    """
    if isinstance(file_path_or_bytes, (str, Path)):
        source = str(file_path_or_bytes)
    elif isinstance(file_path_or_bytes, bytes):
        source = BytesIO(file_path_or_bytes)
    else:
        source = file_path_or_bytes  # assume file-like

    # ─── 0. Fund identity from the report header ─────────────────────────
    # Waystone/InvestOne reports carry FUND NAME / FUND BASE CCY rows on every
    # sheet header. Reading them (instead of assuming GCRIF) lets the same
    # parser ingest any Guinness fund — GDBF arrives next. Unknown funds keep
    # the GCRIF defaults so existing behaviour is unchanged.
    fund_name_raw, base_ccy_raw, valuation_time_raw = None, None, None
    try:
        if isinstance(source, BytesIO):
            source.seek(0)
        _head = pd.read_excel(source, sheet_name=0, header=None, nrows=15)
        for _, _row in _head.iterrows():
            for _ci in range(len(_row) - 1):
                _cell = str(_row[_ci]).strip().upper() if pd.notna(_row[_ci]) else ""
                if _cell.startswith("FUND NAME"):
                    for _vj in range(_ci + 1, len(_row)):
                        if pd.notna(_row[_vj]) and str(_row[_vj]).strip():
                            fund_name_raw = str(_row[_vj]).strip()
                            break
                elif _cell.startswith("FUND BASE CCY"):
                    for _vj in range(_ci + 1, len(_row)):
                        if pd.notna(_row[_vj]) and str(_row[_vj]).strip():
                            base_ccy_raw = str(_row[_vj]).strip().upper()
                            break
                elif _cell.startswith("VALUATION TIME"):
                    # Waystone/InvestOne stamps the fund's valuation point here
                    # (GDBF 12:00:00, GCRIF and the other 18 Guinness funds
                    # 23:00:00). Capturing it lets fund_config.py's declared
                    # value be CHECKED against the administrator's own report on
                    # every ingest, instead of being hardcoded and hoped.
                    for _vj in range(_ci + 1, len(_row)):
                        if pd.notna(_row[_vj]) and str(_row[_vj]).strip():
                            valuation_time_raw = str(_row[_vj]).strip()
                            break
    except Exception:
        pass
    _pid, _fund_display = FUND_MAP.get((fund_name_raw or "").upper(), (None, None))
    if isinstance(source, BytesIO):
        source.seek(0)

    # ─── 1. Balance Sheet → valuation date, cash, total NAV ──────────────
    df_bs = pd.read_excel(source, sheet_name="Balance_Sheet", header=None)
    valuation_date = None
    cash_cnh = 0.0
    foreign_currency = 0.0
    total_net_assets = 0.0
    total_accrued_income = 0.0
    bs_fwd_gain = None
    bs_fwd_loss = None
    # Balance Sheet receivable/payable lines, kept so callers can attribute the
    # non-security part of NAV to a currency.
    bs_detail: dict = {}

    for _, row in df_bs.iterrows():
        label = str(row[0]).strip() if pd.notna(row[0]) else ""
        # Sub-items (e.g. the two forward-contract lines) are indented one
        # column in from top-level items like "Cash" — check both.
        label1 = str(row[1]).strip() if pd.notna(row[1]) else ""
        # Row 4 has date headers like "23-Jan-26" and "26-Jan-26"
        if not valuation_date:
            for col_idx in range(len(row)):
                val = row[col_idx]
                if pd.notna(val) and isinstance(val, str):
                    m = re.match(r"(\d{1,2})-(\w{3})-(\d{2,4})", val.strip())
                    if m:
                        try:
                            valuation_date = pd.to_datetime(val.strip(), format="%d-%b-%y").strftime("%Y-%m-%d")
                        except Exception:
                            try:
                                valuation_date = pd.to_datetime(val.strip(), format="%d-%b-%Y").strftime("%Y-%m-%d")
                            except Exception:
                                pass
        if label == "Cash":
            # Last numeric column is latest valuation
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        cash_cnh = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif label == "Foreign Currency":
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        foreign_currency = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif (label or label1) in _BS_DETAIL_LINES:
            # Receivable/payable lines needed to attribute the non-security part
            # of NAV to a currency. Captured generically because each is a
            # single Balance Sheet figure with no currency of its own — the
            # currency has to come from what the line REPRESENTS (which class
            # subscribed, which bonds were bought), and a caller can only work
            # that out if it can see the amounts.
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        bs_detail[(label or label1)] = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif label == "Accrued Income" or label1 == "Accrued Income":
            # Indented one column in, under the "Receivables" heading — this
            # only tested column 0, so GDBF's 186,969.64 of accrued income read
            # as zero while its holdings plainly carried it.
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        total_accrued_income = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif "Net Assets" in label or "Total Net Assets" in label or "NET ASSETS" in label.upper():
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        total_net_assets = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif label == "Unrealized Gain on Forward Contracts" or label1 == "Unrealized Gain on Forward Contracts":
            # Reconciliation anchor for the FX-hedge ledger below: this plus
            # the matching "Loss" line must equal fund_level_pl + share_class_pl
            # summed off the OpenCurrency TOTAL rows.
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        bs_fwd_gain = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass
        elif label == "Unrealized Loss on Forward Contracts" or label1 == "Unrealized Loss on Forward Contracts":
            for c in [5, 4, 3]:
                if pd.notna(row.get(c)):
                    try:
                        bs_fwd_loss = float(row[c])
                        break
                    except (ValueError, TypeError):
                        pass

    # Also try extracting valuation date from the accrued-income sheet
    # (more reliable format)
    if not valuation_date:
        try:
            df_lfai_head = pd.read_excel(source, sheet_name=_accrued_sheet(source), header=None, nrows=10)
            for _, row in df_lfai_head.iterrows():
                if str(row.get(0, "")).strip().startswith("Valuation Date"):
                    val = row[1]
                    if pd.notna(val):
                        valuation_date = pd.to_datetime(val).strftime("%Y-%m-%d")
                        break
        except Exception:
            pass

    if not valuation_date:
        raise ValueError("Could not extract valuation date from NAV report")

    total_cash = cash_cnh + foreign_currency
    # Cash as the ADMIN reports it, captured before total_cash is overwritten
    # further down with the NAV-minus-securities residual. This is the figure a
    # cash reconciliation has to use — the residual is not cash.
    bs_cash_total = total_cash

    # ─── 2. Accrued income + coupon per holding ────────────────────────────
    accrued_by_sedol = {}
    coupon_by_sedol = {}
    try:
        df_ai = pd.read_excel(source, sheet_name=_accrued_sheet(source), header=None)

        # Find the column indices by scanning for header keywords. The sheet
        # may have prefix columns (grouping/summary) that shift indices per
        # fund, so this is a column *position*, not a currency-specific
        # value — but the historical fallback of 17 happened to be right for
        # GCRIF's layout by coincidence and is NOT guaranteed for other
        # funds' base currencies. If the header scan fails, log it loudly
        # rather than silently trusting the GCRIF-shaped fallback.
        local_col = None
        for _, row in df_ai.iterrows():
            vals = {i: str(v).strip().upper() for i, v in enumerate(row) if pd.notna(v)}
            for i, v in vals.items():
                if 'GROSS INCOME' in v and 'LOCAL' in v and 'WITHHOLDING' not in v:
                    local_col = i
                    logger.info("LFAI: Found Gross Income (Local) at column %d", i)
                    break
            if local_col is not None:
                break
        if local_col is None:
            local_col = 17
            logger.warning(
                "Accrued-income sheet: 'Gross Income (Local)' header not found; "
                "falling back to column 17 (GCRIF-era default) — accrued_by_sedol "
                "may be wrong for this fund"
            )

        for _, row in df_ai.iterrows():
            if pd.notna(row.get(3)) and str(row.get(3)).strip() == "detail":
                sedol = str(row.get(5, "")).strip()
                # Read accrued from Gross Income (Local) column
                accrued_local = 0.0
                if pd.notna(row.get(local_col)):
                    try:
                        accrued_local = float(row[local_col])
                    except (ValueError, TypeError):
                        pass
                # Column 10 = coupon rate as string like "2.8000%"
                if pd.notna(row.get(10)):
                    coupon_str = str(row[10]).replace("%", "").strip()
                    try:
                        coupon_by_sedol[sedol] = float(coupon_str)
                    except (ValueError, TypeError):
                        pass
                if sedol:
                    accrued_by_sedol[sedol] = accrued_local
    except Exception as e:
        logger.warning("Could not parse accrued income sheet: %s", e)

    # ─── 3. Holdings from Detailed_Security_Valuation ────────────────────
    # Some legacy/malformed reports (e.g. a handful of pre-Waystone GCRIF
    # exports) ship generic "Report-N" sheet names instead of this one. Fall
    # back to zero holdings rather than raising, so the rest of the parse
    # (FX hedge ledger, share classes, balance sheet) still completes.
    try:
        if isinstance(source, BytesIO):
            source.seek(0)
        df_h = pd.read_excel(source, sheet_name="Detailed_Security_Valuation", header=14)
        df_h = df_h[df_h["InvestOne Identifier"].notna()]
        df_h = df_h[~df_h["InvestOne Identifier"].astype(str).str.contains(
            "Total|Investments|Unknown|Spot FX", na=False
        )]
        # Keep only rows that have actual holdings (positive MV)
        df_h = df_h[pd.to_numeric(df_h.get("Market Value - Base", pd.Series()), errors="coerce").notna()]
    except Exception as e:
        logger.warning("Could not parse Detailed_Security_Valuation (malformed/legacy report?): %s", e)
        df_h = pd.DataFrame(columns=["Market Value - Base"])

    total_securities_mv = float(df_h["Market Value - Base"].sum()) if len(df_h) else 0.0

    # If total_net_assets wasn't found in balance sheet, estimate it
    if total_net_assets == 0:
        total_net_assets = total_securities_mv + total_cash + total_accrued_income
    else:
        # Dashboard "cash" is the residual non-security NAV so that bonds +
        # cash reconciles exactly to NAV and to the report's % of Fund. Do not
        # add the Balance Sheet "Foreign Currency" asset line: it is offset by
        # FX liabilities/forwards elsewhere and is not free portfolio cash.
        #
        # KEEP THE RESIDUAL, BUT DO NOT CALL IT CASH ANYWHERE THAT MATTERS. It
        # is every non-security component of NAV added together — real cash,
        # accrued income, FX forward MTM and net receivables. On GDBF at
        # 2026-07-24 the residual is 337,277.77 while the admin's actual Cash
        # line is 67,623.48; the other 269,654.29 is accrued (186,969.64), FX
        # forwards (-9,136.57) and receivables (+91,821.42). Reconciling a
        # residual against another system's cash figure compares two different
        # things, so the true components are published beside it below.
        cash_residual = max(0.0, total_net_assets - total_securities_mv)
        total_cash = cash_residual

    holdings = []
    for _, row in df_h.iterrows():
        sedol = str(row["InvestOne Identifier"]).strip()
        isin = str(row["ISIN/Bloomberg Ticker"]).strip() if pd.notna(row.get("ISIN/Bloomberg Ticker")) else ""
        raw_desc = str(row["Security Description"]).strip() if pd.notna(row.get("Security Description")) else ""
        asset_grp = str(row["Asset Grp"]).strip() if pd.notna(row.get("Asset Grp")) else ""
        currency = str(row["Ccy"]).strip() if pd.notna(row.get("Ccy")) else ""
        par_amount = float(row["Holding"]) if pd.notna(row.get("Holding")) else 0
        price = float(row["Price (Local)"]) if pd.notna(row.get("Price (Local)")) else 0
        cost_basis = float(row["Cost Basis"]) if pd.notna(row.get("Cost Basis")) else 0
        market_value = float(row["Market Value - Base"]) if pd.notna(row.get("Market Value - Base")) else 0
        unrealized_pnl = float(row["Unrealised Gain/Loss Base"]) if pd.notna(row.get("Unrealised Gain/Loss Base")) else 0

        if market_value <= 0:
            continue

        # Identify issuer: SEDOL metadata first, then description pattern match
        meta = BOND_METADATA.get(sedol, {})
        if not meta:
            meta = _match_issuer(raw_desc) or {}
        ticker = meta.get("ticker", sedol)
        country = meta.get("country", _guess_country(raw_desc))
        is_cgb = meta.get("is_cgb", False)
        asset_type = meta.get("asset_type", "Government" if asset_grp == "G" else "Corporate")

        coupon = _parse_coupon(raw_desc) or coupon_by_sedol.get(sedol, 0.0)
        maturity_date = _parse_maturity(raw_desc) or meta.get("maturity")
        accrued_income = accrued_by_sedol.get(sedol, 0.0)

        # Build clean description from metadata name + coupon/maturity
        description = meta.get("name", "")
        if not description:
            description = _clean_description(raw_desc, coupon, maturity_date)
        elif coupon or maturity_date:
            parts = [description]
            if coupon:
                parts.append(f"{coupon:.2f}%")
            if maturity_date:
                parts.append(maturity_date[:7].replace("-", "/"))
            description = " ".join(parts)

        w = (market_value / total_net_assets) * 100 if total_net_assets > 0 else 0

        holdings.append({
            "sedol": sedol,
            "isin": isin,
            "description": description,
            "ticker": ticker,
            "country": country,
            "asset_type": asset_type,
            "currency": currency,
            "coupon": coupon,
            "maturity_date": maturity_date,
            "par_amount": par_amount,
            "price": price,
            "cost_basis": cost_basis,
            "market_value": market_value,
            "accrued_income": accrued_income,
            "unrealized_pnl": unrealized_pnl,
            "weight": w,
            "is_cgb": is_cgb,
        })

    holdings.sort(key=lambda h: h["market_value"], reverse=True)

    # ─── 4. Country allocation ───────────────────────────────────────────
    country_map = {}
    for h in holdings:
        c = h["country"]
        if c not in country_map:
            country_map[c] = {"value": 0, "count": 0}
        country_map[c]["value"] += h["market_value"]
        country_map[c]["count"] += 1

    countries = sorted(
        [
            {
                "country": c,
                "country_value": d["value"],
                "country_weight": (d["value"] / total_net_assets) * 100 if total_net_assets else 0,
                "bond_count": d["count"],
            }
            for c, d in country_map.items()
        ],
        key=lambda x: x["country_weight"],
        reverse=True,
    )

    # ─── 5. FX rates from Detailed_Security_Valuation (Spot FX at bottom) ─
    # base_ccy_raw is harvested from the report header in step 0 above (it
    # used to be ignored here in favour of a hardcoded "CNH", which is
    # simply wrong for every non-GCRIF fund — e.g. GDBF is USD-based).
    base_currency = base_ccy_raw or "CNH"
    if not base_ccy_raw:
        logger.warning(
            "Could not read FUND BASE CCY from report header; defaulting to "
            "CNH (GCRIF's base) — verify this is correct for this fund"
        )
    fx_rates = {"base": base_currency, "rates": {base_currency: 1.0}, "source": "NAV Report", "as_of": valuation_date}
    if base_currency in ("CNH", "CNY"):
        fx_rates["rates"]["CNH"] = 1.0
        fx_rates["rates"]["CNY"] = 1.0
    try:
        df_full = pd.read_excel(source, sheet_name="Detailed_Security_Valuation", header=None)
        for _, row in df_full.iterrows():
            id_val = str(row.get(4, "")).strip() if pd.notna(row.get(4)) else ""
            ccy_val = str(row.get(5, "")).strip() if pd.notna(row.get(5)) else ""
            if ccy_val in ("USD", "EUR", "GBP") and id_val:
                try:
                    rate = float(id_val)
                    if 0 < rate < 1:
                        fx_rates["rates"][ccy_val] = rate
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        logger.warning("Could not parse FX rates: %s", e)

    # ─── 6. Share classes from Share_Class_Price_Report ───────────────────
    # Parsed BEFORE the OpenCurrency scan so the (owner, Trans No) join below
    # can resolve each hedged-class forward block to its ISIN/Account Class.
    share_classes = _parse_share_classes(source)
    share_class_index = {_norm_class_name(sc["share_class"]): sc for sc in share_classes}

    # ─── 7. FX forwards + hedge ledger + forward curve from OpenCurrency ──
    # Stateful block scan (see _parse_open_currency docstring) — replaces the
    # old flat column-filter which hardcoded the currency allow-list to
    # ("USD", "CNH") and could not distinguish fund-level FX from FX
    # belonging to an individual hedged share class.
    fx_forwards, hedge_ledger, exchange_rates_grid = _parse_open_currency(
        source, base_currency, share_class_index
    )
    hedged_class_names = hedge_ledger.pop("_hedged_class_names", set())
    if exchange_rates_grid:
        fx_rates["forward_rates"] = exchange_rates_grid

    # Cross-check: OpenCurrency block membership is the AUTHORITATIVE signal
    # for which classes are FX-hedged. The "Hedged" substring in the class
    # name (captured as hedged_name_hint in _parse_share_classes) is only a
    # cross-check — log a mismatch instead of trusting either source blindly.
    for sc in share_classes:
        in_ledger = _norm_class_name(sc["share_class"]) in {_norm_class_name(n) for n in hedged_class_names}
        sc["hedged"] = in_ledger
        if in_ledger != sc["hedged_name_hint"]:
            logger.warning(
                "Share class %r: OpenCurrency membership (hedged=%s) disagrees "
                "with name-based hint (hedged=%s) — trusting OpenCurrency",
                sc["share_class"], in_ledger, sc["hedged_name_hint"],
            )

    # ─── 7b. Reconcile the hedge ledger against the Balance Sheet anchor ──
    # sum(TOTAL <class> CLASS col 11) + sum(TOTAL MAIN ACCOUNT col 11) must
    # equal Balance_Sheet "Unrealized Gain on Forward Contracts" +
    # "Unrealized Loss on Forward Contracts". No silent fallback: if this
    # doesn't tie out, surface it in the returned data and log a warning —
    # never quietly return a plausible-looking number.
    if bs_fwd_gain is not None or bs_fwd_loss is not None:
        bs_pl = (bs_fwd_gain or 0.0) + (bs_fwd_loss or 0.0)
        hedge_ledger["balance_sheet_pl"] = bs_pl
        diff = round(hedge_ledger["total_pl"] - bs_pl, 2)
        hedge_ledger["difference"] = diff
        hedge_ledger["reconciles"] = abs(diff) < 0.01
        if not hedge_ledger["reconciles"]:
            logger.warning(
                "FX hedge ledger does NOT reconcile to Balance Sheet: "
                "ledger total_pl=%.2f vs balance_sheet_pl=%.2f (diff=%.2f)",
                hedge_ledger["total_pl"], bs_pl, diff,
            )
    else:
        # No forward-contract lines on this fund's Balance Sheet at all
        # (e.g. equity funds with zero open forwards this period).
        hedge_ledger["balance_sheet_pl"] = None
        hedge_ledger["difference"] = None
        hedge_ledger["reconciles"] = (hedge_ledger["total_pl"] == 0.0)
        if hedge_ledger["total_pl"] != 0.0:
            logger.warning(
                "FX hedge ledger has non-zero P/L (%.2f) but no matching "
                "Balance Sheet forward-contract line was found to reconcile against",
                hedge_ledger["total_pl"],
            )

    # ─── 7c. Currency exposure + hedge ratio (CNH-base model) ─────────────
    # This model (bonds priced in CNY/CNH vs USD, hedged back to CNH) is
    # specific to GCRIF's portfolio shape and produces nonsense for any
    # other base currency (e.g. GDBF is USD-based and holds EUR/GBP-hedged
    # share classes, not USD/CNH fund-level hedges). Scope it to CNH-base
    # funds only rather than silently emitting wrong numbers for everyone
    # else; a proper base-vs-non-base generalisation is tracked as backlog.
    if base_currency in ("CNH", "CNY"):
        usd_bond_mv = sum(h["market_value"] for h in holdings if h.get("currency") == "USD")
        cny_bond_mv = sum(h["market_value"] for h in holdings if h.get("currency") in ("CNY", "CNH"))
        usd_fwd_mv = sum(
            (fwd.get("non_base_legs") or [{}])[0].get("market_value_base", 0)
            for fwd in fx_forwards
            if fwd.get("owner_type") == "fund" and any(l["currency"] == "USD" for l in fwd.get("legs", []))
        )
        usd_fwd_pnl = sum(
            fwd.get("unrealised_pl_base", 0)
            for fwd in fx_forwards
            if fwd.get("owner_type") == "fund" and any(l["currency"] == "USD" for l in fwd.get("legs", []))
        )
        cnh_fwd_mv = sum(
            (fwd.get("non_base_legs") or [{}])[0].get("market_value_base", 0)
            for fwd in fx_forwards
            if fwd.get("owner_type") == "fund" and any(l["currency"] in ("CNH", "CNY") for l in fwd.get("legs", []))
        )
        net_foreign = usd_bond_mv + usd_fwd_mv
        portfolio_hedge_pct = ((cny_bond_mv + total_cash + cnh_fwd_mv) / total_net_assets * 100) if total_net_assets else 0
        usd_hedge_pct = (abs(usd_fwd_mv) / usd_bond_mv * 100) if usd_bond_mv > 0 else 0

        currency_exposure = {
            "gross": {
                "CNY": {"value": cny_bond_mv, "pct": (cny_bond_mv / total_net_assets * 100) if total_net_assets else 0},
                "USD": {"value": usd_bond_mv, "pct": (usd_bond_mv / total_net_assets * 100) if total_net_assets else 0},
                "Cash": {"value": total_cash, "pct": (total_cash / total_net_assets * 100) if total_net_assets else 0},
            },
            "net": {
                "CNY": {"value": cny_bond_mv + total_cash - usd_fwd_mv, "pct": ((cny_bond_mv + total_cash - usd_fwd_mv) / total_net_assets * 100) if total_net_assets else 0},
                "USD": {"value": net_foreign, "pct": (net_foreign / total_net_assets * 100) if total_net_assets else 0},
            },
            "portfolio_hedge_pct": portfolio_hedge_pct,
            "usd_hedge_pct": usd_hedge_pct,
            "fx_forward_pnl": usd_fwd_pnl,
        }
    else:
        currency_exposure = None

    # ─── 8. Build summary ────────────────────────────────────────────────
    total_unrealized_pnl = sum(h["unrealized_pnl"] for h in holdings)
    total_cost_basis = sum(h["cost_basis"] for h in holdings)

    # FX forward P&L is TWO different things and must not be reported as one.
    #
    #   fund level  — the fund's own currency hedging, on MAIN ACCOUNT. This
    #                 belongs to every holder and is a fund-level number.
    #   share class — hedging run FOR a specific hedged share class. Its P&L
    #                 belongs to that class alone. Attributing it to the fund
    #                 spreads one class's hedging result across holders of
    #                 every other class, including unhedged ones.
    #
    # `summary.fx_forward_pnl` previously carried hedge_ledger["total_pl"], the
    # two ADDED TOGETHER, and was rendered as the fund's "FX Fwd P&L". On GDBF
    # at 2026-07-24 that meant a fund figure of -9,136.57 which was really
    # +16,748.35 of fund hedging netted against -25,884.92 belonging to the 16
    # hedged classes. The split already existed in hedge_ledger; only the
    # summary collapsed it.
    #
    # fx_forward_pnl is therefore now the FUND-LEVEL figure — the one a
    # fund-level summary should carry — with the other two published
    # explicitly beside it so nothing has to re-derive them.
    fx_forward_pnl_fund = hedge_ledger["fund_level_pl"]
    fx_forward_pnl_share_class = hedge_ledger["share_class_pl"]
    fx_forward_pnl = fx_forward_pnl_fund

    summary = {
        "num_holdings": len(holdings),
        "total_market_value": total_securities_mv,
        "cash_balance": total_cash,
        "total_nav": total_net_assets,
        "total_value": total_net_assets,
        "bond_value": total_securities_mv,
        "total_unrealized_pnl": total_unrealized_pnl,
        "unrealized_pnl": total_unrealized_pnl,
        "total_price_pnl": total_unrealized_pnl,
        "total_cost_basis": total_cost_basis,
        "total_accrued_income": total_accrued_income,

        # ── AUM decomposition, straight off the Balance Sheet ──────────────
        # The components a NAV reconciliation actually needs, each from its own
        # reported line rather than inferred from a residual, so an admin figure
        # can be compared like-for-like against Maia or Athena.
        #   clean bond MV + accrued + cash + FX forward P&L + other = NAV
        # Verified on GDBF 2026-07-24:
        #   10,165,176.38 + 186,969.64 + 67,623.28 + (-9,136.57) + 91,821.42
        #     = 10,502,454.15 = Total Net Assets
        "balance_sheet_detail": dict(bs_detail),
        "aum_breakdown": {
            "clean_bond_mv": total_securities_mv,
            "accrued_income": total_accrued_income,
            # Cash as the admin reports it, NOT the residual above.
            "cash": bs_cash_total,
            "fx_forward_pnl": hedge_ledger["total_pl"],
            "fx_forward_pnl_fund": hedge_ledger["fund_level_pl"],
            "fx_forward_pnl_share_class": hedge_ledger["share_class_pl"],
            # Whatever the Balance Sheet holds that none of the above captures
            # (net receivables/payables). Derived last so the parts always sum
            # to NAV — an unexplained gap shows up here as a number instead of
            # silently breaking the reconciliation.
            "other_net": round(
                total_net_assets - total_securities_mv - total_accrued_income
                - bs_cash_total - hedge_ledger["total_pl"], 2),
            "total_nav": total_net_assets,
            # The residual currently shown as "cash" on dashboards, kept so the
            # difference from real cash is visible rather than mysterious.
            "cash_residual": cash_residual,
        },
        # Fund-level only. See the comment above the assignment.
        "fx_forward_pnl": fx_forward_pnl,
        "fx_forward_pnl_fund": fx_forward_pnl_fund,
        # Belongs to the hedged share classes, NOT to the fund. Excluded from
        # the fund valuation; per-class detail is in hedge_ledger.by_share_class.
        "fx_forward_pnl_share_class": fx_forward_pnl_share_class,
        # Both added together — the admin's own combined figure. Kept so the
        # recon can tie back to the report, never for fund-level display.
        "fx_forward_pnl_total": hedge_ledger["total_pl"],
        "cash_pct": (total_cash / total_net_assets * 100) if total_net_assets else 0,
        "num_countries": len(countries),
        "num_issuers": len(set(h["ticker"] for h in holdings)),
        "max_country": countries[0]["country"] if countries else "--",
        "max_country_pct": countries[0]["country_weight"] if countries else 0,
        "base_currency": base_currency,
        "valuation_date": valuation_date,
        # No analytics from static data
        "weighted_duration": None,
        "weighted_spread": None,
        "weighted_ytw": None,
        "weighted_return": None,
        "avg_rating": "--",
    }

    # Cross-check the administrator's stamped valuation time against what
    # fund_config.py declares for this fund. A mismatch means our declaration is
    # wrong (or the fund changed its dealing point) and every downstream label,
    # export stamp and admin recon is comparing unlike with unlike — so log it
    # loudly rather than letting the hardcoded value quietly win.
    valuation_time = (valuation_time_raw or "")[:5] or None   # "23:00:00" -> "23:00"
    valuation_time_matches = None
    if valuation_time and _pid:
        try:
            from fund_config import valuation_point
            _declared = valuation_point(_pid)["local_time"]
            valuation_time_matches = (_declared == valuation_time)
            if not valuation_time_matches:
                logger.warning(
                    "VALUATION POINT MISMATCH for %s: admin report says %s, "
                    "fund_config.py declares %s — fix fund_config.py",
                    _pid, valuation_time, _declared,
                )
        except Exception as _e:      # never let a cross-check break an ingest
            logger.warning("valuation-point cross-check skipped: %s", _e)

    return {
        "valuation_date": valuation_date,
        # As stamped by the administrator, for cross-checking the declaration.
        "valuation_time": valuation_time,
        "valuation_time_matches_declared": valuation_time_matches,
        "base_currency": base_currency,
        "portfolio_id": _pid or "gcrif",
        "fund_name": _fund_display or fund_name_raw or "Guinness China RMB Income Fund",
        "holdings": holdings,
        "summary": summary,
        "issuer_exposure": compute_issuer_exposure(holdings, total_net_assets),
        "countries": countries,
        "fx_rates": fx_rates,
        "fx_forwards": fx_forwards,
        "hedge_ledger": hedge_ledger,
        "currency_exposure": currency_exposure,
        "share_classes": share_classes,
        "parsed_at": datetime.utcnow().isoformat() + "Z",
    }


if __name__ == "__main__":
    import json
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "GUINNESS_CHINA_RMB_INCOME_FUND_-_NAV_REPORTS_-_26-Jan-2026.xls"
    result = parse_nav_report(path)
    print(json.dumps(result, indent=2, default=str))
    print(f"\n✓ Parsed {result['summary']['num_holdings']} holdings, valuation date: {result['valuation_date']}")
