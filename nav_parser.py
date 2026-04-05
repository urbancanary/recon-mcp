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

    # ─── 1. Balance Sheet → valuation date, cash, total NAV ──────────────
    df_bs = pd.read_excel(source, sheet_name="Balance_Sheet", header=None)
    valuation_date = None
    cash_cnh = 0.0
    foreign_currency = 0.0
    total_net_assets = 0.0
    total_accrued_income = 0.0

    for _, row in df_bs.iterrows():
        label = str(row[0]).strip() if pd.notna(row[0]) else ""
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
        elif label == "Accrued Income":
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

    # Also try extracting valuation date from LFAI sheet (more reliable format)
    if not valuation_date:
        try:
            df_lfai_head = pd.read_excel(source, sheet_name="LFAI_Accrued_Income_Recon", header=None, nrows=10)
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

    # ─── 2. Accrued income + coupon per holding ────────────────────────────
    accrued_by_sedol = {}
    coupon_by_sedol = {}
    try:
        df_ai = pd.read_excel(source, sheet_name="LFAI_Accrued_Income_Recon", header=None)

        # Find the column indices by scanning for header keywords
        # The sheet may have prefix columns (grouping/summary) that shift indices
        local_col = 17  # Default: CNH column (fallback)
        for _, row in df_ai.iterrows():
            vals = {i: str(v).strip().upper() for i, v in enumerate(row) if pd.notna(v)}
            for i, v in vals.items():
                if 'GROSS INCOME' in v and 'LOCAL' in v and 'WITHHOLDING' not in v:
                    local_col = i
                    logger.info("LFAI: Found Gross Income (Local) at column %d", i)
                    break
            if local_col != 17:
                break

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
    df_h = pd.read_excel(source, sheet_name="Detailed_Security_Valuation", header=14)
    df_h = df_h[df_h["InvestOne Identifier"].notna()]
    df_h = df_h[~df_h["InvestOne Identifier"].astype(str).str.contains(
        "Total|Investments|Unknown|Spot FX", na=False
    )]
    # Keep only rows that have actual holdings (positive MV)
    df_h = df_h[pd.to_numeric(df_h.get("Market Value - Base", pd.Series()), errors="coerce").notna()]

    total_securities_mv = float(df_h["Market Value - Base"].sum())

    # If total_net_assets wasn't found in balance sheet, estimate it
    if total_net_assets == 0:
        total_net_assets = total_securities_mv + total_cash + total_accrued_income

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
    fx_rates = {"base": "CNH", "rates": {"CNH": 1.0, "CNY": 1.0}, "source": "NAV Report", "as_of": valuation_date}
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

    # ─── 6. FX forwards + forward curve from OpenCurrency ────────────────
    fx_forwards = []
    forward_rates = {}
    try:
        df_fx = pd.read_excel(source, sheet_name="OpenCurrency", header=13)
        df_fx = df_fx[df_fx["Currency"].notna()]
        for _, row in df_fx.iterrows():
            ccy = str(row["Currency"]).strip() if pd.notna(row.get("Currency")) else ""

            # Forward contract details (have a named Broker like "BBH")
            broker = str(row.get("Broker", "")).strip() if pd.notna(row.get("Broker")) else ""
            is_contract = broker and not broker.replace(".", "").replace("-", "").isdigit()
            if ccy in ("USD", "CNH") and is_contract:
                trade_val = float(row.get("Value of Trade Local", 0)) if pd.notna(row.get("Value of Trade Local")) else 0
                contract_rate = float(row.get("Contract Rate", 0)) if pd.notna(row.get("Contract Rate")) else 0
                val_rate = float(row.get("Valuation Rate", 0)) if pd.notna(row.get("Valuation Rate")) else 0
                contract_mv = float(row.get("Contract Value Base", 0)) if pd.notna(row.get("Contract Value Base")) else 0
                mv = float(row.get("Market Value Base", 0)) if pd.notna(row.get("Market Value Base")) else 0
                pnl = float(row.get("Unrealised P/L Base", 0)) if pd.notna(row.get("Unrealised P/L Base")) else 0
                settle = pd.to_datetime(row.get("Settlement Date")).strftime("%Y-%m-%d") if pd.notna(row.get("Settlement Date")) else None
                trade_date = pd.to_datetime(row.get("Trade Date")).strftime("%Y-%m-%d") if pd.notna(row.get("Trade Date")) else None

                fx_forwards.append({
                    "currency": ccy,
                    "broker": broker,
                    "trade_date": trade_date,
                    "settlement_date": settle,
                    "trade_value_local": trade_val,
                    "contract_rate": contract_rate,
                    "valuation_rate": val_rate,
                    "contract_value_base": contract_mv,
                    "market_value_base": mv,
                    "unrealized_pnl": pnl,
                })
            elif not broker and ccy in ("USD", "CNH"):
                # Total rows or non-contract rows
                mv = float(row.get("Market Value Base", 0)) if pd.notna(row.get("Market Value Base")) else 0
                pnl = float(row.get("Unrealised P/L Base", 0)) if pd.notna(row.get("Unrealised P/L Base")) else 0
                if mv != 0 or pnl != 0:
                    fx_forwards.append({
                        "currency": ccy,
                        "market_value_base": mv,
                        "unrealized_pnl": pnl,
                    })

            # Forward rate curve (rows where Broker col = spot rate)
            spot = row.get("Broker")  # Reused column
            if ccy in ("USD",) and pd.notna(spot):
                try:
                    spot_val = float(spot)
                    if 0 < spot_val < 1:
                        forward_rates[ccy] = {
                            "spot": spot_val,
                            "30d": float(row.get("Trans No", 0)) if pd.notna(row.get("Trans No")) else None,
                            "60d": float(row.get("Effective Date", 0)) if pd.notna(row.get("Effective Date")) else None,
                            "90d": float(row.get("Trade Date", 0)) if pd.notna(row.get("Trade Date")) else None,
                            "180d": float(row.get("Settlement Date", 0)) if pd.notna(row.get("Settlement Date")) else None,
                        }
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        logger.warning("Could not parse FX forwards: %s", e)

    if forward_rates:
        fx_rates["forward_rates"] = forward_rates

    # ─── 6b. Calculate currency exposure + hedge ratio ─────────────────
    usd_bond_mv = sum(h["market_value"] for h in holdings if h.get("currency") == "USD")
    cny_bond_mv = sum(h["market_value"] for h in holdings if h.get("currency") in ("CNY", "CNH"))
    usd_fwd_mv = sum(f["market_value_base"] for f in fx_forwards if f.get("currency") == "USD" and f.get("broker"))
    usd_fwd_pnl = sum(f.get("unrealized_pnl", 0) for f in fx_forwards if f.get("currency") == "USD")

    # CNH leg of FX forward (the buy CNH side — positive value)
    cnh_fwd_mv = sum(f["market_value_base"] for f in fx_forwards if f.get("currency") in ("CNH", "CNY") and f.get("broker"))
    # Net foreign exposure (USD bonds minus what's hedged)
    net_foreign = usd_bond_mv + usd_fwd_mv  # small number if well-hedged
    # Portfolio hedge = (CNY bonds + cash + CNH forward leg) / NAV
    portfolio_hedge_pct = ((cny_bond_mv + total_cash + cnh_fwd_mv) / total_net_assets * 100) if total_net_assets else 0
    # Single-leg: what % of USD exposure is covered by forwards
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
        # Portfolio-level: what % of NAV is hedged back to CNH
        "portfolio_hedge_pct": portfolio_hedge_pct,
        # Per-currency: what % of USD bond exposure is covered by forwards
        "usd_hedge_pct": usd_hedge_pct,
        "fx_forward_pnl": usd_fwd_pnl,
    }

    # ─── 7. Share classes from Share_Class_Price_Report ───────────────────
    share_classes = []
    try:
        df_sc = pd.read_excel(source, sheet_name="Share_Class_Price_Report", header=None)
        # Find header row with "ISIN" or "Class"
        header_row = None
        for i, row in df_sc.iterrows():
            for val in row:
                if pd.notna(val) and "ISIN" in str(val).upper():
                    header_row = i
                    break
            if header_row is not None:
                break
        if header_row is not None:
            df_sc.columns = df_sc.iloc[header_row]
            df_sc = df_sc.iloc[header_row + 1:]
            for _, row in df_sc.iterrows():
                isin_val = None
                for col in df_sc.columns:
                    if pd.notna(col) and "ISIN" in str(col).upper() and pd.notna(row.get(col)):
                        isin_val = str(row[col]).strip()
                        break
                if isin_val and len(isin_val) == 12:
                    share_classes.append({"isin": isin_val})
    except Exception as e:
        logger.warning("Could not parse share classes: %s", e)

    # ─── 8. Build summary ────────────────────────────────────────────────
    total_unrealized_pnl = sum(h["unrealized_pnl"] for h in holdings)
    total_cost_basis = sum(h["cost_basis"] for h in holdings)
    fx_forward_pnl = sum(f.get("unrealized_pnl", 0) for f in fx_forwards)

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
        "fx_forward_pnl": fx_forward_pnl,
        "cash_pct": (total_cash / total_net_assets * 100) if total_net_assets else 0,
        "num_countries": len(countries),
        "num_issuers": len(set(h["ticker"] for h in holdings)),
        "max_country": countries[0]["country"] if countries else "--",
        "max_country_pct": countries[0]["country_weight"] if countries else 0,
        "base_currency": "CNH",
        "valuation_date": valuation_date,
        # No analytics from static data
        "weighted_duration": None,
        "weighted_spread": None,
        "weighted_ytw": None,
        "weighted_return": None,
        "avg_rating": "--",
    }

    return {
        "valuation_date": valuation_date,
        "base_currency": "CNH",
        "portfolio_id": "gcrif",
        "fund_name": "Guinness China RMB Income Fund",
        "holdings": holdings,
        "summary": summary,
        "countries": countries,
        "fx_rates": fx_rates,
        "fx_forwards": fx_forwards,
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
