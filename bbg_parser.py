"""
Bloomberg Portfolio Export Parser
Detects and parses BBG portfolio XLS/XLSX exports.
Extracts ISIN + accrued interest for reconciliation.
"""

import logging
import pandas as pd
from io import BytesIO

logger = logging.getLogger(__name__)


def is_bbg_export(xls_bytes: bytes) -> bool:
    """Detect if a file is a Bloomberg portfolio export (not an admin NAV report).
    BBG exports typically have columns like 'ISIN', 'Accrued Interest' in a flat table.
    Admin NAV has specific sheet names like 'LFAI_Accrued_Income_Recon'."""
    try:
        xls = pd.ExcelFile(BytesIO(xls_bytes))
        sheet_names = [s.upper() for s in xls.sheet_names]

        # Admin NAV has these distinctive sheets
        if any('LFAI' in s for s in sheet_names):
            return False

        # Try reading first sheet and check for BBG-like columns
        df = pd.read_excel(BytesIO(xls_bytes), sheet_name=0, header=None, nrows=20)
        flat = ' '.join(str(v).upper() for row in df.values for v in row if pd.notna(v))

        # BBG exports typically contain these terms
        bbg_signals = ['ISIN', 'ACCRUED', 'SECURITY', 'POSITION', 'MARKET VALUE']
        matches = sum(1 for s in bbg_signals if s in flat)
        return matches >= 2

    except Exception as e:
        logger.warning("BBG detection failed: %s", e)
        return False


def parse_bbg_export(xls_bytes: bytes) -> dict:
    """Parse Bloomberg portfolio export and extract ISIN + accrued interest.

    BBG exports have metadata in rows 1-3, then the actual header row (row 4)
    with columns like 'ISIN', 'Acc Int', 'Long Name', etc.

    Returns:
        {
            "type": "bbg",
            "bonds": {isin: accrued_total, ...},
            "count": int,
            "as_of_date": str or None,
            "settle_date": str or None,
        }
    """
    try:
        # First try to extract metadata from rows 1-3
        meta_df = pd.read_excel(BytesIO(xls_bytes), sheet_name=0, header=None, nrows=5)
        as_of_date = None
        base_currency = None
        for _, row in meta_df.iterrows():
            for v in row:
                s = str(v).strip()
                if '/' in s and len(s) <= 10 and not as_of_date:
                    as_of_date = s
                # Currency field in header (e.g. "CNH", "USD")
                if s.upper() in ('CNH', 'CNY', 'USD', 'EUR', 'GBP', 'HKD') and not base_currency:
                    base_currency = s.upper()
            if as_of_date and base_currency:
                break

        # Find the header row — look for a row containing 'ISIN'
        header_row = None
        for idx, row in meta_df.iterrows():
            vals = [str(v).upper().strip() for v in row if pd.notna(v)]
            if any('ISIN' in v for v in vals):
                header_row = idx
                break

        if header_row is None:
            # Try reading more rows
            meta_df = pd.read_excel(BytesIO(xls_bytes), sheet_name=0, header=None, nrows=20)
            for idx, row in meta_df.iterrows():
                vals = [str(v).upper().strip() for v in row if pd.notna(v)]
                if any('ISIN' in v for v in vals):
                    header_row = idx
                    break

        if header_row is None:
            raise ValueError(f"No header row with ISIN found in first 20 rows")

        # Re-read with correct header row
        df = pd.read_excel(BytesIO(xls_bytes), sheet_name=0, header=header_row)

        # Normalise column names — BBG uses short names like 'Acc Int', 'Px Close'
        col_map = {}
        for col in df.columns:
            upper = str(col).upper().strip()
            if 'ISIN' in upper and 'isin' not in col_map:
                col_map['isin'] = col
            elif ('ACC' in upper and 'INT' in upper) and 'accrued' not in col_map:
                col_map['accrued'] = col
            elif 'ACCRUED' in upper and 'accrued' not in col_map:
                col_map['accrued'] = col
            elif 'SETTLE' in upper and 'settle' not in col_map:
                col_map['settle'] = col
            elif 'FX' in upper and ('CLS' in upper or 'CLOSE' in upper) and 'fx' not in col_map:
                col_map['fx'] = col
            # Price — BBG uses 'Px Close', 'Px Mid', 'Price', 'Mid Price', 'Clean Price', etc.
            elif 'price' not in col_map and 'FX' not in upper and (
                ('PX' in upper and ('CLS' in upper or 'CLOSE' in upper or 'MID' in upper or 'LAST' in upper))
                or upper in ('PRICE', 'MID PRICE', 'CLEAN PRICE', 'PX_LAST', 'PX_MID', 'PX CLOSE')
                or ('PRICE' in upper and ('CLEAN' in upper or 'MID' in upper or 'CLOSE' in upper))
            ):
                col_map['price'] = col
            # Yield to Maturity — prefer YTM over YTW for apples-to-apples comparison
            elif 'ytm' not in col_map and (
                upper in ('YTM', 'YTM MID', 'YLD', 'YLD TO MTY', 'YLD_TO_MTY', 'YIELD TO MATURITY', 'YTM_MID',
                          'YIELD TO MAT', 'YLD TO MAT')
                or ('YTM' in upper and 'ACCRUED' not in upper)
                or ('YLD' in upper and 'MTY' in upper)
                or ('YIELD' in upper and 'MAT' in upper and 'WORST' not in upper)
            ):
                col_map['ytm'] = col
            # Yield to Worst — fallback if no YTM column
            elif 'ytw' not in col_map and (
                upper in ('YTW', 'YTW MID', 'YIELD TO WORST', 'YLD TO WORST', 'YLD_TO_WORST', 'YLD TO WST')
                or ('YTW' in upper and 'ACCRUED' not in upper)
                or ('YIELD' in upper and 'WORST' in upper)
            ):
                col_map['ytw'] = col
            # Modified Duration (preferred for recon — matches our QuantLib calculation)
            elif 'mod_dur' not in col_map and (
                upper in ('MOD DUR', 'MODIFIED DURATION', 'DUR MOD', 'MOD_DUR',
                          'LOCAL MOD DUR', 'LOCAL MODIFIED DURATION')
                or ('MOD' in upper and 'DUR' in upper and 'OPT' not in upper)
            ):
                col_map['mod_dur'] = col
            # OAD — Option-Adjusted Duration (fallback if no mod dur)
            elif 'oad' not in col_map and (
                upper in ('OAD', 'OAS DUR', 'OPT ADJ DUR', 'OPTION ADJUSTED DURATION',
                          'DUR ADJ OPT', 'OA DURATION', 'OAD MID')
                or ('OPT' in upper and 'DUR' in upper)
                or ('OA' in upper and 'DUR' in upper and 'ACCRUED' not in upper)
            ):
                col_map['oad'] = col
            # Market Value — for value reconciliation
            elif 'mv' not in col_map and (
                upper in ('MARKET VALUE', 'MKT VAL', 'MV', 'MARKET VAL', 'MKT VALUE',
                          'LOCAL MV', 'LOCAL MARKET VALUE', 'MKT VAL LOCAL')
                or ('MARKET' in upper and 'VALUE' in upper and 'FX' not in upper)
                or ('MKT' in upper and 'VAL' in upper and 'FX' not in upper)
            ):
                col_map['mv'] = col
            # Position / Quantity / Par
            elif 'position' not in col_map and (
                upper in ('POSITION', 'QUANTITY', 'QTY', 'PAR', 'FACE', 'NOMINAL',
                          'PAR AMOUNT', 'FACE AMOUNT', 'NOTIONAL')
            ):
                col_map['position'] = col

        if 'isin' not in col_map:
            raise ValueError(f"No ISIN column found. Columns: {list(df.columns)}")
        if 'accrued' not in col_map:
            raise ValueError(f"No Accrued/Acc Int column found. Columns: {list(df.columns)}")

        isin_col = col_map['isin']
        accrued_col = col_map['accrued']

        fx_col = col_map.get('fx')
        ytm_col = col_map.get('ytm')
        ytw_col = col_map.get('ytw')
        yield_col = ytm_col or ytw_col  # Prefer YTM for primary yield_bonds
        price_col = col_map.get('price')
        oad_col = col_map.get('mod_dur') or col_map.get('oad')  # Prefer mod dur over OAD
        mv_col = col_map.get('mv')
        position_col = col_map.get('position')
        bonds = {}
        yield_bonds = {}    # Primary yield (YTM preferred, YTW fallback)
        ytm_bonds = {}      # YTM specifically
        ytw_bonds = {}      # YTW specifically
        price_bonds = {}
        oad_bonds = {}
        mv_bonds = {}       # Market value per bond
        position_bonds = {} # Position/par per bond
        raw_values = []
        for _, row in df.iterrows():
            isin = str(row[isin_col]).strip()
            if not isin or len(isin) < 12 or not isin[:2].isalpha():
                continue
            try:
                accrued = float(row[accrued_col])
            except (ValueError, TypeError):
                continue

            # If FX rate available and != 1, convert from base (CNH) to local currency
            # FX Cls is the closing FX rate (local per 1 base unit)
            fx_rate = 1.0
            if fx_col is not None:
                try:
                    fx_val = float(row[fx_col])
                    if fx_val > 0 and fx_val != 1.0:
                        accrued = accrued / fx_val
                        fx_rate = fx_val
                except (ValueError, TypeError):
                    pass

            bonds[isin] = accrued
            raw_values.append(abs(accrued))

            # Extract yields — both YTM and YTW if available
            if yield_col is not None:
                try:
                    y = float(row[yield_col])
                    if not (isinstance(y, float) and (y != y)):
                        yield_bonds[isin] = y
                except (ValueError, TypeError):
                    pass
            if ytm_col is not None:
                try:
                    y = float(row[ytm_col])
                    if not (isinstance(y, float) and (y != y)):
                        ytm_bonds[isin] = y
                except (ValueError, TypeError):
                    pass
            if ytw_col is not None:
                try:
                    y = float(row[ytw_col])
                    if not (isinstance(y, float) and (y != y)):
                        ytw_bonds[isin] = y
                except (ValueError, TypeError):
                    pass

            # Extract price if column present
            if price_col is not None:
                try:
                    px = float(row[price_col])
                    if not (isinstance(px, float) and (px != px)):  # not NaN
                        price_bonds[isin] = px
                except (ValueError, TypeError):
                    pass

            # Extract OAD (Option-Adjusted Duration) if column present
            if oad_col is not None:
                try:
                    oad = float(row[oad_col])
                    if not (isinstance(oad, float) and (oad != oad)):  # not NaN
                        oad_bonds[isin] = oad
                except (ValueError, TypeError):
                    pass

            # Extract Market Value if column present
            if mv_col is not None:
                try:
                    mv = float(row[mv_col])
                    if not (isinstance(mv, float) and (mv != mv)):
                        mv_bonds[isin] = mv
                except (ValueError, TypeError):
                    pass

            # Extract Position/Par if column present
            if position_col is not None:
                try:
                    pos = float(row[position_col])
                    if not (isinstance(pos, float) and (pos != pos)):
                        position_bonds[isin] = pos
                except (ValueError, TypeError):
                    pass

        # BBG PORT exports store values in thousands — detect and correct
        if raw_values:
            median_val = sorted(raw_values)[len(raw_values) // 2]
            if median_val > 100000:
                logger.info("BBG values appear to be in thousands (median=%.0f), dividing by 1000", median_val)
                bonds = {k: v / 1000 for k, v in bonds.items()}

        # Get settle date from data if available
        settle_date = None
        if 'settle' in col_map:
            for _, row in df.iterrows():
                try:
                    settle_date = str(row[col_map['settle']]).strip()
                    if settle_date and settle_date != 'nan':
                        break
                except Exception:
                    pass

        # Try to find portfolio total MV from summary/total rows after bond data
        # BBG PORT exports often have a row with "Total" in the ISIN or description column
        total_mv = None
        total_mv_with_cash = None
        if mv_col is not None:
            for _, row in df.iterrows():
                isin_val = str(row[isin_col]).strip() if pd.notna(row[isin_col]) else ""
                # Look for total rows — they won't have a valid ISIN
                if isin_val and len(isin_val) >= 12 and isin_val[:2].isalpha():
                    continue  # skip bond rows
                # Check if any cell in this row contains "Total" or similar
                row_text = ' '.join(str(v).upper() for v in row if pd.notna(v))
                if 'TOTAL' in row_text or 'PORTFOLIO' in row_text or 'NET' in row_text:
                    try:
                        mv = float(row[mv_col])
                        if not (isinstance(mv, float) and (mv != mv)) and mv > 0:
                            # Largest total is likely the portfolio total (inc cash)
                            if total_mv_with_cash is None or mv > total_mv_with_cash:
                                total_mv_with_cash = mv
                            # Smaller total might be securities-only
                            if total_mv is None:
                                total_mv = mv
                            elif mv < total_mv:
                                total_mv = mv
                    except (ValueError, TypeError):
                        pass

        # Sum MV from individual bonds as a fallback
        bbg_securities_mv = sum(mv_bonds.values()) if mv_bonds else None

        yield_source = 'YTM' if col_map.get('ytm') else ('YTW' if col_map.get('ytw') else 'none')
        logger.info("BBG export parsed: %d bonds, as_of=%s, settle=%s, base_ccy=%s, yield_col=%s (%d bonds), price_bonds=%d, oad_bonds=%d, mv_bonds=%d",
                    len(bonds), as_of_date, settle_date, base_currency, yield_source, len(yield_bonds), len(price_bonds), len(oad_bonds), len(mv_bonds))

        return {
            "type": "bbg",
            "bonds": bonds,
            "yield_bonds": yield_bonds,
            "ytm_bonds": ytm_bonds,
            "ytw_bonds": ytw_bonds,
            "price_bonds": price_bonds,
            "oad_bonds": oad_bonds,
            "mv_bonds": mv_bonds,
            "position_bonds": position_bonds,
            "bbg_securities_mv": bbg_securities_mv,
            "bbg_total_mv": total_mv_with_cash,
            "count": len(bonds),
            "as_of_date": as_of_date,
            "settle_date": settle_date,
            "base_currency": base_currency,
        }

    except Exception as e:
        logger.error("BBG parse failed: %s", e)
        raise
