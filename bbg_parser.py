"""
Bloomberg Portfolio Export Parser
Detects and parses BBG portfolio XLS/XLSX exports.
Extracts ISIN + accrued interest for reconciliation.
"""

import logging
import re
from datetime import datetime
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
                          'PAR AMOUNT', 'FACE AMOUNT', 'NOTIONAL', 'POS')
            ):
                col_map['position'] = col
            # Issue Date
            elif 'issue_date' not in col_map and (
                upper in ('ISS DT', 'ISSUE DATE', 'ISSUE DT', 'ISSUANCE DATE', 'ISS DATE',
                          'DATED DATE', 'ISSUE_DATE', 'ISS_DT')
                or (upper.startswith('ISS') and 'DT' in upper and len(upper) <= 10)
            ):
                col_map['issue_date'] = col
            # Long Name — contains embedded maturity date, e.g. "CGB 3.38 07/04/26"
            elif 'long_name' not in col_map and upper in ('LONG NAME', 'LONGNAME', 'SECURITY NAME',
                                                           'SECURITY', 'SEC NAME', 'BOND NAME'):
                col_map['long_name'] = col
            # Coupon rate (explicit)
            elif 'cpn_rate' not in col_map and upper in ('CPN', 'COUPON', 'COUPON RATE', 'CPN RATE'):
                col_map['cpn_rate'] = col
            # Coupon frequency (1=Annual, 2=Semi)
            elif 'cpn_freq' not in col_map and (
                upper in ('CPN FREQ', 'COUPON FREQ', 'CPN FREQUENCY', 'COUPON FREQUENCY', 'FREQ')
                or upper.replace(' ', '').replace('_', '') in ('CPNFREQ', 'COUPONFREQ')
            ):
                col_map['cpn_freq'] = col
            # Day Count convention
            elif 'day_count' not in col_map and (
                upper in ('DAY COUNT', 'DAY COUNT CONV', 'DAY_COUNT', 'DCB')
                or ('DAY' in upper and 'COUNT' in upper)
            ):
                col_map['day_count'] = col
            # Effective / Exact Maturity Date (not month-end rounded like CBonds)
            elif 'eff_maturity' not in col_map and (
                upper in ('EFF MATURITY DATE', 'EFF MATURITY', 'EFFECTIVE MATURITY DATE',
                          'EFFECTIVE MATURITY', 'MTY', 'MATURITY')
                or (('EFF' in upper or 'EFFECTIVE' in upper) and 'MATURI' in upper)
            ):
                col_map['eff_maturity'] = col
            # First Coupon Date
            elif 'first_coupon' not in col_map and (
                upper in ('FIRST CPN DT', 'FIRST COUPON DATE', 'FIRST COUPON', 'FIRST CPN DATE',
                          '1ST CPN DT', 'IST CPN DT')
                or ('FIRST' in upper and ('CPN' in upper or 'COUPON' in upper))
            ):
                col_map['first_coupon'] = col
            # Accrued Int (%) — per-100 accrued in local currency (most reliable cross-currency value)
            elif 'accrued_pct' not in col_map and '%' in upper and 'ACCRUED' in upper:
                col_map['accrued_pct'] = col
            # Moody's rating
            elif 'moodys' not in col_map and upper.replace("'", '').replace('\u2019', '') in (
                'MOODYS', 'MOODYS RATING', 'MDY', 'MOODYS LONG TERM'
            ):
                col_map['moodys'] = col
            # S&P rating
            elif 'sp' not in col_map and upper in ('S&P', 'SP', 'S&P RATING', 'S&P LONG TERM', 'S&P LT'):
                col_map['sp'] = col
            # Fitch rating
            elif 'fitch' not in col_map and upper in ('FITCH', 'FITCH RATING', 'FITCH LONG TERM'):
                col_map['fitch'] = col
            # Bloomberg Composite rating
            elif 'bb_comp' not in col_map and upper in ('BB COMP', 'BBG COMP', 'BLOOMBERG COMPOSITE',
                                                         'COMPOSITE RATING', 'BB COMP RATING'):
                col_map['bb_comp'] = col

        logger.info("BBG columns detected: %s → col_map: %s", [str(c) for c in df.columns], col_map)

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
        issue_date_col = col_map.get('issue_date')
        long_name_col = col_map.get('long_name')
        bonds = {}
        yield_bonds = {}    # Primary yield (YTM preferred, YTW fallback)
        ytm_bonds = {}      # YTM specifically
        ytw_bonds = {}      # YTW specifically
        price_bonds = {}
        oad_bonds = {}
        mv_bonds = {}         # Market value per bond
        position_bonds = {}   # Position/par per bond
        issue_date_bonds = {}   # Issue date per bond
        maturity_date_bonds = {}  # Maturity date parsed from Long Name (more accurate than CBonds)
        coupon_bonds = {}         # Coupon parsed from Long Name, e.g. "CGB 3.38 07/04/26" → 3.38
        cpn_rate_bonds = {}       # Explicit coupon rate from Cpn column
        cpn_freq_bonds = {}       # Coupon frequency (1=Annual, 2=Semi)
        day_count_bonds = {}      # Day count convention
        eff_maturity_bonds = {}   # Exact maturity date from BBG
        first_coupon_bonds = {}   # First coupon date
        accrued_pct_bonds = {}    # Per-100 accrued in local currency
        moodys_bonds = {}         # Moody's rating
        sp_bonds = {}             # S&P rating
        fitch_bonds = {}          # Fitch rating
        bb_comp_bonds = {}        # Bloomberg composite rating
        bonds_from_pct = {}       # Absolute accrued computed from pos × accrued_pct/100 (pre-scale)
        raw_values = []

        # Regex to extract MM/DD/YY maturity from BBG Long Name, e.g. "CGB 3.38 07/04/26"
        _mat_re = re.compile(r'(\d{2}/\d{2}/\d{2})\s*$')
        # Regex to extract coupon from Long Name, e.g. "CGB 3.38 07/04/26" → 3.38
        _coupon_re = re.compile(r'\s(\d+\.?\d+)\s+\d{2}/\d{2}/\d{2}\s*$')
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

            # Extract Issue Date if column present
            if issue_date_col is not None:
                try:
                    iss_dt = row[issue_date_col]
                    if pd.notna(iss_dt):
                        if hasattr(iss_dt, 'strftime'):
                            issue_date_bonds[isin] = iss_dt.strftime('%Y-%m-%d')
                        else:
                            s = str(iss_dt).strip()
                            if s and s != 'nan':
                                issue_date_bonds[isin] = s
                except (ValueError, TypeError):
                    pass

            # Parse maturity date and coupon from Long Name: "CGB 3.38 07/04/26"
            if long_name_col is not None:
                try:
                    ln = str(row[long_name_col]).strip() if pd.notna(row[long_name_col]) else ''
                    m = _mat_re.search(ln)
                    if m:
                        mat_str = m.group(1)  # MM/DD/YY
                        mat_dt = datetime.strptime(mat_str, '%m/%d/%y')
                        maturity_date_bonds[isin] = mat_dt.strftime('%Y-%m-%d')
                    c = _coupon_re.search(ln)
                    if c:
                        coupon_bonds[isin] = float(c.group(1))
                except (ValueError, TypeError):
                    pass

            # Extract new fields
            if col_map.get('cpn_rate') is not None:
                try:
                    cr = float(row[col_map['cpn_rate']])
                    if not (cr != cr):  # not NaN
                        cpn_rate_bonds[isin] = cr
                except (ValueError, TypeError):
                    pass

            if col_map.get('cpn_freq') is not None:
                try:
                    cf = row[col_map['cpn_freq']]
                    if pd.notna(cf):
                        cpn_freq_bonds[isin] = str(int(float(cf))) if str(cf).replace('.', '').isdigit() else str(cf).strip()
                except (ValueError, TypeError):
                    pass

            if col_map.get('day_count') is not None:
                try:
                    dc = str(row[col_map['day_count']]).strip()
                    if dc and dc.lower() != 'nan':
                        day_count_bonds[isin] = dc
                except (ValueError, TypeError):
                    pass

            if col_map.get('eff_maturity') is not None:
                try:
                    em = row[col_map['eff_maturity']]
                    if pd.notna(em):
                        if hasattr(em, 'strftime'):
                            eff_maturity_bonds[isin] = em.strftime('%Y-%m-%d')
                        else:
                            s = str(em).strip()
                            if s and s != 'nan':
                                for dfmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%d/%m/%Y'):
                                    try:
                                        from datetime import datetime as _dt
                                        eff_maturity_bonds[isin] = _dt.strptime(s, dfmt).strftime('%Y-%m-%d')
                                        break
                                    except ValueError:
                                        continue
                except (ValueError, TypeError):
                    pass

            if col_map.get('first_coupon') is not None:
                try:
                    fc = row[col_map['first_coupon']]
                    if pd.notna(fc):
                        if hasattr(fc, 'strftime'):
                            first_coupon_bonds[isin] = fc.strftime('%Y-%m-%d')
                        else:
                            s = str(fc).strip()
                            if s and s != 'nan':
                                for dfmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%d/%m/%Y'):
                                    try:
                                        from datetime import datetime as _dt
                                        first_coupon_bonds[isin] = _dt.strptime(s, dfmt).strftime('%Y-%m-%d')
                                        break
                                    except ValueError:
                                        continue
                except (ValueError, TypeError):
                    pass

            if col_map.get('accrued_pct') is not None:
                try:
                    pct = float(row[col_map['accrued_pct']])
                    if not (pct != pct):
                        accrued_pct_bonds[isin] = pct
                        # Compute absolute accrued from pos × pct/100 BEFORE any scaling.
                        # This gives correct local-currency accrued for all bond currencies.
                        if col_map.get('position') is not None:
                            pos_raw = float(row[col_map['position']])
                            bonds_from_pct[isin] = pos_raw * pct / 100
                except (ValueError, TypeError):
                    pass

            if col_map.get('moodys') is not None:
                try:
                    rv = str(row[col_map['moodys']]).strip()
                    if rv and rv.lower() not in ('nan', 'nr', 'n/r', 'n.r.', 'n/a'):
                        moodys_bonds[isin] = rv
                except (ValueError, TypeError):
                    pass

            if col_map.get('sp') is not None:
                try:
                    rv = str(row[col_map['sp']]).strip()
                    if rv and rv.lower() not in ('nan', 'nr', 'n/r', 'n.r.', 'n/a'):
                        sp_bonds[isin] = rv
                except (ValueError, TypeError):
                    pass

            if col_map.get('fitch') is not None:
                try:
                    rv = str(row[col_map['fitch']]).strip()
                    if rv and rv.lower() not in ('nan', 'nr', 'n/r', 'n.r.', 'n/a'):
                        fitch_bonds[isin] = rv
                except (ValueError, TypeError):
                    pass

            if col_map.get('bb_comp') is not None:
                try:
                    rv = str(row[col_map['bb_comp']]).strip()
                    if rv and rv.lower() not in ('nan', 'nr', 'n/r', 'n.r.', 'n/a'):
                        bb_comp_bonds[isin] = rv
                except (ValueError, TypeError):
                    pass

        # BBG PORT exports store values in thousands — detect and correct.
        # When accrued values are in thousands, so are par (position) and market value.
        if raw_values:
            median_val = sorted(raw_values)[len(raw_values) // 2]
            if median_val > 100000:
                logger.info("BBG values appear to be in thousands (median=%.0f), dividing accrued/par/mv by 1000", median_val)
                bonds = {k: v / 1000 for k, v in bonds.items()}
                position_bonds = {k: v / 1000 for k, v in position_bonds.items()}
                mv_bonds = {k: v / 1000 for k, v in mv_bonds.items()}

        # Independent par scale detection — catches cases where accrued values are small
        # (e.g. CNH portfolios after FX division) but par is still 1000x too large.
        # A per-bond par > 1B is unrealistic for any normal fund position.
        if position_bonds:
            pos_vals = sorted(abs(v) for v in position_bonds.values())
            median_pos = pos_vals[len(pos_vals) // 2]
            if median_pos > 1_000_000_000:
                logger.info("BBG par values oversized (median=%.0f), dividing par/mv by 1000", median_pos)
                position_bonds = {k: v / 1000 for k, v in position_bonds.items()}
                mv_bonds = {k: v / 1000 for k, v in mv_bonds.items()}

        # If we computed absolute accrued from pos × accrued_pct/100 (pre-scale), use that
        # to replace/supplement the Acc Int-derived values. This avoids FX/scale issues
        # for multi-currency portfolios (e.g. CNH base portfolio with USD bonds).
        if bonds_from_pct:
            bonds.update(bonds_from_pct)

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
        logger.info(
            "BBG export parsed: %d bonds, as_of=%s, settle=%s, base_ccy=%s, yield_col=%s (%d bonds), "
            "price_bonds=%d, oad_bonds=%d, mv_bonds=%d, issue_dates=%d, maturity_dates=%d, coupons=%d, "
            "cpn_rate=%d, cpn_freq=%d, day_count=%d, eff_maturity=%d, first_coupon=%d, accrued_pct=%d, "
            "moodys=%d, sp=%d, fitch=%d, bb_comp=%d",
            len(bonds), as_of_date, settle_date, base_currency, yield_source, len(yield_bonds),
            len(price_bonds), len(oad_bonds), len(mv_bonds), len(issue_date_bonds),
            len(maturity_date_bonds), len(coupon_bonds),
            len(cpn_rate_bonds), len(cpn_freq_bonds), len(day_count_bonds), len(eff_maturity_bonds),
            len(first_coupon_bonds), len(accrued_pct_bonds),
            len(moodys_bonds), len(sp_bonds), len(fitch_bonds), len(bb_comp_bonds),
        )

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
            "issue_date_bonds": issue_date_bonds,
            "maturity_date_bonds": maturity_date_bonds,
            "coupon_bonds": coupon_bonds,
            "cpn_rate_bonds": cpn_rate_bonds,
            "cpn_freq_bonds": cpn_freq_bonds,
            "day_count_bonds": day_count_bonds,
            "eff_maturity_bonds": eff_maturity_bonds,
            "first_coupon_bonds": first_coupon_bonds,
            "accrued_pct_bonds": accrued_pct_bonds,
            "moodys_bonds": moodys_bonds,
            "sp_bonds": sp_bonds,
            "fitch_bonds": fitch_bonds,
            "bb_comp_bonds": bb_comp_bonds,
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
