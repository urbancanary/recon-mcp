-- ============================================================================
-- Local bond-data cache tables in Athena Supabase
-- Target: Athena Supabase project (iociqthaxysqqqamonqa)
--
-- These tables are synced from bond-data Supabase via REST API by recon-mcp.
-- Sync happens: on startup, after each upload, and via POST /sync/bond-data.
--
-- NOTE: FDW was tested but too slow (full table scans on 33K rows).
-- REST API with ISIN filter is fast and reliable.
-- ============================================================================

-- Local cached copy of bond_identity (branded/corrected descriptions)
CREATE TABLE IF NOT EXISTS local_bond_identity (
    isin                TEXT PRIMARY KEY,
    branded_description TEXT,
    branded_ticker      TEXT,
    issuer_description  TEXT,
    coupon              NUMERIC,
    maturity_date       DATE,
    synced_at           TIMESTAMPTZ DEFAULT now()
);

-- Local cached copy of bond_reference (static data)
CREATE TABLE IF NOT EXISTS local_bond_reference (
    isin                TEXT PRIMARY KEY,
    ticker_description  TEXT,
    currency            TEXT,
    standard_country    TEXT,
    sector              TEXT,
    applied_rating      TEXT,
    coupon              REAL,
    maturity_date       DATE,
    day_count           TEXT,
    synced_at           TIMESTAMPTZ DEFAULT now()
);

-- Local cached copy of bond_analytics (latest prices)
CREATE TABLE IF NOT EXISTS local_bond_analytics (
    isin                TEXT PRIMARY KEY,
    price               NUMERIC,
    price_date          DATE,
    price_source        TEXT,
    ytw                 NUMERIC,
    oad                 NUMERIC,
    oas                 NUMERIC,
    spread              NUMERIC,
    duration            NUMERIC,
    accrued_interest    NUMERIC,
    synced_at           TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT ON local_bond_identity TO anon, authenticated;
GRANT SELECT ON local_bond_reference TO anon, authenticated;
GRANT SELECT ON local_bond_analytics TO anon, authenticated;
