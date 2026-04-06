-- ============================================================================
-- Recon MCP — Supabase table definitions
-- Target: Athena Supabase project (iociqthaxysqqqamonqa)
-- Run in Supabase SQL Editor in order.
-- ============================================================================

-- ── recon_bbg: Bloomberg portfolio export data ─────────────────────────────
-- Source: BBG Excel upload, parsed by recon-mcp bbg_parser.py
-- Upsert key: (portfolio_id, date, isin)

CREATE TABLE IF NOT EXISTS recon_bbg (
    portfolio_id  TEXT        NOT NULL,
    date          DATE        NOT NULL,
    isin          TEXT        NOT NULL,
    description   TEXT,
    currency      TEXT        DEFAULT 'USD',
    coupon        NUMERIC,
    maturity_date DATE,
    par           NUMERIC,            -- nominal / position size
    price         NUMERIC,            -- clean price (per 100)
    accrued       NUMERIC,            -- accrued interest (absolute $)
    yield_to_worst NUMERIC,
    duration      NUMERIC,
    mv            NUMERIC,            -- market value (absolute $)
    uploaded_by   TEXT,
    created_at    TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, date, isin)
);

-- ── recon_admin: Guinness admin NAV report data ────────────────────────────
-- Source: Admin NAV XLS upload, parsed by recon-mcp nav_parser.py
-- Upsert key: (portfolio_id, date, isin)

CREATE TABLE IF NOT EXISTS recon_admin (
    portfolio_id  TEXT        NOT NULL,
    date          DATE        NOT NULL,
    isin          TEXT        NOT NULL,
    description   TEXT,
    currency      TEXT        DEFAULT 'USD',
    coupon        NUMERIC,
    maturity_date DATE,
    country       TEXT,
    par           NUMERIC,
    price         NUMERIC,
    accrued       NUMERIC,
    mv            NUMERIC,
    uploaded_by   TEXT,
    created_at    TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, date, isin)
);

-- ── recon_maia: Maia holdings export data ──────────────────────────────────
-- Source: Maia TSV/Excel upload, parsed by recon-mcp
-- Upsert key: (portfolio_id, date, isin)

CREATE TABLE IF NOT EXISTS recon_maia (
    portfolio_id  TEXT        NOT NULL,
    date          DATE        NOT NULL,
    isin          TEXT        NOT NULL,
    description   TEXT,
    currency      TEXT        DEFAULT 'USD',
    coupon        NUMERIC,
    maturity_date DATE,
    par           NUMERIC,
    price         NUMERIC,
    mv            NUMERIC,
    uploaded_by   TEXT,
    created_at    TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, date, isin)
);

-- ── recon_calcs: GA10 QuantLib calculations (from BBG prices) ──────────────
-- Source: ga10-pricing service, triggered after BBG upload
-- Per-100 face values, stored for reference
-- Upsert key: (portfolio_id, date, isin)

CREATE TABLE IF NOT EXISTS recon_calcs (
    portfolio_id      TEXT        NOT NULL,
    date              DATE        NOT NULL,
    isin              TEXT        NOT NULL,
    source_price      NUMERIC,           -- the BBG price used for calculation
    ga10_accrued      NUMERIC,           -- accrued at T+0 (per 100)
    ga10_accrued_c1   NUMERIC,           -- accrued at C+1 (calendar +1)
    ga10_accrued_t1   NUMERIC,           -- accrued at T+1 (business day +1)
    ga10_accrued_t2   NUMERIC,           -- accrued at T+2
    ga10_accrued_t3   NUMERIC,           -- accrued at T+3
    ga10_yield        NUMERIC,           -- yield to maturity
    ga10_yield_c1     NUMERIC,           -- yield at C+1
    ga10_yield_t1     NUMERIC,           -- yield at T+1
    ga10_yield_worst  NUMERIC,           -- yield to worst (callable bonds)
    ga10_duration     NUMERIC,           -- modified duration
    ga10_duration_worst NUMERIC,         -- duration to worst
    ga10_spread       NUMERIC,           -- Z-spread
    ga10_convexity    NUMERIC,
    ga10_dv01         NUMERIC,           -- dollar value of 1bp
    created_at        TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, date, isin)
);

-- ── athena_bbg: Par-scaled absolute accrued interest from BBG prices ───────
-- Source: recon-mcp, computed from recon_calcs per-100 × par
-- This is the display-ready accrued at multiple settlement dates
-- Upsert key: (portfolio_id, date, isin)

CREATE TABLE IF NOT EXISTS athena_bbg (
    portfolio_id  TEXT        NOT NULL,
    date          DATE        NOT NULL,
    isin          TEXT        NOT NULL,
    par           NUMERIC,              -- nominal used for scaling
    source_price  NUMERIC,              -- BBG price used
    accrued_t0    NUMERIC,              -- absolute accrued at T+0
    accrued_c1    NUMERIC,              -- absolute accrued at C+1
    accrued_t1    NUMERIC,              -- absolute accrued at T+1
    accrued_c2    NUMERIC,              -- absolute accrued at C+2
    accrued_c3    NUMERIC,              -- absolute accrued at C+3
    created_at    TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, date, isin)
);

-- ── recon_uploads: Upload metadata + raw file tracking ─────────────────────
-- Source: recon-mcp on each file upload
-- Upsert key: (portfolio_id, source, date)

CREATE TABLE IF NOT EXISTS recon_uploads (
    portfolio_id  TEXT        NOT NULL,
    source        TEXT        NOT NULL,   -- 'bbg', 'admin', 'maia'
    date          DATE        NOT NULL,
    file_path     TEXT,                   -- Supabase Storage path
    file_name     TEXT,
    file_size     INTEGER,
    file_hash     TEXT,                   -- SHA256 for dedup
    uploaded_by   TEXT,
    bonds_parsed  INTEGER     DEFAULT 0,
    parse_status  TEXT        DEFAULT 'ok',
    parse_error   TEXT,
    uploaded_at   TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (portfolio_id, source, date)
);

-- ── Indexes for common query patterns ──────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_recon_bbg_date ON recon_bbg (date);
CREATE INDEX IF NOT EXISTS idx_recon_admin_date ON recon_admin (date);
CREATE INDEX IF NOT EXISTS idx_recon_maia_date ON recon_maia (date);
CREATE INDEX IF NOT EXISTS idx_recon_calcs_date ON recon_calcs (date);
CREATE INDEX IF NOT EXISTS idx_athena_bbg_date ON athena_bbg (date);

-- ── Disable RLS (recon tables accessed via anon key, no user-level auth) ───

ALTER TABLE recon_bbg ENABLE ROW LEVEL SECURITY;
ALTER TABLE recon_admin ENABLE ROW LEVEL SECURITY;
ALTER TABLE recon_maia ENABLE ROW LEVEL SECURITY;
ALTER TABLE recon_calcs ENABLE ROW LEVEL SECURITY;
ALTER TABLE athena_bbg ENABLE ROW LEVEL SECURITY;
ALTER TABLE recon_uploads ENABLE ROW LEVEL SECURITY;

-- Allow all operations for anon role (RLS effectively disabled)
CREATE POLICY IF NOT EXISTS "anon_all" ON recon_bbg FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY IF NOT EXISTS "anon_all" ON recon_admin FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY IF NOT EXISTS "anon_all" ON recon_maia FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY IF NOT EXISTS "anon_all" ON recon_calcs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY IF NOT EXISTS "anon_all" ON athena_bbg FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY IF NOT EXISTS "anon_all" ON recon_uploads FOR ALL USING (true) WITH CHECK (true);
