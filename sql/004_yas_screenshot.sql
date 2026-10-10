-- ============================================================================
-- YAS screenshot storage for duration matching evidence
-- Target: Athena Supabase project
-- Requires: 003 (recon views)
-- 
-- Keeps YAS uploads separate from PRTU (recon_bbg.yas_screenshot_url) for
-- provenance clarity. Each upload records file metadata and links back to
-- the recon_bbg row via (portfolio_id, date, isin).
-- ============================================================================

-- Add column to recon_bbg for direct file URL (simple inline storage)
ALTER TABLE recon_bbg ADD COLUMN IF NOT EXISTS yas_screenshot_url text;

-- Separate table for file metadata (scalable: multiple uploads, audit trail)
CREATE TABLE IF NOT EXISTS recon_bbg_yas (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id  text NOT NULL,
    date          date NOT NULL,
    isin          text NOT NULL,
    file_url      text NOT NULL,          -- Supabase Storage URL
    file_name     text,
    uploaded_by   text NOT NULL,
    uploaded_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (portfolio_id, date, isin)     -- one YAS per (portfolio,date,isin) per upload
);

GRANT SELECT, INSERT ON recon_bbg_yas TO anon, authenticated;
