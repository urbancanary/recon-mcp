-- ─────────────────────────────────────────────────────────────────────────
-- 005_calc_staleness_hashes.sql
--
-- Hash-based calc staleness detector. Stamps recon_calcs rows with the
-- inputs used (static_hash, price_hash) and the engine layer ids that
-- produced them. A view (v_stale_calcs) joins against the current static
-- hash on local_bond_reference to surface rows where the inputs no longer
-- match.
--
-- See: athena_html_v3 memory project_calc_staleness_detector.md
-- See: codebase-mcp backlog id=371 (Option A — promote engine_hash to GCP
--      backend; this migration ships Option C: CF Worker layer ids only)
--
-- RUN STATEMENTS ONE AT A TIME, verifying after each. Per project CLAUDE.md.
-- All ALTERs are additive (NULL-allowed) — existing rows survive untouched
-- and read as NULL, which v_stale_calcs treats as "drift" so they are
-- recalculated naturally on next cron pass.
-- ─────────────────────────────────────────────────────────────────────────

-- Step 1 — recon_calcs: provenance columns ──────────────────────────────
ALTER TABLE recon_calcs
  ADD COLUMN IF NOT EXISTS calc_static_hash       TEXT,
  ADD COLUMN IF NOT EXISTS calc_price_hash        TEXT,
  ADD COLUMN IF NOT EXISTS calc_engine_pricing_id TEXT,
  ADD COLUMN IF NOT EXISTS calc_engine_gateway_id TEXT,
  ADD COLUMN IF NOT EXISTS calculated_at          TIMESTAMPTZ;

-- Verification:
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name='recon_calcs' AND column_name LIKE 'calc_%';
--   -> 4 rows expected (calc_static_hash, calc_price_hash,
--      calc_engine_pricing_id, calc_engine_gateway_id) plus calculated_at

-- Step 2 — recon_calcs: index for the comparison join ───────────────────
CREATE INDEX IF NOT EXISTS idx_recon_calcs_static_hash
  ON recon_calcs(isin, calc_static_hash);

-- Step 3 — local_bond_reference: current static hash ────────────────────
ALTER TABLE local_bond_reference
  ADD COLUMN IF NOT EXISTS current_static_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_lbr_static_hash
  ON local_bond_reference(current_static_hash);

-- Step 4 — v_stale_calcs view ───────────────────────────────────────────
-- Treats NULL stored hashes as drift, which is correct: post-migration the
-- existing rows have NULL calc_*_hash and the cron should recalc them all
-- (they will then be stamped). After backfill the NULL state never returns.
--
-- Engine ids come from GUC vars set per-session by the cron worker:
--   SET LOCAL app.engine_pricing_id = '<id from GET ga10-pricing/engine/version>';
--   SET LOCAL app.engine_gateway_id = '<id from GET ga10-gateway/engine/version>';
-- If a GUC is unset (current_setting returns ''), engine_drift is treated
-- as false to avoid spurious work on ad-hoc reads.

CREATE OR REPLACE VIEW v_stale_calcs AS
SELECT
  c.portfolio_id,
  c.date,
  c.isin,
  -- Drift only when reference hash exists AND calc hash is missing or different.
  -- If r.current_static_hash IS NULL we cannot tell — leave alone. Avoids
  -- cron looping forever on bonds with missing bond_reference data.
  (r.current_static_hash IS NOT NULL
   AND (c.calc_static_hash IS NULL
        OR c.calc_static_hash <> r.current_static_hash)) AS static_drift,
  (NULLIF(current_setting('app.engine_pricing_id', true), '') IS NOT NULL
   AND (c.calc_engine_pricing_id IS NULL
        OR c.calc_engine_pricing_id <> current_setting('app.engine_pricing_id', true))) AS engine_pricing_drift,
  (NULLIF(current_setting('app.engine_gateway_id', true), '') IS NOT NULL
   AND (c.calc_engine_gateway_id IS NULL
        OR c.calc_engine_gateway_id <> current_setting('app.engine_gateway_id', true))) AS engine_gateway_drift,
  c.calc_static_hash,
  r.current_static_hash,
  c.calc_engine_pricing_id,
  c.calc_engine_gateway_id,
  c.calculated_at
FROM recon_calcs c
LEFT JOIN local_bond_reference r ON r.isin = c.isin
WHERE (r.current_static_hash IS NOT NULL
       AND (c.calc_static_hash IS NULL
            OR c.calc_static_hash <> r.current_static_hash))
   OR (NULLIF(current_setting('app.engine_pricing_id', true), '') IS NOT NULL
       AND (c.calc_engine_pricing_id IS NULL
            OR c.calc_engine_pricing_id <> current_setting('app.engine_pricing_id', true)))
   OR (NULLIF(current_setting('app.engine_gateway_id', true), '') IS NOT NULL
       AND (c.calc_engine_gateway_id IS NULL
            OR c.calc_engine_gateway_id <> current_setting('app.engine_gateway_id', true)));

-- Verification:
--   SELECT count(*) FROM v_stale_calcs;
--   -> equals total recon_calcs rows immediately after migration
--      (because every row has NULL calc_static_hash). Drops to ~0 once
--      the cron has run a full pass.


-- ─── REVERSAL ────────────────────────────────────────────────────────────
-- Run these in reverse order to undo the migration:
--
-- DROP VIEW IF EXISTS v_stale_calcs;
-- DROP INDEX IF EXISTS idx_lbr_static_hash;
-- ALTER TABLE local_bond_reference DROP COLUMN IF EXISTS current_static_hash;
-- DROP INDEX IF EXISTS idx_recon_calcs_static_hash;
-- ALTER TABLE recon_calcs
--   DROP COLUMN IF EXISTS calculated_at,
--   DROP COLUMN IF EXISTS calc_engine_gateway_id,
--   DROP COLUMN IF EXISTS calc_engine_pricing_id,
--   DROP COLUMN IF EXISTS calc_price_hash,
--   DROP COLUMN IF EXISTS calc_static_hash;
