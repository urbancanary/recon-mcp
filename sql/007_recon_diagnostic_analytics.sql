-- ════════════════════════════════════════════════════════════════════
-- 007 — Diagnostic analytics from the per-bond GA10 fan-out
--
-- PREPARED ONLY. Not applied by any lane. Apply by hand, then merge.
--
-- WHY
--   recon_engine.recalc_with_bbg_prices calls GA10's batched FLDS
--   /api/v1/portfolio/analysis for accrued / yield / duration / spread, which
--   is the load-bearing recon. That endpoint does NOT return ytw separately
--   from ytm, nor convexity, pvbp, dirty_price, accrued_days or ytal.
--
--   The per-bond /api/v1/bond/analysis endpoint does. recon_engine now fans out
--   one call per bond and needs somewhere to put the answers.
--
-- WHAT THIS DOES NOT DO
--   It does not touch any column the recon views compare against BBG:
--   ga10_yield, ga10_yield_c1, ga10_yield_worst, ga10_ytal, ga10_duration,
--   ga10_spread, ga10_accrued*, athena_bbg.accrued_*, days_accrued.
--   Those keep their meaning. A price, a yield and an accrued a client reads
--   are unchanged by this migration and by the code that writes alongside it.
--
--   ga10_yield_worst stays "the yield we compare against BBG YTW" — FLDS
--   convention-aware `yield`, or the call-override YTW for callables. The new
--   ga10_ytw_explicit is GA10's own ytw for the same bond/price/settle, kept
--   BESIDE it, not in place of it. Where the two disagree the difference is a
--   convention fact. That difference is a question for Andy, not a lane:
--   see the report for recon-mcp package theme:analytics.
-- ════════════════════════════════════════════════════════════════════

-- ── recon_calcs: the per-bond split ─────────────────────────────────
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_ytw_explicit     numeric;
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_amortizing_yield numeric;
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_dirty_price      numeric;
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_accrued_days     integer;
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_is_amortizing    boolean;
-- Comparator vs GA10's explicit ytw, in bps. NULL unless both are known.
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS ga10_ytw_delta_bps    numeric;
-- GA10's own conventions block (day_count / frequency / BDC / EOM), as one
-- JSON blob — it is evidence about which convention GA10 used, not a number
-- the page asserts, so it is stored whole rather than flattened.
ALTER TABLE recon_calcs ADD COLUMN IF NOT EXISTS convention_used_detail jsonb;

-- ── athena_bbg: dirty price, PER 100, NOT par-scaled ────────────────
-- The accrued_* columns on this table are par-scaled dollars and are frozen
-- and BBG-comparable. dirty_price is a price; storing it par-scaled would make
-- it look like cash. Keep it per-100, consistent with source_price.
ALTER TABLE athena_bbg ADD COLUMN IF NOT EXISTS dirty_price    numeric;
ALTER TABLE athena_bbg ADD COLUMN IF NOT EXISTS dirty_price_c1 numeric;

COMMENT ON COLUMN recon_calcs.ga10_yield_worst IS
  'Yield compared against BBG yield_to_worst. FLDS convention-aware `yield`, '
  'or the call-override YTW for callable bonds. NOT GA10''s analytics.ytw — '
  'that is ga10_ytw_explicit.';
COMMENT ON COLUMN recon_calcs.ga10_ytw_explicit IS
  'GA10 /api/v1/bond/analysis analytics.ytw for this bond/price/settle. '
  'Diagnostic companion to ga10_yield_worst, not a replacement.';


-- ════════════════════════════════════════════════════════════════════
-- v_athena_bbg_yield_diag — same rows as v_athena_bbg_yield, plus the
-- diagnostic split and the comparator-vs-explicit gap.
--
-- A NEW view, deliberately not a change to v_athena_bbg_yield: that view feeds
-- the recon badge and its athena_ytconv / diff_bps_ytconv columns are
-- client-visible. Nothing there moves until the convention question is
-- answered.
--
-- `comparable` is true only where the comparator we already use IS the same
-- workout GA10 named explicitly, i.e. where the gap is zero to 1/100 bp. Rows
-- where it is false are the ones worth a human eye; the UI should mute them,
-- not red-flag them — a convention difference is not a break.
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_bbg_yield_diag;

CREATE VIEW v_athena_bbg_yield_diag AS
SELECT
    y.portfolio_id,
    y.date,
    y.isin,
    y.description,
    y.ticker,
    y.currency,
    y.maturity_date,
    y.athena_price,
    y.bbg_price,
    -- What the recon already uses.
    y.athena_ytconv,
    y.convention,
    y.bbg_ytw,
    y.diff_bps_ytconv,
    -- The split GA10 returns per bond.
    c.ga10_ytw_explicit,
    y.athena_ytal,
    c.ga10_amortizing_yield,
    c.ga10_is_amortizing,
    c.ga10_dirty_price,
    c.ga10_accrued_days,
    c.convention_used,
    c.convention_used_detail,
    -- Gap between the comparator and GA10's explicit ytw, bps.
    c.ga10_ytw_delta_bps,
    -- True where the comparator is the workout GA10 named explicitly.
    CASE
        WHEN c.ga10_ytw_delta_bps IS NULL THEN NULL
        ELSE ABS(c.ga10_ytw_delta_bps) < 0.01
    END AS comparable,
    -- How many days off the fan-out's own accrued count is from the BBG-style
    -- ACT count the recon stores. GA10's accrued_days is known to disagree on
    -- stub periods (see recon_engine's days_accrued comment) — this makes the
    -- disagreement visible per bond instead of hypothetical.
    CASE
        WHEN c.ga10_accrued_days IS NULL OR ab.days_accrued IS NULL THEN NULL
        ELSE c.ga10_accrued_days - ab.days_accrued
    END AS accrued_days_delta
FROM v_athena_bbg_yield y
LEFT JOIN recon_calcs c
       ON c.portfolio_id = y.portfolio_id AND c.date = y.date AND c.isin = y.isin
LEFT JOIN athena_bbg ab
       ON ab.portfolio_id = y.portfolio_id AND ab.date = y.date AND ab.isin = y.isin;

GRANT SELECT ON v_athena_bbg_yield_diag TO anon, authenticated;
