-- Migration 004: Convention hypothesis + best-offset analysis for accrued recon
--
-- Adds two columns to athena_bbg (stored by recalc_accrued):
--   conv_hypothesis:  e.g. "ACT/365 Annual C+1" — the day_count×freq×offset combo
--                     that best matches BBG when C+1 is off by >0.01/100 face.
--   conv_diff_per100: accrued diff per 100 face at the hypothesis (0 = perfect match)
--
-- Adds two computed columns to v_athena_bbg_accrued:
--   best_offset:       which of T+0/C+1/C+2/C+3 is closest to BBG
--   best_diff_per100:  diff per 100 face at best offset

ALTER TABLE athena_bbg
    ADD COLUMN IF NOT EXISTS conv_hypothesis  TEXT,
    ADD COLUMN IF NOT EXISTS conv_diff_per100 FLOAT8;

-- Rebuild the accrued view with new stored and computed columns
DROP VIEW IF EXISTS v_athena_bbg_accrued CASCADE;

CREATE VIEW v_athena_bbg_accrued AS
WITH base AS (
    SELECT
        ab.portfolio_id,
        ab.date,
        ab.isin,
        COALESCE(bi.branded_description, b.description) AS description,
        bi.branded_ticker AS ticker,
        COALESCE(br.currency, 'USD') AS currency,
        b.par         AS bbg_nominal,
        ab.par        AS athena_nominal,
        ab.accrued_t0 AS athena_t0,
        ab.accrued_c1 AS athena_c1,
        ab.accrued_t1 AS athena_t1,
        ab.accrued_c2 AS athena_c2,
        ab.accrued_c3 AS athena_c3,
        -- par × accrued_pct/100 gives local-currency accrued without FX/scale issues.
        -- Falls back to stored b.accrued for older uploads.
        COALESCE(ab.par * b.accrued_pct / 100, b.accrued) AS bbg_accrued,
        CASE WHEN ab.accrued_c1 IS NOT NULL
                  AND COALESCE(ab.par * b.accrued_pct / 100, b.accrued) IS NOT NULL
             THEN ab.accrued_c1 - COALESCE(ab.par * b.accrued_pct / 100, b.accrued) ELSE NULL
        END AS diff,
        CASE WHEN ab.accrued_c1 IS NOT NULL
                  AND COALESCE(ab.par * b.accrued_pct / 100, b.accrued) IS NOT NULL
                  AND COALESCE(ab.par * b.accrued_pct / 100, b.accrued) != 0
             THEN ROUND(((ab.accrued_c1 - COALESCE(ab.par * b.accrued_pct / 100, b.accrued))
                         / ABS(COALESCE(ab.par * b.accrued_pct / 100, b.accrued)) * 100)::numeric, 2)
             ELSE NULL
        END AS diff_pct,
        (ab.par IS NOT NULL AND b.par IS NOT NULL AND ab.par != b.par) AS par_mismatch,
        -- Diagnostic: what convention our engine used
        ab.day_count,
        br.frequency,
        ab.last_coupon_date,
        ab.days_accrued,
        -- Convention hypothesis (stored by recalc_accrued when C+1 diff > 0.01/100 face)
        ab.conv_hypothesis,
        ab.conv_diff_per100,
        -- For best-offset computed columns
        ab.par AS _par
    FROM athena_bbg ab
    LEFT JOIN recon_bbg b USING (portfolio_id, date, isin)
    LEFT JOIN local_bond_identity bi ON bi.isin = ab.isin
    LEFT JOIN local_bond_reference br ON br.isin = ab.isin
)
SELECT
    portfolio_id, date, isin, description, ticker, currency,
    bbg_nominal, athena_nominal,
    athena_t0, athena_c1, athena_t1, athena_c2, athena_c3,
    bbg_accrued,
    diff, diff_pct, par_mismatch,
    day_count, frequency, last_coupon_date, days_accrued,
    conv_hypothesis, conv_diff_per100,
    -- Which settlement offset is closest to BBG?
    CASE
        WHEN bbg_accrued IS NULL THEN NULL
        WHEN COALESCE(ABS(COALESCE(athena_t0, 1e10) - bbg_accrued), 1e10) <=
             LEAST(
                 COALESCE(ABS(COALESCE(athena_c1, 1e10) - bbg_accrued), 1e10),
                 COALESCE(ABS(COALESCE(athena_c2, 1e10) - bbg_accrued), 1e10),
                 COALESCE(ABS(COALESCE(athena_c3, 1e10) - bbg_accrued), 1e10)
             ) THEN 'T+0'
        WHEN COALESCE(ABS(COALESCE(athena_c1, 1e10) - bbg_accrued), 1e10) <=
             LEAST(
                 COALESCE(ABS(COALESCE(athena_c2, 1e10) - bbg_accrued), 1e10),
                 COALESCE(ABS(COALESCE(athena_c3, 1e10) - bbg_accrued), 1e10)
             ) THEN 'C+1'
        WHEN COALESCE(ABS(COALESCE(athena_c2, 1e10) - bbg_accrued), 1e10) <=
             COALESCE(ABS(COALESCE(athena_c3, 1e10) - bbg_accrued), 1e10)
            THEN 'C+2'
        ELSE 'C+3'
    END AS best_offset,
    -- Diff per 100 face at best offset
    CASE
        WHEN bbg_accrued IS NULL OR _par IS NULL OR _par = 0 THEN NULL
        ELSE ROUND((
            LEAST(
                COALESCE(ABS(COALESCE(athena_t0, 1e10) - bbg_accrued), 1e10),
                COALESCE(ABS(COALESCE(athena_c1, 1e10) - bbg_accrued), 1e10),
                COALESCE(ABS(COALESCE(athena_c2, 1e10) - bbg_accrued), 1e10),
                COALESCE(ABS(COALESCE(athena_c3, 1e10) - bbg_accrued), 1e10)
            ) / _par * 100
        )::numeric, 4)
    END AS best_diff_per100
FROM base;

GRANT SELECT ON v_athena_bbg_accrued TO anon, authenticated;
