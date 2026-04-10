-- ============================================================================
-- Recon comparison views
-- Target: Athena Supabase project (iociqthaxysqqqamonqa)
-- Requires: 000 (local_bond_* tables), 001 (recon tables), 002 (recon_view)
--
-- Naming: v_{source}_{target}_{metric}
--   source = where "our" number comes from (athena = GA10 QuantLib)
--   target = what we're comparing against (bbg, admin, maia)
--   metric = what we're comparing (accrued, yield, duration, value)
-- ============================================================================


-- ════════════════════════════════════════════════════════════════════
-- v_athena_bbg_accrued — Our accrued (QuantLib C+1) vs BBG accrued
-- Tab: Recon > Accrued > vs BBG
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_bbg_accrued CASCADE;

-- ── Stable column contract (do not rename or remove columns) ──────────────────
-- portfolio_id, date, isin, description, ticker, currency
-- bbg_nominal, athena_nominal
-- athena_t0*, athena_c1, athena_t1*, athena_c2*, athena_c3*  (* hidden in vs BBG tab)
-- bbg_accrued  ← par × accrued_pct/100; fallback to stored accrued
-- diff, diff_pct, par_mismatch
-- day_count, frequency, last_coupon_date, days_accrued        ← diagnostic
-- conv_hypothesis, conv_diff_per100                            ← iteration result
-- ─────────────────────────────────────────────────────────────────────────────
CREATE VIEW v_athena_bbg_accrued AS
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
    -- par × accrued_pct/100 gives local-currency accrued, avoids FX/scale issues.
    -- Falls back to stored b.accrued for uploads before accrued_pct was added.
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
    -- Diagnostic: what did our engine actually use?
    ab.day_count,
    br.frequency,
    ab.last_coupon_date,
    ab.days_accrued,
    -- Convention hypothesis: set when C+1 diff > 0.01/100 face after iterating all
    -- day_count × frequency × offset combinations. NULL = current convention is correct.
    ab.conv_hypothesis,
    ab.conv_diff_per100
FROM athena_bbg ab
LEFT JOIN recon_bbg b USING (portfolio_id, date, isin)
LEFT JOIN local_bond_identity bi ON bi.isin = ab.isin
LEFT JOIN local_bond_reference br ON br.isin = ab.isin;

GRANT SELECT ON v_athena_bbg_accrued TO anon, authenticated;


-- ════════════════════════════════════════════════════════════════════
-- v_athena_admin_accrued — Our accrued (QuantLib T+0) vs Admin accrued
-- Tab: Recon > Accrued > vs Admin
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_admin_accrued;

CREATE VIEW v_athena_admin_accrued AS
SELECT
    a.portfolio_id,
    a.date,
    a.isin,
    COALESCE(bi.branded_description, a.description) AS description,
    bi.branded_ticker AS ticker,
    COALESCE(br.currency, a.currency, 'USD') AS currency,
    a.par         AS admin_nominal,
    ab.par        AS athena_nominal,
    ab.accrued_t0 AS athena_t0,
    ab.accrued_c1 AS athena_c1,
    a.accrued     AS admin,
    CASE WHEN ab.accrued_t0 IS NOT NULL AND a.accrued IS NOT NULL
         THEN ab.accrued_t0 - a.accrued ELSE NULL
    END AS diff,
    CASE WHEN ab.accrued_t0 IS NOT NULL AND a.accrued IS NOT NULL AND a.accrued != 0
         THEN ROUND(((ab.accrued_t0 - a.accrued) / ABS(a.accrued) * 100)::numeric, 2)
         ELSE NULL
    END AS diff_pct
FROM recon_admin a
LEFT JOIN athena_bbg ab USING (portfolio_id, date, isin)
LEFT JOIN local_bond_identity bi ON bi.isin = a.isin
LEFT JOIN local_bond_reference br ON br.isin = a.isin;

GRANT SELECT ON v_athena_admin_accrued TO anon, authenticated;


-- ════════════════════════════════════════════════════════════════════
-- v_athena_bbg_yield — Our YTW (GA10 QuantLib) vs BBG YTW
-- Tab: Recon > Yield
-- Diff in basis points (yield × 100)
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_bbg_yield;

CREATE VIEW v_athena_bbg_yield AS
SELECT
    c.portfolio_id,
    c.date,
    c.isin,
    COALESCE(bi.branded_description, b.description) AS description,
    bi.branded_ticker AS ticker,
    COALESCE(br.currency, 'USD') AS currency,
    COALESCE(bi.coupon, br.coupon::numeric) AS coupon,
    COALESCE(bi.maturity_date, br.maturity_date) AS maturity_date,
    c.source_price AS athena_price,
    b.price        AS bbg_price,
    c.ga10_yield         AS athena_ytm,
    c.ga10_yield_worst   AS athena_ytw,
    c.ga10_yield_c1      AS athena_ytw_c1,
    c.ga10_yield_t1      AS athena_ytw_t1,
    b.yield_to_worst     AS bbg_ytw,
    CASE WHEN c.ga10_yield_worst IS NOT NULL AND b.yield_to_worst IS NOT NULL
         THEN ROUND(((c.ga10_yield_worst - b.yield_to_worst) * 100)::numeric, 1)
         ELSE NULL
    END AS diff_bps,
    CASE WHEN c.ga10_yield_worst IS NOT NULL AND b.yield_to_worst IS NOT NULL
         THEN ABS((c.ga10_yield_worst - b.yield_to_worst) * 100)
         ELSE 0
    END AS abs_bps
FROM recon_calcs c
LEFT JOIN recon_bbg b USING (portfolio_id, date, isin)
LEFT JOIN local_bond_identity bi ON bi.isin = c.isin
LEFT JOIN local_bond_reference br ON br.isin = c.isin;

GRANT SELECT ON v_athena_bbg_yield TO anon, authenticated;


-- ════════════════════════════════════════════════════════════════════
-- v_athena_bbg_duration — Our duration (GA10 QuantLib) vs BBG duration
-- Tab: Recon > Duration
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_bbg_duration;

CREATE VIEW v_athena_bbg_duration AS
SELECT
    c.portfolio_id,
    c.date,
    c.isin,
    COALESCE(bi.branded_description, b.description) AS description,
    bi.branded_ticker AS ticker,
    COALESCE(br.currency, 'USD') AS currency,
    COALESCE(bi.coupon, br.coupon::numeric) AS coupon,
    COALESCE(bi.maturity_date, br.maturity_date) AS maturity_date,
    c.ga10_duration       AS athena_duration,
    c.ga10_duration_worst AS athena_duration_worst,
    b.duration            AS bbg_duration,
    CASE WHEN COALESCE(c.ga10_duration_worst, c.ga10_duration) IS NOT NULL AND b.duration IS NOT NULL
         THEN ROUND((COALESCE(c.ga10_duration_worst, c.ga10_duration) - b.duration)::numeric, 3)
         ELSE NULL
    END AS diff,
    CASE WHEN COALESCE(c.ga10_duration_worst, c.ga10_duration) IS NOT NULL AND b.duration IS NOT NULL
         THEN ABS(COALESCE(c.ga10_duration_worst, c.ga10_duration) - b.duration)
         ELSE 0
    END AS abs_diff
FROM recon_calcs c
LEFT JOIN recon_bbg b USING (portfolio_id, date, isin)
LEFT JOIN local_bond_identity bi ON bi.isin = c.isin
LEFT JOIN local_bond_reference br ON br.isin = c.isin;

GRANT SELECT ON v_athena_bbg_duration TO anon, authenticated;


-- ════════════════════════════════════════════════════════════════════
-- v_athena_bbg_value — MV comparison: Athena vs BBG vs Admin vs Maia
-- Tab: Recon > Value
-- Athena MV = latest price (bond_analytics) × par / 100
-- BBG MV = bbg price × bbg par / 100
-- ════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS v_athena_bbg_value;

CREATE VIEW v_athena_bbg_value AS
SELECT
    COALESCE(b.portfolio_id, a.portfolio_id, m.portfolio_id) AS portfolio_id,
    COALESCE(b.date, a.date, m.date) AS date,
    COALESCE(b.isin, a.isin, m.isin) AS isin,
    COALESCE(bi.branded_description, b.description, a.description, m.description) AS description,
    bi.branded_ticker AS ticker,
    COALESCE(br.currency, 'USD') AS currency,
    COALESCE(b.par, a.par, m.par) AS athena_par,
    b.par AS bbg_par,
    ba.price       AS athena_price,
    ba.price_date  AS athena_price_date,
    ba.price_source AS athena_price_source,
    b.price        AS bbg_price,
    a.price        AS admin_price,
    m.price        AS maia_price,
    CASE WHEN ba.price IS NOT NULL AND b.price IS NOT NULL
         THEN ROUND((ba.price - b.price)::numeric, 3)
         ELSE NULL
    END AS price_diff,
    CASE WHEN ba.price IS NOT NULL AND COALESCE(b.par, a.par, m.par) IS NOT NULL
         THEN ROUND((ba.price * COALESCE(b.par, a.par, m.par) / 100)::numeric, 2)
         ELSE NULL
    END AS athena_mv,
    CASE WHEN b.par IS NOT NULL AND b.price IS NOT NULL
         THEN ROUND((b.par * b.price / 100)::numeric, 2)
         ELSE b.mv
    END AS bbg_mv,
    a.mv AS admin_mv,
    m.mv AS maia_mv,
    CASE WHEN ba.price IS NOT NULL AND COALESCE(b.par, a.par, m.par) IS NOT NULL
              AND b.par IS NOT NULL AND b.price IS NOT NULL
         THEN ROUND((ba.price * COALESCE(b.par, a.par, m.par) / 100
                    - b.par * b.price / 100)::numeric, 2)
         ELSE NULL
    END AS mv_diff,
    CASE WHEN ba.price IS NOT NULL AND COALESCE(b.par, a.par, m.par) IS NOT NULL
              AND b.par IS NOT NULL AND b.price IS NOT NULL
         THEN ABS(ba.price * COALESCE(b.par, a.par, m.par) / 100
                - b.par * b.price / 100)
         ELSE 0
    END AS abs_mv_diff
FROM recon_bbg b
FULL OUTER JOIN recon_admin a USING (portfolio_id, date, isin)
FULL OUTER JOIN recon_maia m
    ON  m.portfolio_id = COALESCE(b.portfolio_id, a.portfolio_id)
    AND m.date = COALESCE(b.date, a.date)
    AND m.isin = COALESCE(b.isin, a.isin)
LEFT JOIN local_bond_identity bi ON bi.isin = COALESCE(b.isin, a.isin, m.isin)
LEFT JOIN local_bond_reference br ON br.isin = COALESCE(b.isin, a.isin, m.isin)
LEFT JOIN local_bond_analytics ba ON ba.isin = COALESCE(b.isin, a.isin, m.isin);

GRANT SELECT ON v_athena_bbg_value TO anon, authenticated;
