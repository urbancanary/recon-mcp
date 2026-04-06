-- ============================================================================
-- recon_view — Joins all recon source tables + live bond-data via FDW
-- Target: Athena Supabase project (iociqthaxysqqqamonqa)
-- Requires: 000_fdw_bond_data.sql run first
--
-- This is the single view that recon-mcp queries via GET /recon/data.
-- All field names here become the JSON keys returned to Athena's frontend.
--
-- Descriptions come from bond_identity (corrected/branded, live via FDW).
-- Static data (currency, country, sector) from bond_reference (live via FDW).
-- Prices from bond_analytics (latest snapshot, live via FDW).
-- ============================================================================

DROP VIEW IF EXISTS recon_view;

CREATE VIEW recon_view AS
SELECT
    -- ── Key columns ───────────────────────────────────────────────────
    COALESCE(b.portfolio_id, a.portfolio_id, m.portfolio_id)  AS portfolio_id,
    COALESCE(b.date, a.date, m.date)                          AS date,
    COALESCE(b.isin, a.isin, m.isin)                          AS isin,

    -- ── Bond identity (live from bond-data, always current) ───────────
    COALESCE(
        bi.branded_description,
        bi.issuer_description,
        br.ticker_description,
        b.description, a.description, m.description
    ) AS description,
    COALESCE(br.currency, b.currency, a.currency, m.currency, 'USD') AS currency,
    COALESCE(bi.coupon, br.coupon::numeric, b.coupon, a.coupon, m.coupon) AS coupon,
    COALESCE(bi.maturity_date, br.maturity_date, b.maturity_date, a.maturity_date, m.maturity_date) AS maturity_date,
    br.standard_country AS country,
    br.sector,
    br.applied_rating AS rating,
    bi.branded_ticker AS ticker,

    -- ── BBG source data ───────────────────────────────────────────────
    b.par           AS bbg_par,
    b.price         AS bbg_price,
    b.accrued       AS bbg_accrued,
    b.yield_to_worst AS bbg_ytw,
    b.duration      AS bbg_duration,
    b.mv            AS bbg_mv,
    CASE WHEN b.par IS NOT NULL AND b.price IS NOT NULL
         THEN b.par * b.price / 100
         ELSE NULL
    END AS bbg_mv_computed,

    -- ── Admin source data ─────────────────────────────────────────────
    a.par           AS admin_par,
    a.price         AS admin_price,
    a.accrued       AS admin_accrued,
    a.mv            AS admin_mv,
    a.country       AS admin_country,

    -- ── Maia source data ──────────────────────────────────────────────
    m.par           AS maia_par,
    m.price         AS maia_price,
    m.mv            AS maia_mv,

    -- ── Athena price (latest from bond-data, live via FDW) ────────────
    ba.price        AS athena_price,
    ba.price_date   AS athena_price_date,
    ba.price_source AS athena_price_source,

    -- ── GA10 QuantLib calculations (per-100 face, from BBG prices) ────
    c.source_price,
    c.ga10_accrued,
    c.ga10_accrued_c1,
    c.ga10_accrued_t1,
    c.ga10_accrued_t2,
    c.ga10_accrued_t3,
    c.ga10_yield,
    c.ga10_yield_c1,
    c.ga10_yield_t1,
    c.ga10_yield_worst,
    c.ga10_duration,
    c.ga10_duration_worst,
    c.ga10_spread,
    c.ga10_convexity,
    c.ga10_dv01,

    -- ── Athena BBG: par-scaled absolute accrued interest ──────────────
    ab.par          AS athena_par,
    ab.accrued_t0   AS athena_accrued,
    ab.accrued_c1   AS athena_accrued_c1,
    ab.accrued_t1   AS athena_accrued_t1,
    ab.accrued_c2   AS athena_accrued_t2,
    ab.accrued_c3   AS athena_accrued_t3,

    -- ── Derived aliases ───────────────────────────────────────────────
    c.ga10_yield_worst  AS athena_ytw,
    c.ga10_duration     AS athena_duration,
    c.ga10_duration_worst AS athena_duration_worst

FROM recon_bbg b
FULL OUTER JOIN recon_admin a
    ON  a.portfolio_id = b.portfolio_id
    AND a.date = b.date
    AND a.isin = b.isin
FULL OUTER JOIN recon_maia m
    ON  m.portfolio_id = COALESCE(b.portfolio_id, a.portfolio_id)
    AND m.date = COALESCE(b.date, a.date)
    AND m.isin = COALESCE(b.isin, a.isin)
LEFT JOIN recon_calcs c
    ON  c.portfolio_id = COALESCE(b.portfolio_id, a.portfolio_id, m.portfolio_id)
    AND c.date = COALESCE(b.date, a.date, m.date)
    AND c.isin = COALESCE(b.isin, a.isin, m.isin)
LEFT JOIN athena_bbg ab
    ON  ab.portfolio_id = COALESCE(b.portfolio_id, a.portfolio_id, m.portfolio_id)
    AND ab.date = COALESCE(b.date, a.date, m.date)
    AND ab.isin = COALESCE(b.isin, a.isin, m.isin)
-- ── Live bond-data via FDW ──────────────────────────────────────────
LEFT JOIN local_bond_identity bi
    ON  bi.isin = COALESCE(b.isin, a.isin, m.isin)
LEFT JOIN local_bond_reference br
    ON  br.isin = COALESCE(b.isin, a.isin, m.isin)
LEFT JOIN local_bond_analytics ba
    ON  ba.isin = COALESCE(b.isin, a.isin, m.isin);

-- Grant access to anon role for PostgREST
GRANT SELECT ON recon_view TO anon;
GRANT SELECT ON recon_view TO authenticated;
