-- ─────────────────────────────────────────────────────────────────────────
-- 006_fix_static_change_trigger_and_add_coupon.sql
--
-- Fixes for the calc-staleness rollout (May 2026), discovered while
-- deploying sql/005:
--
-- 1. Add coupon column to local_bond_reference. recon-mcp's sync_bond_data
--    selects coupon from upstream bond_reference but local table was missing
--    the column — silent upsert failure path.
--
-- 2. Fix notify_recon_static_changed() trigger function. Body was being
--    cast ::text but pg_net's net.http_post expects body as jsonb. Trigger
--    fires AFTER UPDATE, so every conflict-update upsert against
--    local_bond_reference was raising and reverting — root cause of
--    "local_bond_reference: 0 rows updated" on every sync.
--
-- Applied 2026-05-05 to Athena Supabase (iociqthaxysqqqamonqa).
-- ─────────────────────────────────────────────────────────────────────────

-- Step 1 — add coupon column ──────────────────────────────────────────
ALTER TABLE local_bond_reference
  ADD COLUMN IF NOT EXISTS coupon NUMERIC;

-- Step 2 — fix trigger function (drop ::text cast on body) ────────────
CREATE OR REPLACE FUNCTION public.notify_recon_static_changed()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  PERFORM net.http_post(
    url     := 'https://recon-mcp-production.up.railway.app/webhooks/static-changed',
    headers := jsonb_build_object(
      'Content-Type',     'application/json',
      'X-Webhook-Secret', 'a02e8c00fd97da39314c903226138dedfaacee4c2cfb2e7ff8466a70672e3ff6'
    ),
    body    := jsonb_build_object(
      'type',       TG_OP,
      'table',      TG_TABLE_NAME,
      'schema',     TG_TABLE_SCHEMA,
      'record',     row_to_json(NEW),
      'old_record', row_to_json(OLD)
    )
  );
  RETURN NEW;
END;
$function$;

-- Verification:
--   Trigger one upsert against local_bond_reference, then:
--   SELECT COUNT(*) FROM local_bond_reference WHERE current_static_hash IS NOT NULL;
--   -> matches the synced ISIN count

-- ─── REVERSAL ────────────────────────────────────────────────────────────
-- Step 2 reversal would restore the broken ::text cast — pointless. Leave fixed.
-- Step 1 reversal:
--   ALTER TABLE local_bond_reference DROP COLUMN IF EXISTS coupon;
