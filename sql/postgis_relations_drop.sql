-- Drop all FK constraints and secondary indexes on managed PostGIS tables.
-- Idempotent: every statement uses IF EXISTS so missing objects are no-ops.
-- Executed before bulk loads on a fresh start (and before every full reload),
-- so any pre-existing relations are removed even when tables are recreated.

DROP INDEX IF EXISTS public.idx_assets_assettype;
DROP INDEX IF EXISTS public.idx_assets_actief;

ALTER TABLE IF EXISTS public.geometrie DROP CONSTRAINT IF EXISTS geometrie_assets_fkey;
DROP INDEX IF EXISTS public.idx_geometrie_geom;

ALTER TABLE IF EXISTS public.bestekkoppelingen DROP CONSTRAINT IF EXISTS bestekkoppelingen_assets_fkey;
ALTER TABLE IF EXISTS public.bestekkoppelingen DROP CONSTRAINT IF EXISTS bestekkoppelingen_bestekken_fkey;
DROP INDEX IF EXISTS public.idx_bestekkoppelingen_bestek_uuid;
DROP INDEX IF EXISTS public.idx_bestekkoppelingen_asset_uuid;

ALTER TABLE IF EXISTS public.assets DROP CONSTRAINT IF EXISTS assets_assettypes_fkey;
ALTER TABLE IF EXISTS public.assets DROP CONSTRAINT IF EXISTS beheerders_assets_fkey;
ALTER TABLE IF EXISTS public.assets DROP CONSTRAINT IF EXISTS toezichtgroepen_assets_fkey;
ALTER TABLE IF EXISTS public.assets DROP CONSTRAINT IF EXISTS identiteiten_assets_fkey;

ALTER TABLE IF EXISTS public.assetrelaties DROP CONSTRAINT IF EXISTS assetrelaties_bron_fkey;
ALTER TABLE IF EXISTS public.assetrelaties DROP CONSTRAINT IF EXISTS assetrelaties_doel_fkey;
ALTER TABLE IF EXISTS public.assetrelaties DROP CONSTRAINT IF EXISTS assetrelaties_relatietype_fkey;
DROP INDEX IF EXISTS public.idx_assetrelaties_bron;
DROP INDEX IF EXISTS public.idx_assetrelaties_doel;
DROP INDEX IF EXISTS public.idx_assetrelaties_relatietype;
DROP INDEX IF EXISTS public.idx_assetrelaties_bron_type;
DROP INDEX IF EXISTS public.idx_assetrelaties_doel_type;

ALTER TABLE IF EXISTS public.betrokkenerelaties DROP CONSTRAINT IF EXISTS betrokkenerelaties_bron_asset_fkey;
ALTER TABLE IF EXISTS public.betrokkenerelaties DROP CONSTRAINT IF EXISTS betrokkenerelaties_bron_agent_fkey;
ALTER TABLE IF EXISTS public.betrokkenerelaties DROP CONSTRAINT IF EXISTS betrokkenerelaties_doel_agent_fkey;
DROP INDEX IF EXISTS public.idx_betrokkenerelaties_bron_asset;
DROP INDEX IF EXISTS public.idx_betrokkenerelaties_bron_agent;
DROP INDEX IF EXISTS public.idx_betrokkenerelaties_doel_agent;
DROP INDEX IF EXISTS public.idx_betrokkenerelaties_actief;
