-- Create all FK constraints and secondary indexes on managed PostGIS tables.
-- Idempotent: every statement uses IF NOT EXISTS / DO blocks so existing
-- objects are no-ops. Executed as the final step after all data has been synced.

DO $$
BEGIN
    ALTER TABLE public.geometrie
        ADD CONSTRAINT geometrie_assets_fkey
        FOREIGN KEY (asset_uuid) REFERENCES public.assets (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_geometrie_geom ON public.geometrie USING GIST (geom);

DO $$
BEGIN
    ALTER TABLE public.bestekkoppelingen
        ADD CONSTRAINT bestekkoppelingen_assets_fkey
        FOREIGN KEY (asset_uuid) REFERENCES public.assets (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.bestekkoppelingen
        ADD CONSTRAINT bestekkoppelingen_bestekken_fkey
        FOREIGN KEY (bestek_uuid) REFERENCES public.bestekken (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_bestekkoppelingen_bestek_uuid ON public.bestekkoppelingen (bestek_uuid);
CREATE INDEX IF NOT EXISTS idx_bestekkoppelingen_asset_uuid ON public.bestekkoppelingen (asset_uuid);

DO $$
BEGIN
    ALTER TABLE public.assets
        ADD CONSTRAINT assets_assettypes_fkey
        FOREIGN KEY (assettype) REFERENCES public.assettypes (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_assets_assettype ON public.assets (assettype);
CREATE INDEX IF NOT EXISTS idx_assets_actief ON public.assets (actief);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'schadebeheerder'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT beheerders_assets_fkey
            FOREIGN KEY (schadebeheerder) REFERENCES public.beheerders (uuid)
            MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'toezichtgroep'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT toezichtgroepen_assets_fkey
            FOREIGN KEY (toezichtgroep) REFERENCES public.toezichtgroepen (uuid)
            MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'toezichter'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT identiteiten_assets_fkey
            FOREIGN KEY (toezichter) REFERENCES public.identiteiten (uuid)
            MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.assetrelaties
        ADD CONSTRAINT assetrelaties_bron_fkey
        FOREIGN KEY (bron_asset_uuid) REFERENCES public.assets (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.assetrelaties
        ADD CONSTRAINT assetrelaties_doel_fkey
        FOREIGN KEY (doel_asset_uuid) REFERENCES public.assets (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.assetrelaties
        ADD CONSTRAINT assetrelaties_relatietype_fkey
        FOREIGN KEY (relatietype_uuid) REFERENCES public.relatietypes (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_assetrelaties_bron ON public.assetrelaties (bron_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_doel ON public.assetrelaties (doel_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_relatietype ON public.assetrelaties (relatietype_uuid);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_bron_type ON public.assetrelaties (bron_type);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_doel_type ON public.assetrelaties (doel_type);

DO $$
BEGIN
    ALTER TABLE public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_bron_asset_fkey
        FOREIGN KEY (bron_asset_uuid) REFERENCES public.assets (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_bron_agent_fkey
        FOREIGN KEY (bron_agent_uuid) REFERENCES public.agents (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_doel_agent_fkey
        FOREIGN KEY (doel_agent_uuid) REFERENCES public.agents (uuid)
        MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_bron_asset ON public.betrokkenerelaties (bron_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_bron_agent ON public.betrokkenerelaties (bron_agent_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_doel_agent ON public.betrokkenerelaties (doel_agent_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_actief ON public.betrokkenerelaties (actief);
