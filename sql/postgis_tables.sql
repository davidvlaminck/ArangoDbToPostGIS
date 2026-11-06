-- Table definitions copied from AWVInfraPostGISSyncer/setup_tables_querys.sql
-- (same level as this repo)
--
-- NOT NULL annotations added after inspecting live data: every column
-- marked NOT NULL here had zero NULL (and zero empty-string) values across
-- all rows at analysis time. Columns that had any NULL/empty are left nullable.

-- Table: public.params

CREATE TABLE IF NOT EXISTS public.params
(
    key_name VARCHAR(40) NOT NULL,
    value_int integer,
    value_text text,
    value_bool boolean,
    value_timestamp TIMESTAMP WITH TIME ZONE,
    CONSTRAINT params_pkey PRIMARY KEY (key_name)
);

INSERT INTO public.params(key_name, value_bool)
VALUES ('fresh_start', TRUE)
ON CONFLICT (key_name) DO UPDATE SET value_bool = EXCLUDED.value_bool;

INSERT INTO public.params(key_name, value_int)
VALUES ('pagesize', 100)
ON CONFLICT (key_name) DO UPDATE SET value_int = EXCLUDED.value_int;

-- Table: public.agents

CREATE TABLE IF NOT EXISTS public.agents
(
    uuid uuid NOT NULL,
    naam text NOT NULL,
    void uuid,
    actief boolean NOT NULL,
    contact_info jsonb NOT NULL,
    ovo_code text,
    PRIMARY KEY (uuid)
);


-- Table: public.assettypes

CREATE TABLE IF NOT EXISTS public.assettypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    label text COLLATE pg_catalog."default" NOT NULL,
    uri text COLLATE pg_catalog."default" NOT NULL,
    short_uri text COLLATE pg_catalog."default" NOT NULL,
    definitie text COLLATE pg_catalog."default" NOT NULL,
    actief boolean NOT NULL,
    -- bestek boolean,
    -- geometrie boolean,
    -- locatie boolean,
    -- beheerder boolean,
    -- toezicht boolean,
    -- gevoedDoor boolean,
    -- elek_aansluiting boolean,
    -- vplan boolean,
    -- attributen boolean,
    CONSTRAINT assettypes_pkey PRIMARY KEY (uuid)
);


-- Table: public.assets
-- Column mapping derived from attributes.md (Arango assets, namespace buckets normalized).
-- geometry and bs are intentionally skipped for now (planned separate tables).

CREATE TABLE IF NOT EXISTS public.assets
(
    uuid uuid NOT NULL,
    assettype uuid,                        -- full assettype uuid (resolved from Arango assettype_key)
    actief boolean NOT NULL,
    asset_id_identificator text,
    asset_id_toegekend_door text,
    type_uri text,
    toestand text,
    notitie text,
    naam text,
    naampad text,
    naampad_parts jsonb,
    naampad_parent text,
    asset_versie_context text,
    asset_versie_timestamp timestamptz,
    asset_versie_versienummer text,
    datum_oprichting_object date,
    -- namespace buckets (jsonb) / special fields
    geo jsonb,
    wkt text,
    loc jsonb,
    grp jsonb,
    wl jsonb,
    bz jsonb,
    tz jsonb,
    lgc jsonb,
    ins jsonb,
    ond jsonb,
    vtc jsonb,
    -- all remaining Arango fields not mapped above (excl. @id, @type)
    attributes jsonb,
    CONSTRAINT assets_pkey PRIMARY KEY (uuid)
);

CREATE INDEX IF NOT EXISTS idx_assets_assettype ON public.assets (assettype);

-- Optional JSONB indexes (add only if/when specific queries are slow; not created by default).
-- jsonb is built-in (no extension). GIN indexes are large and slow writes/sync at 4M rows,
-- so prefer a targeted index on the concrete column/path you search, not all jsonb columns.
--
-- Containment queries (e.g. attributes @> '{"SomeKey":"value"}'):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_assets_attributes_gin
--     ON public.assets USING GIN (attributes jsonb_path_ops);  -- smaller; supports @>
--   -- use jsonb_ops instead of jsonb_path_ops if you also need key-existence (?, ?|, ?&).
--
-- Single nested path / equality (often best):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_assets_attr_somekey
--     ON public.assets ((attributes->>'SomeKey'));
--
-- Extensions (not required for the above): btree_gin only for mixed multicolumn GIN;
-- pg_trgm only for LIKE/ILIKE/trigram text search, not JSONB containment.

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assets
        ADD CONSTRAINT assets_assettypes_fkey
        FOREIGN KEY (assettype)
        REFERENCES public.assettypes (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN
    -- constraint already exists, skip
END $$;


-- Table: public.geometrie
-- Canonical spatial data derived from Arango asset top-level `wkt` (SRID=3812 / Lambert 2008).
-- WKT is the lead; no fallback to Arango `geometry` (GeoJSON) or `geo.Geometrie_log`.

CREATE TABLE IF NOT EXISTS public.geometrie
(
    asset_uuid uuid NOT NULL,
    wkt text NOT NULL,
    geom geometry(GeometryZ, 3812) NOT NULL,
    CONSTRAINT geometrie_pkey PRIMARY KEY (asset_uuid),
    CONSTRAINT geometrie_assets_fkey FOREIGN KEY (asset_uuid)
        REFERENCES public.assets (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_geometrie_geom ON public.geometrie USING GIST (geom);


-- Table: public.bestekken

CREATE TABLE IF NOT EXISTS public.bestekken
(
    uuid uuid NOT NULL,
    e_delta_dossiernummer text COLLATE pg_catalog."default",
    e_delta_besteknummer text COLLATE pg_catalog."default",
    awv_id text COLLATE pg_catalog."default",
    aannemer_naam text COLLATE pg_catalog."default",
    aannemer_referentie text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT bestekken_pkey PRIMARY KEY (uuid)
);


-- Table: public.bestekkoppelingen
-- n-n relation between assets and bestekken, derived from Arango edge collection
-- `bestekkoppelingen` (edge attribute `volgnummer`, transitional fallback: `volgorde`).
-- `volgnummer` preserves Arango edge order as provided (may be 0-based).

CREATE TABLE IF NOT EXISTS public.bestekkoppelingen
(
    asset_uuid uuid NOT NULL,
    bestek_uuid uuid NOT NULL,
    volgnummer smallint NOT NULL,
    start_datum timestamptz,
    eind_datum timestamptz,
    koppeling_status text,
    CONSTRAINT bestekkoppelingen_pkey PRIMARY KEY (asset_uuid, volgnummer),
    CONSTRAINT bestekkoppelingen_assets_fkey FOREIGN KEY (asset_uuid)
        REFERENCES public.assets (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bestekkoppelingen_bestek_uuid
    ON public.bestekkoppelingen (bestek_uuid);
CREATE INDEX IF NOT EXISTS idx_bestekkoppelingen_asset_uuid
    ON public.bestekkoppelingen (asset_uuid);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.bestekkoppelingen
        ADD CONSTRAINT bestekkoppelingen_bestekken_fkey
        FOREIGN KEY (bestek_uuid)
        REFERENCES public.bestekken (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN
    NULL;
END $$;


-- Table: public.beheerders

CREATE TABLE IF NOT EXISTS public.beheerders
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    referentie text COLLATE pg_catalog."default" NOT NULL,
    type_beheerder text COLLATE pg_catalog."default" NOT NULL,
    actief boolean NOT NULL,
    CONSTRAINT beheerders_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'schadebeheerder'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT beheerders_assets_fkey
            FOREIGN KEY (schadebeheerder)
            REFERENCES public.beheerders (uuid) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN
    NULL;
END $$;


-- Table: public.toezichtgroepen

CREATE TABLE IF NOT EXISTS public.toezichtgroepen
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    type_groep text COLLATE pg_catalog."default" NOT NULL,
    omschrijving text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT toezichtgroepen_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'toezichtgroep'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT toezichtgroepen_assets_fkey
            FOREIGN KEY (toezichtgroep)
            REFERENCES public.toezichtgroepen (uuid) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN
    NULL;
END $$;


-- Table: public.identiteiten

CREATE TABLE IF NOT EXISTS public.identiteiten
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    voornaam text COLLATE pg_catalog."default" NOT NULL,
    gebruikersnaam text COLLATE pg_catalog."default" NOT NULL,
    systeem boolean NOT NULL,
    type_identiteit text COLLATE pg_catalog."default" NOT NULL,
    vo_id text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT identiteiten_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assets' AND column_name = 'toezichter'
    ) THEN
        ALTER TABLE public.assets
            ADD CONSTRAINT identiteiten_assets_fkey
            FOREIGN KEY (toezichter)
            REFERENCES public.identiteiten (uuid) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
    END IF;
EXCEPTION WHEN duplicate_object OR undefined_column OR undefined_table THEN
    NULL;
END $$;


-- Table: public.relatietypes

CREATE TABLE IF NOT EXISTS public.relatietypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    label text COLLATE pg_catalog."default" NOT NULL,
    definitie text COLLATE pg_catalog."default" NOT NULL,
    uri text COLLATE pg_catalog."default" NOT NULL,
    short_uri text COLLATE pg_catalog."default" NOT NULL,
    actief boolean NOT NULL,
    gericht boolean NOT NULL,
    CONSTRAINT relatietypes_pkey PRIMARY KEY (uuid)
);


-- Table: public.assetrelaties
-- n-n directed relations between assets, derived from Arango edge collection `assetrelaties`.
-- `bron_asset_uuid`/`doel_asset_uuid` come from edge `_from`/`_to` (assets/<uuid>).
-- `relatietype_uuid` resolved from Arango `relatietype_key` (preferred) or type URI.
-- Redundant `RelatieObject_bronAssetId`/`RelatieObject_doelAssetId` are excluded;
-- `RelatieObject_assetId` is flattened; remaining unmapped fields go to `attributes`.

CREATE TABLE IF NOT EXISTS public.assetrelaties
(
    uuid uuid NOT NULL,
    bron_asset_uuid uuid NOT NULL,
    doel_asset_uuid uuid NOT NULL,
    relatietype_uuid uuid NOT NULL,
    bron_type text,
    doel_type text,
    type_uri text,
    asset_id_identificator text,
    asset_id_toegekend_door text,
    actief boolean,
    attributes jsonb,
    CONSTRAINT assetrelaties_pkey PRIMARY KEY (uuid)
);

-- For DBs where the table existed before relatietype_uuid became mandatory.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'assetrelaties'
          AND column_name = 'relatietype_uuid' AND is_nullable = 'YES'
    ) AND NOT EXISTS (
        SELECT 1 FROM public.assetrelaties WHERE relatietype_uuid IS NULL
    ) THEN
        ALTER TABLE public.assetrelaties
            ALTER COLUMN relatietype_uuid SET NOT NULL;
    END IF;
EXCEPTION WHEN undefined_table OR undefined_column THEN
    NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_assetrelaties_bron ON public.assetrelaties (bron_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_doel ON public.assetrelaties (doel_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_assetrelaties_relatietype ON public.assetrelaties (relatietype_uuid);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assetrelaties
        ADD CONSTRAINT assetrelaties_bron_fkey
        FOREIGN KEY (bron_asset_uuid)
        REFERENCES public.assets (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
        NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN
    NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assetrelaties
        ADD CONSTRAINT assetrelaties_doel_fkey
        FOREIGN KEY (doel_asset_uuid)
        REFERENCES public.assets (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
        NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN
    NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assetrelaties
        ADD CONSTRAINT assetrelaties_relatietype_fkey
        FOREIGN KEY (relatietype_uuid)
        REFERENCES public.relatietypes (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN
    NULL;
END $$;
-- Table: public.betrokkenerelaties
-- Edge collection; source may be asset or agent; target always agent.

CREATE TABLE IF NOT EXISTS public.betrokkenerelaties
(
    uuid uuid NOT NULL,
    bron_asset_uuid uuid,
    bron_agent_uuid uuid,
    doel_agent_uuid uuid NOT NULL,
    relatietype_uuid uuid NOT NULL,
    rol text,
    bron_type text,
    doel_type text,
    type_uri text,
    asset_id_identificator text,
    asset_id_toegekend_door text,
    actief boolean,
    attributes jsonb,
    CONSTRAINT betrokkenerelaties_pkey PRIMARY KEY (uuid)
);

CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_bron_asset ON public.betrokkenerelaties (bron_asset_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_bron_agent ON public.betrokkenerelaties (bron_agent_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_doel_agent ON public.betrokkenerelaties (doel_agent_uuid);
CREATE INDEX IF NOT EXISTS idx_betrokkenerelaties_relatietype ON public.betrokkenerelaties (relatietype_uuid);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_bron_asset_fkey
        FOREIGN KEY (bron_asset_uuid) REFERENCES public.assets (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_bron_agent_fkey
        FOREIGN KEY (bron_agent_uuid) REFERENCES public.agents (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_doel_agent_fkey
        FOREIGN KEY (doel_agent_uuid) REFERENCES public.agents (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE CASCADE NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.betrokkenerelaties
        ADD CONSTRAINT betrokkenerelaties_relatietype_fkey
        FOREIGN KEY (relatietype_uuid) REFERENCES public.relatietypes (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
EXCEPTION WHEN duplicate_object OR undefined_table THEN NULL;
END $$;
