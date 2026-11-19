-- Table definitions copied from AWVInfraPostGISSyncer/setup_tables_querys.sql
-- (same level as this repo)
--
-- Changed: 2026-11-09 — relation views now filter on relatietype_uuid (exact,
-- index-backed) instead of type_uri ILIKE; added assets_geometry, heeftkeuring,
-- gemigreerdnaar views; switched materialized -> regular views.
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
    agent_identificator text,
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

-- Indexes and foreign-key constraints for assets are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.


-- Table: public.geometrie
-- Canonical spatial data derived from Arango asset top-level `wkt` (SRID=3812 / Lambert 2008).
-- WKT is the lead; no fallback to Arango `geometry` (GeoJSON) or `geo.Geometrie_log`.

CREATE TABLE IF NOT EXISTS public.geometrie
(
    asset_uuid uuid NOT NULL,
    wkt text NOT NULL,
    geom geometry(GeometryZ, 3812) NOT NULL,
    CONSTRAINT geometrie_pkey PRIMARY KEY (asset_uuid)
);

-- Indexes and foreign keys for geometrie are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.


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
    CONSTRAINT bestekkoppelingen_pkey PRIMARY KEY (asset_uuid, volgnummer)
);

-- Indexes and foreign keys for bestekkoppelingen are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.


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

-- Indexes and foreign keys referencing assets from beheerders are managed in
-- arangodbtopostgis.setup_postgis_tables (FK_CONSTRAINTS) so they can be
-- dropped before bulk loads and restored afterward.


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

-- Indexes and foreign keys referencing assets from toezichtgroepen are managed in
-- arangodbtopostgis.setup_postgis_tables (FK_CONSTRAINTS) so they can be
-- dropped before bulk loads and restored afterward.


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

-- Indexes and foreign keys referencing assets from identiteiten are managed in
-- arangodbtopostgis.setup_postgis_tables (FK_CONSTRAINTS) so they can be
-- dropped before bulk loads and restored afterward.


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
    relatietype_uuid uuid,
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

-- Indexes and foreign keys for assetrelaties are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.


-- Table: public.betrokkenerelaties
-- Edge collection; source may be asset or agent; target always agent.

CREATE TABLE IF NOT EXISTS public.betrokkenerelaties
(
    uuid uuid NOT NULL,
    bron_asset_uuid uuid,
    bron_agent_uuid uuid,
    doel_agent_uuid uuid NOT NULL,
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

-- Indexes and foreign keys for betrokkenerelaties are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.


-- View: aantallen (row counts across all managed tables)
CREATE OR REPLACE VIEW public.aantallen AS
SELECT 'agents' AS tabel, count(*) AS aantal FROM public.agents
UNION ALL SELECT 'assettypes', count(*) FROM public.assettypes
UNION ALL SELECT 'assets', count(*) FROM public.assets
UNION ALL SELECT 'geometrie', count(*) FROM public.geometrie
UNION ALL SELECT 'bestekken', count(*) FROM public.bestekken
UNION ALL SELECT 'bestekkoppelingen', count(*) FROM public.bestekkoppelingen
UNION ALL SELECT 'beheerders', count(*) FROM public.beheerders
UNION ALL SELECT 'toezichtgroepen', count(*) FROM public.toezichtgroepen
UNION ALL SELECT 'identiteiten', count(*) FROM public.identiteiten
UNION ALL SELECT 'relatietypes', count(*) FROM public.relatietypes
UNION ALL SELECT 'assetrelaties', count(*) FROM public.assetrelaties
UNION ALL SELECT 'betrokkenerelaties', count(*) FROM public.betrokkenerelaties;

-- View: assets_geometry (geometry per asset, preferring computed geom over WKT)
CREATE OR REPLACE VIEW public.assets_geometry AS
SELECT assets.uuid,
       COALESCE(g.geom, ST_SetSRID(ST_GeomFromEWKT(assets.wkt), 3812)) AS asset_geometry,
       COALESCE(g.wkt, assets.wkt) AS asset_wkt_string
FROM public.assets
LEFT JOIN public.geometrie g ON g.asset_uuid = assets.uuid;

CREATE OR REPLACE VIEW public.bevestiging_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = '3ff9bf1c-d852-442e-a044-6200fe064b20';
CREATE OR REPLACE VIEW public.sturing_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = '93c88f93-6e8c-4af3-a723-7e7a6d6956ac';
CREATE OR REPLACE VIEW public.hoortbij_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = '812dd4f3-c34e-43d1-88f1-3bcd0b1e89c2';
CREATE OR REPLACE VIEW public.voedt_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = 'f2c5c4a1-0899-4053-b3b3-2d662c717b44';
CREATE OR REPLACE VIEW public.ligtop_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = '321c18b8-92ca-4188-a28a-f00cdfaa0e31';
CREATE OR REPLACE VIEW public.heeftkeuring_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = '8677a92e-7021-4e39-a66a-251891b42bfd';
CREATE OR REPLACE VIEW public.gemigreerdnaar_relaties AS
SELECT * FROM public.assetrelaties WHERE relatietype_uuid = 'f0ed1efa-fe29-4861-89dc-5d3bc40f0894';

-- All secondary indexes and foreign-key constraints are managed in
-- arangodbtopostgis.setup_postgis_tables (SECONDARY_INDEXES / FK_CONSTRAINTS)
-- so they can be dropped before bulk loads and restored afterward.
