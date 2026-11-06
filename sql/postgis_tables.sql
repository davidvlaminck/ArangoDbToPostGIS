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
    "isActief" boolean,
    "assetId_identificator" text,
    "assetId_toegekendDoor" text,
    "typeURI" text,
    toestand text,
    notitie text,
    naam text,
    naampad text,
    naampad_parts jsonb,
    naampad_parent text,
    "assetVersie_context" text,
    "assetVersie_timestamp" timestamptz,
    "assetVersie_versienummer" text,
    "datumOprichtingObject" date,
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
CREATE INDEX IF NOT EXISTS idx_assets_is_actief ON public.assets ("isActief");

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


-- Table: public.bestekken

CREATE TABLE IF NOT EXISTS public.bestekken
(
    uuid uuid NOT NULL,
    eDeltaDossiernummer text COLLATE pg_catalog."default" NOT NULL,
    eDeltaBesteknummer text COLLATE pg_catalog."default" NOT NULL,
    awvId text COLLATE pg_catalog."default",
    aannemerNaam text COLLATE pg_catalog."default",
    aannemerReferentie text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT bestekken_pkey PRIMARY KEY (uuid)
);


-- Table: public.beheerders

CREATE TABLE IF NOT EXISTS public.beheerders
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    referentie text COLLATE pg_catalog."default" NOT NULL,
    typeBeheerder text COLLATE pg_catalog."default" NOT NULL,
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
    typeGroep text COLLATE pg_catalog."default" NOT NULL,
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
    typeIdentiteit text COLLATE pg_catalog."default" NOT NULL,
    voId text COLLATE pg_catalog."default",
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