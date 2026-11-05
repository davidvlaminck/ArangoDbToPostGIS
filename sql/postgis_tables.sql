-- Table definitions copied from AWVInfraPostGISSyncer/setup_tables_querys.sql
-- (same level as this repo)

-- Table: public.params

CREATE TABLE IF NOT EXISTS public.params
(
    key_name VARCHAR(40),
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

CREATE TABLE IF NOT EXISTS public.agents
(
    uuid uuid NOT NULL,
    naam text,
    void uuid,
    actief boolean NOT NULL,
    contact_info json,
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
    definitie text COLLATE pg_catalog."default",
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
    naam text COLLATE pg_catalog."default",
    referentie text COLLATE pg_catalog."default",
    typeBeheerder text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT beheerders_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assets
        ADD CONSTRAINT beheerders_assets_fkey
        FOREIGN KEY (schadebeheerder)
        REFERENCES public.beheerders (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN
    -- constraint already exists, skip
END $$;


-- Table: public.toezichtgroepen

CREATE TABLE IF NOT EXISTS public.toezichtgroepen
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    typeGroep text COLLATE pg_catalog."default",
    omschrijving text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT toezichtgroepen_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assets
        ADD CONSTRAINT toezichtgroepen_assets_fkey
        FOREIGN KEY (toezichtgroep)
        REFERENCES public.toezichtgroepen (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN
    -- constraint already exists, skip
END $$;


-- Table: public.identiteiten

CREATE TABLE IF NOT EXISTS public.identiteiten
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    voornaam text COLLATE pg_catalog."default",
    gebruikersnaam text COLLATE pg_catalog."default",
    systeem boolean NOT NULL,
    typeIdentiteit text COLLATE pg_catalog."default",
    voId text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT identiteiten_pkey PRIMARY KEY (uuid)
);

DO $$
BEGIN
    ALTER TABLE IF EXISTS public.assets
        ADD CONSTRAINT identiteiten_assets_fkey
        FOREIGN KEY (toezichter)
        REFERENCES public.identiteiten (uuid) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN
    -- constraint already exists, skip
END $$;


-- Table: public.relatietypes

CREATE TABLE IF NOT EXISTS public.relatietypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    label text COLLATE pg_catalog."default",
    definitie text COLLATE pg_catalog."default",
    uri text COLLATE pg_catalog."default",
    short_uri text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    gericht boolean NOT NULL,
    CONSTRAINT relatietypes_pkey PRIMARY KEY (uuid)
);
