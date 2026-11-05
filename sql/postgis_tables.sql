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
VALUES ('fresh_start', TRUE);


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

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT assets_assettypes_fkey
    FOREIGN KEY (assettype)
    REFERENCES public.assettypes (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
