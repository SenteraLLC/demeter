create schema raster;
set schema 'raster';
COMMENT ON SCHEMA raster IS 'Demeter Raster Schema v0.0.0';

-- TODO: Move user creation to a different SQL file
-- Once we have knowledge of all of the schemas that are going to go into the demeter database and the users we will need to create, then we...
-- (1) Create all of the users with passwords
-- (2) Initialize each individual schema and, in the same SQL file, grant access to the appropriate users.
-- Essentially, this separates out the CREATE USER statements from the CREATE SCHEMA statements.

-- create extension if not exists postgis with schema public;
-- create extension if not exists postgis_raster with schema public;


set search_path = raster, public;

-- TABLE: 'raster_simple'
create table raster_simple (
    raster_id bigserial primary key,
    file_sentera_id text not null,
    sentera_id_ancestry jsonb
                  default '{}'::jsonb
                  not null,
    captured_at timestamp without time zone
                not null, 
    geom geometry(Geometry, 4326) not null,
        check (ST_IsValid(geom)),
    sentera_mosaic_name text not null,
    sentera_mosaic_type text not null
);

-- TABLE: 'collections'
create table collections (
    collection_id bigint primary key,
    sentera_id text not null,
    capture_start timestamp without time zone
                not null, 
    capture_end timestamp without time zone
                not null, 
    s3_path text not null,
    sensor_make text,
    sensor_model text,
    hardware_other jsonb
                  default '{}'::jsonb,
    created timestamp without time zone,
    uploaded timestamp with time zone
);

-- TABLE: 'raster_meta'

CREATE TYPE rast_type AS ENUM ('RAW','PRIMARY','DERIVED','UNKNOWN');
CREATE TYPE rast_subtype AS ENUM ('RGB','MULTISPECTRAL','THERMAL','HYPERSPECTRAL','DEM','SENTERA');

create table raster_meta (
    rid bigserial primary key,
    raster_type rast_type not null,
    raster_subtype rast_subtype not null,
    collection_id bigint 
            references collections(collection_id)
            not null,
    sentera_id text not null,
    s3_path text not null,
    profile jsonb
            default '{}'::jsonb,
    tags_exif jsonb
                  default '{}'::jsonb,
    tags_xml jsonb
                  default '{}'::jsonb,
    tags_tifftag jsonb
                  default '{}'::jsonb,
    tags_derived jsonb
                  default '{}'::jsonb,
    tags_sentera jsonb
                  default '{}'::jsonb,
    workflow jsonb
                  default '{}'::jsonb,
    geom geometry(Geometry, 4326) not null
);

-- give `demeter_user` read and write access to `raster`
grant select, insert on all tables in schema raster to demeter_user;
grant usage, select on all sequences in schema raster to demeter_user;
alter default privileges in schema raster grant usage on sequences to demeter_user;
grant usage on schema raster to demeter_user;

-- create read and write user access
create user raster_user with password 'raster_user_password';
grant select, insert on all tables in schema raster to raster_user;
grant usage, select on all sequences in schema raster to raster_user;
alter default privileges in schema raster grant usage on sequences to raster_user;
grant usage on schema raster to raster_user;
alter role raster_user set search_path = raster,public;

-- create read only access user
create user raster_ro_user with password 'raster_ro_user_password';
grant select on all tables in schema raster to raster_ro_user;
grant select on all sequences in schema raster to raster_ro_user;
grant usage on schema raster to raster_ro_user;
alter role raster_ro_user set search_path = raster,public;