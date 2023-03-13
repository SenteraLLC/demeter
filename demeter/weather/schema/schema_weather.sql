create schema weather;
set schema 'weather';
COMMENT ON SCHEMA weather IS 'Demeter Weather Schema v0.0.0';

-- TODO: Move user creation to a different SQL file
-- Once we have knowledge of all of the schemas that are going to go into the demeter database and the users we will need to create, then we...
-- (1) Create all of the users with passwords
-- (2) Initialize each individual schema and, in the same SQL file, grant access to the appropriate users.
-- Essentially, this separates out the CREATE USER statements from the CREATE SCHEMA statements.

-- create extension if not exists postgis with schema public;
-- create extension if not exists postgis_raster with schema public;


set search_path = weather, public;

-- TABLE: 'world_utm'
-- utm_row doesn't include "I" or "O" rows
CREATE TYPE utm_row AS ENUM ('A','B','C','D','E','F','G','H','J','K','L','M','N','P','Q','R','S','T','U','V','W','X','Y','Z');

create table world_utm (
    world_utm_id smallserial primary key,
    zone smallint not null,
    row utm_row not null,
    unique (zone, row),
    geom geometry(Geometry, 4326) not null,
    check (ST_IsValid(geom)),
    utc_offset time with time zone not null,
    raster_epsg smallint not null
);

-- CREATE INDEX CONCURRENTLY world_utm_idx on world_utm using SPGIST(geom);

-- TABLE: 'raster_5km'
create table raster_5km (
    raster_5km_id smallserial primary key,
    world_utm_id smallint
                 references world_utm(world_utm_id),
    unique(world_utm_id),
    rast_cell_id raster,
    rast_metadata jsonb
                  default '{}'::jsonb
);

-- create ENUM for Meteomatics parameter
CREATE TYPE weather_parameter AS ENUM (
    PARAMETER_LIST
);

-- TABLE: 'weather_type'
create table weather_type (
    weather_type_id smallserial primary key,
    weather_type weather_parameter not null,
    unique (weather_type),
    temporal_extent interval not null,
    units text,
    description text not null
);

-- TABLE: 'daily'
create table daily (
    daily_id bigserial primary key,
    world_utm_id smallint
                 not null
                 references world_utm(world_utm_id),
    cell_id integer not null,
    date date not null,
    weather_type_id smallint not null references weather_type(weather_type_id),
    value real,
    date_requested timestamp without time zone
                not null
);

-- TABLE: 'request_log'
CREATE TYPE request_status AS ENUM (
    'SUCCESS', 'FAIL'
);

create table request_log (
    request_id serial primary key,
    zone smallint not null,
    utm_request_id smallint not null,
    n_pts_requested int not null,
    startdate timestamp without time zone
                not null,
    enddate timestamp without time zone
                not null,
    parameters text,
    date_requested timestamp without time zone
                not null,
    status request_status not null,
    request_seconds real not null
);

-- give `demeter_user` read and write access to `weather`
grant select, insert on all tables in schema weather to demeter_user;
grant usage, select on all sequences in schema weather to demeter_user;
alter default privileges in schema weather grant usage on sequences to demeter_user;
grant usage on schema weather to demeter_user;

-- create read and write user access
create user weather_user with password 'weather_user_password';
grant select, insert on all tables in schema weather to weather_user;
grant usage, select on all sequences in schema weather to weather_user;
alter default privileges in schema weather grant usage on sequences to weather_user;
grant usage on schema weather to weather_user;
alter role weather_user set search_path = weather,public;

-- create read only access user
create user weather_ro_user with password 'weather_ro_user_password';
grant select on all tables in schema weather to weather_ro_user;
grant select on all sequences in schema weather to weather_ro_user;
grant usage on schema weather to weather_ro_user;
alter role weather_ro_user set search_path = weather,public;