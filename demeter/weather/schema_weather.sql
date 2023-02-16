create schema weather;
set schema 'weather';

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

-- TABLE: 'weather_type'
create table weather_type (
    weather_type_id serial primary key,
    weather_type text not null,
    temporal_extent interval not null,
    units text,
    description text not null
);

-- TABLE: 'daily'
create table daily (
    world_utm_id smallint
                 not null
                 references world_utm(world_utm_id),
    cell_id integer not null,
    date date not null,
    weather_type_id bigint not null references weather_type(weather_type_id),
    value float not null,
    date_extracted timestamp without time zone
                not null
);

-- create read and write user access
create user weather_user with password 'weather_user_password';
grant select, insert on all tables in schema weather to weather_user;
grant usage, select on all sequences in schema weather to weather_user;
alter default privileges in schema weather grant usage on sequences to weather_user;
grant usage on schema weather to weather_user;

-- create read only access user
create user weather_ro_user with password 'weather_ro_user_password';
grant select on all tables in schema weather to weather_ro_user;
grant select on all sequences in schema weather to weather_ro_user;
grant usage on schema weather to weather_ro_user;