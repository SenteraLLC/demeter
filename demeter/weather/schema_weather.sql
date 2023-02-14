create schema weather;
set schema 'weather';

create extension if not exists postgis with schema public;
create extension if not exists postgis_raster with schema public;


set search_path = weather, public;

-- TABLE: 'world_utm'
CREATE TYPE utm_row AS ENUM ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z');

create table world_utm (
    world_utm_id smallserial primary key,
    zone smallint not null, 
    row utm_row not null,
    unique (zone, row),
    geom geometry(Geometry, 4326) not null,
    check (ST_IsValid(geom)),
    utc_offset time with time zone not null, 
    raster_espg smallint not null
);

-- CREATE INDEX CONCURRENTLY world_utm_idx on world_utm using SPGIST(geom);

-- TABLE: 'raster_5km'
create table raster_5km (
    world_utm_id smallint
                 not null
                 references world_utm(world_utm_id),
    unique(world_utm_id),
    rast_cell_id raster not null, 
    rast_metadata jsonb
                  not null
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
