-- TODO: Cascading behaviors
-- TODO: Consider alternatives to constraint triggers
--       They aren't technically 100% serializable
--       Need to look into the importance of serializability
-- TODO: Identity columns instead of serials

-- Database Setup
-- drop schema if exists demeter cascade;
create schema test_demeter;
set schema 'test_demeter';
COMMENT ON SCHEMA demeter IS 'Demeter Schema v0.0.0';

create extension if not exists postgis with schema public;
create extension if not exists postgis_raster with schema public;
-- TODO: Fix this extension
-- create extension "postgres-json-schema" with schema public;

set search_path = test_demeter, public;

CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.last_updated = now();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Geometry Tables
-- TODO: Enforce SRID like this:
--  ALTER xxxx ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 28355)
-- TODO: Table for geometries that get 'repaired' with their 'IsValidMessage' and 'IsValidDetails'

create table geom (
  geom_id bigserial primary key,
  geom geometry(Geometry, 4326) not null,
  check (ST_IsValid(geom))
);

CREATE INDEX CONCURRENTLY geom_idx on geom using SPGIST(geom);

CREATE TRIGGER update_geom_last_updated BEFORE UPDATE
ON geom FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

create function geom_must_be_unique() RETURNS trigger
  LANGUAGE plpgsql AS
$$BEGIN
  IF (
    select exists(
        select ST_Equals(OLD.geom, geom.geom) from geom
        where OLD.geom_id <> geom.geom_id
      )
     )
  THEN
    RAISE EXCEPTION 'A geometry must be unique';
  END IF;

  RETURN old;
END;$$;

create constraint trigger geom_must_be_unique
       after insert or update on geom
       deferrable initially deferred
       for each row execute procedure geom_must_be_unique();


-- FIELD GROUP

create table field_group (
  field_group_id bigserial primary key,
  -- TODO: Add cycle detection constraint

  name text
        not null,

  parent_field_group_id bigint
                        references field_group(field_group_id),

  unique (field_group_id, parent_field_group_id),
  unique (parent_field_group_id, name),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc')
);

CREATE UNIQUE INDEX unique_name_for_null_roots_idx on field_group (name) where parent_field_group_id is null;

-- FIELD

create table field (
  field_id bigserial
           primary key,

  name text,

  geom_id   bigint
            not null
            references geom(geom_id),

  date_start      timestamp without time zone
                  not null,

  date_end        timestamp without time zone
                  not null
                  default ('infinity'::timestamp at time zone 'utc'),

  field_group_id bigint
                  references field_group(field_group_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc')
);

-- CROP TYPE

create table crop_type (
  crop_type_id bigserial
               primary key,

  crop        text
               not null,

  product_name text,

  unique (crop, product_name),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc')
);

CREATE UNIQUE INDEX crop_product_name_null_unique_idx
  ON crop_type(crop)
  WHERE (product_name is NULL);

CREATE TRIGGER update_crop_type_last_updated BEFORE UPDATE
ON crop_type FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_crop_lowercase_ck
  CHECK (crop = lower(crop));
ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_product_name_lowercase_ck
  CHECK (product_name = lower(product_name));

-- OBSERVATION TYPE

create table observation_type (
  observation_type_id bigserial primary key,
  type_name     text not null,
  type_category text,
  unique (type_name, type_category)
);

CREATE UNIQUE INDEX observation_type_category_null_unique_idx
  ON observation_type(type_name)
  WHERE (type_category is NULL);

ALTER TABLE observation_type
  ADD CONSTRAINT observation_type_lowercase_ck
  CHECK (type_name = lower(type_name));


-- UNIT TYPE

create table unit_type (
  unit_type_id   bigserial
                 primary key,
  unit_name     text
                 not null,
  observation_type_id  bigint
                 not null
                 references observation_type(observation_type_id),
  unique(unit_name, observation_type_id)
);
ALTER TABLE unit_type
  ADD CONSTRAINT unit_type_start_end_w_alphanumeric_ck
  CHECK (unit_name ~ '^[A-Za-z](.*[A-Za-z0-9])?$');

---------------------
-- Value Tables --
---------------------

-- -- I'm not sure we need these?
-- create table geospatial_key (
--   geospatial_key_id bigserial
--                     primary key,
--   geom_id    bigint
--              not null
--              references geom(geom_id),
--   field_id   bigint
--              references field(field_id)
-- );


-- -- I'm not sure we need these?
-- create table temporal_key (
--   temporal_key_id bigserial
--                   primary key,
--   date_start      date
--                   not null,
--   date_end        date
--                   not null
--                   -- default ('infinity'::timestamp at time zone 'utc')
-- );

-- ACT

CREATE TYPE act_type_enum AS ENUM ('fertilize', 'harvest', 'irrigate', 'plant');

create table act (
  act_id         bigserial primary key,

  act_type       act_type_enum not null,

  field_id       bigint
                  not null
                  references field(field_id),

  date_performed timestamp without time zone
                  not null,

  crop_type_id   bigint
                  references crop_type(crop_type_id),

  geom_id       bigint
                  references geom(geom_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc')
);

CREATE TRIGGER update_act_last_updated BEFORE UPDATE
ON act FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

create function field_geom_covers_act_geom() RETURNS trigger
  LANGUAGE plpgsql as
$$BEGIN
  IF (NEW.geom_id IS NOT NULL)
  THEN
    IF (
      SELECT not exists(
        SELECT Q1.geom, Q2.geom
        FROM (SELECT * FROM field F INNER JOIN geom GEO on F.geom_id = GEO.geom_id WHERE F.field_id = NEW.field_id) Q1
        CROSS JOIN geom Q2
        WHERE Q2.geom_id = NEW.geom_id AND ST_COVERS(Q1.geom, Q2.geom)
      )
    )
    THEN
      RAISE EXCEPTION 'Field geometry must cover act geometry.';
    END IF;
  END IF;

  RETURN NEW;
END;$$;

create constraint trigger field_geom_covers_act_geom
  after insert or update on act
  for each row execute procedure field_geom_covers_act_geom();

-- OBSERVATION

create table observation (
  observation_id bigserial
                 primary key,

  field_id      bigint
                 not null
                 references field(field_id),
  unit_type_id   bigint
                 references unit_type(unit_type_id)
                 not null,
  observation_type_id bigint
                not null
                references observation_type(observation_type_id),
  value_observed float
                 not null,
  date_observed timestamp without time zone,
  geom_id        bigint
                 references geom(geom_id),

  act_id         bigint
                  references act(act_id),
  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc')
);


CREATE TRIGGER update_observation_last_updated BEFORE UPDATE
ON observation FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


create function observation_types_match() RETURNS trigger
  LANGUAGE plpgsql as
$$BEGIN
  IF (
    SELECT not exists(
      SELECT U.observation_type_id FROM unit_type U
      WHERE U.unit_type_id = NEW.unit_type_id and U.observation_type_id = NEW.observation_type_id
    )
  )
  THEN
    RAISE EXCEPTION 'Unit type observation type does not match given observation type.';
  END IF;

  RETURN NEW;
END;$$;

create constraint trigger observation_types_match
  after insert or update on observation
  for each row execute procedure observation_types_match();

-- MUST GRANT USERS ACCESS ONCE THE TABLES ARE ALREADY CREATED

-- create read and write user access
create user demeter_user with password 'demeter_user_password';
grant select, insert on all tables in schema test_demeter to demeter_user;
grant usage, select on all sequences in schema test_demeter to demeter_user;
alter default privileges in schema test_demeter grant usage on sequences to demeter_user;
grant usage on schema test_demeter to demeter_user;

-- create read only access user
create user demeter_ro_user with password 'demeter_ro_user_password';
grant select on all tables in schema test_demeter to demeter_ro_user;
grant select on all sequences in schema test_demeter to demeter_ro_user;
grant usage on schema test_demeter to demeter_ro_user;
