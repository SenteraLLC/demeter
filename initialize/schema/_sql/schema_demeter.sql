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


-- ORGANIZATION

create table organization (
  organization_id bigserial primary key,

  name  text
        not null,

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

CREATE TRIGGER update_organization_last_updated BEFORE UPDATE
ON organization FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


-- GROUPER

create table grouper (
  grouper_id bigserial primary key,
  -- TODO: Add cycle detection constraint

  name  text
        not null,

  organization_id bigint
                  not null
                  references organization(organization_id),

  parent_grouper_id bigint
                    references grouper(grouper_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  UNIQUE NULLS NOT DISTINCT (name, organization_id, parent_grouper_id)
);

-- CREATE UNIQUE INDEX unique_grouper_null_parent_grouper_id
--   ON grouper (name)
--   WHERE parent_grouper_id is null;

CREATE TRIGGER update_grouper_last_updated BEFORE UPDATE
ON grouper FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- FIELD
create table field (
  field_id bigserial
            primary key,

  name  text
        not null,

  date_start  timestamp without time zone
              not null,

  date_end  timestamp without time zone
            not null
            default ('infinity'::timestamp at time zone 'utc'),

  geom_id bigint
          not null
          references geom(geom_id),

  organization_id bigint
                  not null
                  references organization(organization_id),

  grouper_id  bigint
              references grouper(grouper_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created timestamp without time zone
          not null
          default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  -- Same organization-level geometry and date range cannot have same field name
  -- Cannot add another field if the only things to change are grouper_id and/or details
  UNIQUE NULLS NOT DISTINCT (name, date_start, date_end, geom_id, organization_id)
);

-- CREATE UNIQUE INDEX unique_field_null_grouper_id
--   ON field (name)
--   WHERE grouper_id is null;

CREATE TRIGGER update_field_last_updated BEFORE UPDATE
ON field FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- FIELD TRIAL

create table field_trial (
  field_trial_id bigserial
                  primary key,

  -- Either geom_id or name must be non-null, not both.
  name  text
        not null,

  field_id bigint
            not null
            references field(field_id),

  date_start  timestamp without time zone
              not null,

  date_end    timestamp without time zone
              not null
              default ('infinity'::timestamp at time zone 'utc'),

  geom_id   bigint
            references geom(geom_id),

  grouper_id  bigint
              references grouper(grouper_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created timestamp without time zone
          not null
          default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),
  -- Same field and date range cannot have same name, regardless of geom_id
  UNIQUE (name, field_id, date_start, date_end)
);

CREATE TRIGGER update_field_trial_last_updated BEFORE UPDATE
ON field_trial FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- PLOT

create table plot (
  plot_id bigserial
          primary key,

  name  text
        not null,

  field_id  bigint
            not null
            references field(field_id),

  field_trial_id  bigint
                  not null
                  references field_trial(field_trial_id),

  geom_id   bigint
            references geom(geom_id),

  treatment_id smallint,

  block_id smallint,  -- a subset of plots; blocks are used to reduce unexplained variability.

  replication_id smallint,

  details jsonb
          not null
          default '{}'::jsonb,

  created timestamp without time zone
          not null
          default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),
  -- Same field and field_trial cannot have same name (name must be unique within a field_trial)
  UNIQUE (name, field_id, field_trial_id)
);

CREATE TRIGGER update_plot_last_updated BEFORE UPDATE
ON plot FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- CROP TYPE

create table crop_type (
  crop_type_id bigserial
               primary key,

  crop  text
        not null,

  product_name text,

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),
  -- Product name must be unique within a crop
  -- Cannot add another crop_type if the only thing to change is details
  UNIQUE NULLS NOT DISTINCT (crop, product_name)
);

-- CREATE UNIQUE INDEX crop_product_name_null_unique_idx
--   ON crop_type(crop)
--   WHERE (product_name is NULL);

CREATE TRIGGER update_crop_type_last_updated BEFORE UPDATE
ON crop_type FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_crop_uppercase_ck
  CHECK (crop = upper(crop));
ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_product_name_uppercase_ck
  CHECK (product_name = upper(product_name));

-- OBSERVATION TYPE
CREATE TYPE cateogry_type_enum AS ENUM (NULL, 'REMOTE_SENSING', 'SOIL', 'TISSUE', 'GRAIN', 'STOVER', 'WEATHER', 'SENSOR');

create table observation_type (
  observation_type_id bigserial
                      primary key,

  observation_type_name text
                        not null,

  category  cateogry_type_enum
            default 'NULL'::cateogry_type_enum,

  details jsonb
          not null
          default '{}'::jsonb,

  created timestamp without time zone
          not null
          default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  -- Cannot add another observation_type if the only thing to change is details
  UNIQUE NULLS NOT DISTINCT (observation_type_name, category)
);

-- -- All are NULL
-- -- TODO: Are these necesary if GetOrInsertObservationType() is already checking for this?
-- CREATE UNIQUE INDEX observation_type_all_null_unique_idx
--   ON observation_type(observation_type_name)
--   WHERE (category is NULL);

CREATE TRIGGER update_observation_last_updated BEFORE UPDATE
ON observation_type FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

ALTER TABLE observation_type
	ADD CONSTRAINT observation_type_observation_type_name_uppercase_ck CHECK (observation_type_name = upper(observation_type_name));
  -- ADD CONSTRAINT observation_type_category_uppercase_ck CHECK (category = upper(category));

-- UNIT TYPE

create table unit_type (
  unit_type_id  bigserial
                primary key,

  unit_name text
            not null,

  observation_type_id bigint
                      not null
                      references observation_type(observation_type_id),

  -- Cannot add another unit_type if unit_name/observation_type_id combination are not unique
  UNIQUE NULLS NOT DISTINCT (unit_name, observation_type_id)
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

CREATE TYPE act_type_enum AS ENUM ('APPLY', 'HARVEST', 'MECHANICAL', 'PLANT', 'TILL');

create table act (
  act_id         bigserial primary key,

  act_type  act_type_enum
            not null,

  date_performed  timestamp without time zone
                  not null,

  crop_type_id  bigint
                references crop_type(crop_type_id),

  field_id  bigint
            references field(field_id),

  field_trial_id  bigint
                  references field_trial(field_trial_id),

  plot_id bigint
          references plot(plot_id),

  geom_id bigint
          references geom(geom_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  -- Cannot add an act if the only thing to change is details (should edit existing act instead)
  UNIQUE NULLS NOT DISTINCT (act_type, date_performed, crop_type_id, field_id, field_trial_id, plot_id, geom_id)
);

ALTER TABLE act
  ADD CONSTRAINT act_nullable_ids_at_least_one_not_null_ck
  CHECK (num_nonnulls(field_id, field_trial_id, plot_id) >= 1);

CREATE TRIGGER update_act_last_updated BEFORE UPDATE
ON act FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- TODO: Rethink the 1e-7 tol if we ever store geoms at variable precision (i.e., expose tol as arg to insertOrGetGeom())
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
        WHERE Q2.geom_id = NEW.geom_id AND ST_COVERS(ST_BUFFER(Q1.geom, 1e-6), Q2.geom)
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

  value_observed float
                 not null,

  date_observed timestamp without time zone,

  observation_type_id bigint
                      not null
                      references observation_type(observation_type_id),

  unit_type_id  bigint
                references unit_type(unit_type_id)
                not null,

  field_id  bigint
            references field(field_id),

  field_trial_id  bigint
                  references field_trial(field_trial_id),

  plot_id bigint
          references plot(plot_id),

  geom_id bigint
          references geom(geom_id),

  act_id  bigint
          references act(act_id),

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  -- Cannot have duplicate observations across date, observation_type, and field/field_trial/plot/geom/act
  -- Unit type doesn't matter (don't store duplicate observations if only the unit changes)
  UNIQUE NULLS NOT DISTINCT (value_observed, date_observed, observation_type_id, field_id, field_trial_id, plot_id, geom_id, act_id)

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

-- adjust permissions for `demeter_user`
grant select, insert on all tables in schema test_demeter to demeter_user;
grant usage, select on all sequences in schema test_demeter to demeter_user;
alter default privileges in schema test_demeter grant usage on sequences to demeter_user;
grant usage on schema test_demeter to demeter_user;

-- adjust permissions for `demeter_ro_user`
grant select on all tables in schema test_demeter to demeter_ro_user;
grant select on all sequences in schema test_demeter to demeter_ro_user;
grant usage on schema test_demeter to demeter_ro_user;
