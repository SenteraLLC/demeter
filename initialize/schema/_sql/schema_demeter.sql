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
   NEW.last_updated = (now() at time zone 'utc');
   RETURN NEW;
END;
$$ language 'plpgsql';


-- get_geometry_from_id() takes a field geom_id, field_trial geom_id, and plot geom_id, then returns the most specific geometry
CREATE OR REPLACE FUNCTION get_geom_id(f_geom_id bigint, ft_geom_id bigint, p_geom_id bigint)
RETURNS bigint AS
$$
BEGIN
  IF (p_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom_id FROM geom g WHERE g.geom_id = p_geom_id
	);
  ELSIF (ft_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom_id FROM geom g WHERE g.geom_id = ft_geom_id
	);
  ELSIF (f_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom_id FROM geom g WHERE g.geom_id = f_geom_id
	);
  ELSE
    RETURN NULL;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- get_geometry_from_id() takes a field geom_id, field_trial geom_id, and plot geom_id, then returns the most specific geometry
CREATE OR REPLACE FUNCTION get_geometry_from_id(f_geom_id bigint, ft_geom_id bigint, p_geom_id bigint)
RETURNS geometry AS
$$
DECLARE
geom_out geometry;
BEGIN
  IF (p_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom FROM geom g WHERE g.geom_id = p_geom_id
	);
  ELSIF (ft_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom FROM geom g WHERE g.geom_id = ft_geom_id
	);
  ELSIF (f_geom_id IS NOT NULL) THEN
    RETURN (
		SELECT g.geom FROM geom g WHERE g.geom_id = f_geom_id
	);
  ELSE
    RETURN NULL;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- get_field_id() takes a field_id, field_trial_id, and plot_id, and returns the corresponding field_id
CREATE OR REPLACE FUNCTION get_field_id(f_id bigint, ft_id bigint, p_id bigint) RETURNS bigint AS $$
BEGIN
  IF (f_id IS NOT NULL) THEN
    RETURN (
	  SELECT f.field_id FROM field f WHERE f.field_id = f_id
    );
  ELSIF (ft_id IS NOT NULL) THEN
    RETURN (
	  SELECT f.field_id FROM field_trial ft JOIN field f USING(field_id) WHERE ft.field_trial_id = ft_id
	);
  ELSIF (p_id IS NOT NULL) THEN
    RETURN (
	  SELECT f.field_id FROM plot JOIN field_trial USING(field_trial_id) JOIN field f ON f.field_id = field_trial.field_id WHERE plot.plot_id = p_id
	);
  ELSE
    RETURN NULL;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION act_valid_date()
  RETURNS TRIGGER AS $act_valid_date_performed$
BEGIN
  if (NEW.date_performed NOT BETWEEN
	  (SELECT f.date_start FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id))
	  AND (SELECT f.date_end FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)))
  THEN
    RAISE EXCEPTION 'Act date_performed (%) is not valid for Field ID "%". Must be within bounds of date_start (%) and date_end (%) from field table.',
    NEW.date_performed,
	(SELECT f.field_id FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)),
	(SELECT f.date_start FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)),
	(SELECT f.date_end FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id));
  END IF;
  RETURN NEW;
END;
$act_valid_date_performed$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app_valid_date()
  RETURNS TRIGGER AS $app_valid_date_applied$
BEGIN
  if (NEW.date_applied NOT BETWEEN
	  (SELECT f.date_start FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id))
	  AND (SELECT f.date_end FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)))
  THEN
    RAISE EXCEPTION 'App date_applied (%) is not valid for Field ID "%". Must be within bounds of date_start (%) and date_end (%) from field table.',
    NEW.date_applied,
	(SELECT f.field_id FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)),
	(SELECT f.date_start FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id)),
	(SELECT f.date_end FROM field f WHERE f.field_id = get_field_id(NEW.field_id, NEW.field_trial_id, NEW.plot_id));
  END IF;
  RETURN NEW;
END;
$app_valid_date_applied$ LANGUAGE plpgsql;

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
  UNIQUE NULLS NOT DISTINCT (name, field_id, date_start, date_end)
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
-- CREATE TYPE cateogry_type_enum AS ENUM ('REMOTE_SENSING', 'SOIL', 'TISSUE', 'GRAIN', 'STOVER', 'WEATHER', 'SENSOR');

create table observation_type (
  observation_type_id bigserial
                      primary key,

  observation_type_name text
                        not null,

  category  cateogry_type_enum
            default NULL::cateogry_type_enum,

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

-- Force exactly one of field_id, field_trial_id, plot_id to be non-null; more than one is ambiguous.
-- TODO: Alternatively, could look at creating a trigger to post-fill relevant id columns by referencing that table.
ALTER TABLE act
  ADD CONSTRAINT act_nullable_ids_one_and_only_one_not_null_ck
  CHECK (num_nonnulls(field_id, field_trial_id, plot_id) = 1);

ALTER TABLE act
  ADD CONSTRAINT act_type_not_null_ck
  CHECK (act_type IS NOT NULL);

-- Trigger to ensure date_performed is > field.date_start and < field.date_end
CREATE TRIGGER act_valid_date_performed
BEFORE INSERT OR UPDATE ON act
FOR EACH ROW EXECUTE FUNCTION act_valid_date();

CREATE TRIGGER update_act_last_updated BEFORE UPDATE
ON act FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- APPLY
CREATE TYPE app_type_enum AS ENUM ('BIOLOGICAL', 'FERTILIZER', 'FUNGICIDE', 'HERBICIDE', 'INHIBITOR', 'INSECTICIDE', 'IRRIGATION', 'LIME', 'MANURE', 'NEMATICIDE', 'STABILIZER');
CREATE TYPE app_method_enum AS ENUM ('BAND', 'BROADCAST', 'CULTIVATE', 'DRY-DROP', 'FOLIAR', 'KNIFE', 'SEED', 'Y-DROP');

create table app (
  app_id  bigserial primary key,

  app_type  app_type_enum
            not null,

  app_method  app_method_enum,

  date_applied  timestamp without time zone
                not null,

  rate  float
        not null,

  rate_unit text
            not null,

  crop_type_id  bigint
                references crop_type(crop_type_id),

  nutrient_source_id  bigint
                      references nutrient_source(nutrient_source_id),

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

-- Cannot add an application if the only thing to change is details (should edit existing application instead)
  UNIQUE NULLS NOT DISTINCT (app_type, app_method, date_applied, rate, rate_unit, crop_type_id, nutrient_source_id, field_id, field_trial_id, plot_id, geom_id)
);

-- Force exactly one of field_id, field_trial_id, plot_id to be non-null; more than one is ambiguous.
-- TODO: Alternatively, could look at creating a trigger to post-fill relevant id columns by referencing that table.
ALTER TABLE app
  ADD CONSTRAINT app_nullable_ids_one_and_only_one_not_null_ck
  CHECK (num_nonnulls(field_id, field_trial_id, plot_id) = 1);

ALTER TABLE app
  ADD CONSTRAINT app_type_not_null_ck
  CHECK (app_type IS NOT NULL);

-- Trigger to ensure date_applied is > field.date_start and < field.date_end
CREATE TRIGGER app_valid_date_performed
BEFORE INSERT OR UPDATE ON app
FOR EACH ROW EXECUTE FUNCTION app_valid_date();

CREATE TRIGGER update_app_last_updated BEFORE UPDATE
ON app FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

-- Nutrient Source

create table nutrient_source (
  nutrient_source_id bigserial primary key,
  nutrient  text
            not null,
  n float
    not null
    default 0.0,
  p2o5  float
        not null
        default 0.0,
  k2o float
      not null
      default 0.0,
  s float
    not null
    default 0.0,
  ca  float
      not null
      default 0.0,
  mg  float
      not null
      default 0.0,
  b float
    not null
    default 0.0,
  cu  float
      not null
      default 0.0,
  fe  float
      not null
      default 0.0,
  mn  float
      not null
      default 0.0,
  mo  float
      not null
      default 0.0,
  zn  float
      not null
      default 0.0,
  ch  float
      not null
      default 0.0,
  organization_id bigint
                  not null
                  references organization(organization_id),
  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

-- Cannot add a nutrient with the same name because of its ambiguity (should add a new name instead)
  UNIQUE NULLS NOT DISTINCT (nutrient)
);

CREATE TRIGGER nutrient_source_valid_date_performed BEFORE UPDATE
ON nutrient_source FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

ALTER TABLE nutrient_source
  ADD CONSTRAINT nutrient_analysis_between_zero_100_ck
  CHECK (
    n >= 0 AND n <= 100
  AND p2o5 >= 0 AND p2o5 <= 100
  AND k2o >= 0 AND k2o <= 100
  AND s >= 0 AND s <= 100
  AND ca >= 0 AND ca <= 100
  AND mg >= 0 AND mg <= 100
  AND b >= 0 AND b <= 100
  AND cu >= 0 AND cu <= 100
  AND fe >= 0 AND fe <= 100
  AND mn >= 0 AND mn <= 100
  AND mo >= 0 AND mo <= 100
  AND zn >= 0 AND zn <= 100
  AND ch >= 0 AND ch <= 100
  );

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

-- Force exactly one of field_id, field_trial_id, plot_id to be non-null; more than one is ambiguous.
ALTER TABLE observation
  ADD CONSTRAINT observation_nullable_ids_one_and_only_one_not_null_ck
  CHECK (num_nonnulls(field_id, field_trial_id, plot_id) = 1);

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

-- S3

CREATE TYPE cateogry_type_enum AS ENUM ('REMOTE_SENSING', 'SOIL', 'TISSUE', 'GRAIN', 'STOVER', 'WEATHER', 'SENSOR');
CREATE TYPE file_format_type_enum AS ENUM ('PARQUET', 'TIF', 'CSV', 'GEOJSON', 'JSON');

create table s3 (
  s3_id bigserial
        primary key,

  s3_url  text
          not null,

  file_format file_format_type_enum
              not null,

  organization_id bigint
                  not null
                  references organization(organization_id),

  category  cateogry_type_enum[]
            default NULL::cateogry_type_enum[],  -- Allows multiple categories to be listed

  -- TODO: version, last_modified, size, other s3 information

  details jsonb
          not null
          default '{}'::jsonb,

  created  timestamp without time zone
              not null
              default (now() at time zone 'utc'),

  last_updated  timestamp without time zone
                not null
                default (now() at time zone 'utc'),

  -- Cannot have duplicate s3_urls
  UNIQUE NULLS NOT DISTINCT (s3_url)

);

CREATE TRIGGER update_s3_last_updated BEFORE UPDATE
ON s3 FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


-- MUST GRANT USERS ACCESS ONCE THE TABLES ARE ALREADY CREATED

-- adjust permissions for `demeter_user`
grant select, insert, update, delete on all tables in schema test_demeter to demeter_user;
grant usage, select on all sequences in schema test_demeter to demeter_user;
alter default privileges in schema test_demeter grant usage on sequences to demeter_user;
grant usage on schema test_demeter to demeter_user;

-- adjust permissions for `demeter_ro_user`
grant select on all tables in schema test_demeter to demeter_ro_user;
grant select on all sequences in schema test_demeter to demeter_ro_user;
grant usage on schema test_demeter to demeter_ro_user;


CREATE OR REPLACE VIEW plot_details AS
SELECT
  f.organization_id,
  f.field_id,
  ft.field_trial_id,
  p.plot_id,
  p.name as name_plot,
  p.treatment_id,
  p.block_id,
  p.replication_id,
  ft.name as name_field_trial,
  ft.date_start as date_start_field_trial,
  ft.date_end as date_end_field_trial,
  ft.grouper_id as grouper_id_field_trial,
  ft.details as details_field_trial,
  get_geometry_from_id(f.geom_id, ft.geom_id, p.geom_id) as geometry
FROM plot p
JOIN field_trial ft USING(field_trial_id)
JOIN field f ON f.field_id = ft.field_id
JOIN geom g
  ON g.geom_id IN (f.geom_id, ft.geom_id, p.geom_id)
  AND (g.geom_id = get_geom_id(f.geom_id, ft.geom_id, p.geom_id));