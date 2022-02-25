-- TODO: Cascading behaviors
-- TODO: Consider alternatives to constraint triggers
--       They aren't technically 100% serializable
--       Need to look into the importance of serializability
-- TODO: Identity columns instead of serials

-- Database Setup

drop schema if exists test_mlops cascade;
create schema test_mlops;
set schema 'test_mlops';

create extension postgis with schema public;
create extension postgis_raster with schema public;
create extension "postgres-json-schema" with schema public;

set search_path = test_mlops, public;

CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.last_updated = now();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Geometry Tables

-- TODO: Unique geometry?
create table geom (
  geom_id bigserial primary key,
  container_geom_id bigint
                   references geom(geom_id)
                   check (geom_id <> container_geom_id),

  geom geometry(Geometry, 4326) not null,
  check (ST_IsValid(geom)),

  last_updated  timestamp without time zone
                not null
                default now()
);

CREATE INDEX CONCURRENTLY geom_idx on geom using SPGIST(geom);

CREATE TRIGGER update_geom_last_updated BEFORE UPDATE
ON geom FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

alter table geom add constraint fk_container_geom foreign key (container_geom_id) references geom(geom_id);

-- TODO: Implement this when the API is capable of cleaning geometries
--       Right now, the sample data can't pass this constraint

--create function geom_container_must_contain_geom() RETURNS trigger
--  LANGUAGE plpgsql AS
--$$BEGIN
--  --(select OLD.geom_id is not NULL) AND
--  IF NOT (select ST_contains(geom.geom, OLD.geom)
--          from geom
--          where OLD.container_id = geom.geom_id and OLD.container_id is not NULL
--         )
--  THEN
--    RAISE EXCEPTION 'A container must contain its child geometry.';
--  END IF;
--
--  RETURN old;
--END;$$;

--create constraint trigger geom_container_must_contain_geom
--       after insert or update on geom
--       deferrable initially deferred
--       for each row execute procedure geom_container_must_contain_geom();


-- TODO: Is this true?
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


create table owner (
  owner_id bigserial primary key,
  owner text not null unique
);

create table grower (
  grower_id bigserial primary key,
  farm text not null unique
);


-- TODO: This geom must be a polygon / multipolygon
create table field (
  field_id bigserial
           primary key,

  owner_id bigint
           references owner(owner_id)
           not null,
  -- TODO: I think that this may be made redundant by the 'acquired' fields in the 'input' table.
  year     smallint
           not null,
  geom_id   bigint
           not null
           references geom(geom_id),

  unique (geom_id, owner_id, year),

  grower_id bigint
           references grower(grower_id),

  created  timestamp without time zone
              not null
              default now()


  -- TODO: Is this constraint true?
  -- CHECK (container_geom_id IS null),
  -- CHECK fields cannot overlap?
);


create function field_cannot_have_container_geom() RETURNS trigger
  LANGUAGE plpgsql AS
$$BEGIN
  IF (select geom.container_geom_id is not NULL
        from geom
        where OLD.geom_id = geom.geom_id
     )
  THEN
    RAISE EXCEPTION 'A field geometry cannot have a container geometry.';
  END IF;

  RETURN old;
END;$$;

create constraint trigger field_cannot_have_container_geom
       after insert or update on field
       deferrable initially deferred
       for each row execute procedure field_cannot_have_container_geom();

create table crop_type (
  crop_type_id bigserial
               primary key,

  species      text
               not null,

  cultivar     text,

  unique (species, cultivar),

  -- TODO: Check not equal
  parent_id_1  bigint,
  parent_id_2  bigint,
  unique (parent_id_1, parent_id_2),
  check (parent_id_1 <> parent_id_2),

  last_updated  timestamp without time zone
                not null
                default now()
);

CREATE UNIQUE INDEX unique_species_cultivar_null_idx on crop_type (species, cultivar) where cultivar is null;

CREATE UNIQUE INDEX crop_type_parent_idx ON crop_type (
  LEAST(parent_id_1, parent_id_2)
, GREATEST(parent_id_1, parent_id_2)
);

CREATE TRIGGER update_crop_type_last_updated BEFORE UPDATE
ON crop_type FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_species_lowercase_ck
  CHECK (species = lower(species));
ALTER TABLE crop_type
  ADD CONSTRAINT crop_type_cultivar_lowercase_ck
  CHECK (cultivar = lower(cultivar));
CREATE UNIQUE INDEX crop_variety_null_unique_idx
  ON crop_type(species)
  WHERE (cultivar is NULL);
alter table crop_type add constraint fk_parent1_crop_type foreign key (parent_id_1) references crop_type(crop_type_id);
alter table crop_type add constraint fk_parent2_crop_type foreign key (parent_id_2) references crop_type(crop_type_id);


-- TODO: Input data and field data should be fully contained by a field

-- Note: A planting geometry must intersect one-and-only-one field
create table planting (
  field_id      bigint
                not null
                references field(field_id),
  crop_type_id  bigint
                not null
                references crop_type(crop_type_id),
  geom_id       bigint
                not null
                references geom(geom_id),
  primary key (field_id, geom_id, crop_type_id),

  completed     date,

  last_updated  timestamp without time zone
                not null
                default now(),
  details       jsonb
                not null
                default '{}'::jsonb
);

CREATE TRIGGER update_planting_last_updated BEFORE UPDATE
ON planting FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();

create table crop_stage (
  crop_stage_id bigserial primary key,
  crop_stage text unique
);

create table crop_progress (
  field_id         bigint,
  crop_type_id     bigint,
  planting_geom_id bigint,

  foreign key (field_id, crop_type_id, planting_geom_id)
    references planting (field_id, crop_type_id, geom_id),

  geom_id bigint
         references geom(geom_id),

  crop_stage_id bigint references crop_stage(crop_stage_id) not null,

  day    date,

  primary key (field_id, planting_geom_id, geom_id, crop_type_id, crop_stage_id)
);



create table harvest (
  field_id      bigint
                not null
                references field(field_id),
  crop_type_id  bigint
                not null
                references crop_type(crop_type_id),
  geom_id       bigint
               not null
               references geom(geom_id),
  primary key (field_id, geom_id, crop_type_id),

  completed     date
                not null,
  last_updated  timestamp without time zone
                not null
                default now(),

  details       jsonb
                not null
                default '{}'::jsonb
);

CREATE TRIGGER update_harvest_last_updated BEFORE UPDATE
ON harvest FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();



-----------------
-- Type Tables --
-----------------

-- Raw Input Types

create table local_type (
  local_type_id bigserial primary key,
  type_name     text unique not null,
  type_category text,
  unique(type_name, type_category)
);
ALTER TABLE local_type
  ADD CONSTRAINT local_type_lowercase_ck
  CHECK (type_name = lower(type_name));
ALTER TABLE local_type
  ADD CONSTRAINT local_category_lowercase_ck
  CHECK (type_category = lower(type_category));


create table unit_type (
  unit_type_id   bigserial
                 primary key,
  local_type_id  bigint
                 not null
                 references local_type(local_type_id),
  unit           text
                 not null,
  unique (local_type_id, unit)
);
ALTER TABLE unit_type
  ADD CONSTRAINT unit_type_start_end_w_alphanumeric_ck
  CHECK (unit ~ '^[A-Za-z].+[A-Za-z0-9]$');





-- S3 Type
--   Matrix, Raster, etc

create table s3_type (
  s3_type_id bigserial primary key,
  type_name text not null
);
ALTER TABLE s3_type
  ADD CONSTRAINT s3_type_lowercase_ck
  CHECK (type_name = lower(type_name));

-- HTTP Input Type

-- TODO: Add JSON Schema plugin
create table http_type (
  http_type_id        bigserial primary key,
  type_name           text   unique not null,
  verb                text   not null, -- TODO: Enum?
  uri                 text   not null,
  uri_parameters      text[],
  request_body_schema jsonb
);

-- TODO: Model Type
--create table model (
--  model_id   uuid primary key,
--
--  output_id  bigint
--             references output(output_id),
--
--  model_name text not null
--);
--ALTER TABLE model
--  ADD CONSTRAINT model_name_lowercase_ck
--  CHECK (model_name = lower(model_name));


-----------------------
-- Function Tables --
-----------------------

-- TODO: Should include the following types for now:
--  Transformation, SageMaker, Scoring
--  At some point, maybe add the more discrete modeling types:
--    Training, Data Report, Model Report, Feature Selection, Sampling etc
create table function_type (
  function_type_id bigserial primary key,
  function_type_name text not null,
  function_subtype_name text
);

ALTER TABLE function_type
  ADD CONSTRAINT function_type_lowercase_ck
  CHECK (function_type_name = lower(function_type_name));
ALTER TABLE function_type
  ADD CONSTRAINT function_subtype_lowercase_ck
  CHECK (function_subtype_name = lower(function_subtype_name));
CREATE UNIQUE INDEX function_subtype_null_unique_idx
  ON function_type(function_type_name)
  WHERE (function_subtype_name is NULL);


-- TODO: Probably need 'created_by' and 'updated_by' columns
create table function (
  -- TODO: UUID seems like the correct type but we need to understand function storage
  function_id   bigserial
                primary key,

  function_name text
                not null,

  -- These are development versions which are distinct from published versions
  major            int    not null,
  minor            serial not null,

  function_type_id bigint
                   references function_type(function_type_id)
                   not null,

  created          timestamp without time zone
                   not null
                   default now(),

  last_updated     timestamp without time zone
                   not null
                   default now(),

  details          jsonb
                   not null
                   default '{}'::jsonb
);
ALTER TABLE function
  ADD CONSTRAINT function_name_lowercase_ck
  CHECK (function_name = lower(function_name));


---------------------
-- Argument Tables --
---------------------

create table geospatial_key (
  geospatial_key_id bigserial
                    primary key,
  geom_id    bigint
             not null
             references geom(geom_id),
  field_id   bigint
             references field(field_id)
);


create table temporal_key (
  temporal_key_id bigserial
                  primary key,
  start_date      date
                  not null,
  end_date        date
                  not null
);


create table local_group (
  local_group_id bigserial primary key,
  group_name     text not null,
  group_category text
);
ALTER TABLE local_group
  ADD CONSTRAINT local_group_name_lowercase_ck
  CHECK (group_name = lower(group_name));


create table local_value (
  local_value_id bigserial
                 primary key,

  geom_id        bigint
                 not null
                 references geom(geom_id),
  field_id       bigint
                 references field(field_id),

  unit_type_id   bigint
                 references unit_type(unit_type_id)
                 not null,
  quantity       float
                 not null,

  local_group_id bigint
                 references local_group(local_group_id),

  acquired       timestamp without time zone
                 not null
                 default now(),

  last_updated   timestamp without time zone
                 not null
                 default now(),

  details        jsonb
                 not null
                 default '{}'::jsonb
);


CREATE TRIGGER update_local_last_updated BEFORE INSERT or UPDATE
ON local_value FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


----------------------
-- Parameter Tables --
----------------------

create table local_parameter (
  local_parameter_name text,
  function_id          bigint
                       references function(function_id),

  primary key(local_parameter_name, function_id),

  local_value_id      bigserial
                      not null
                      references local_value(local_value_id)
);

create table http_parameter (
  http_parameter_name text unique,
  function_id         bigint
                      references function(function_id),
  primary key(http_parameter_name, function_id),

  http_type_id bigint
               references http_type(http_type_id),
  details      jsonb
               not null
               default '{}'::jsonb
);

create table s3_parameter (
  s3_parameter_name text unique,
  function_id   bigint
                references function(function_id),
  unique (s3_parameter_name, function_id),

  s3_type_id    bigint references s3_type(s3_type_id),
  primary key   (s3_parameter_name, s3_type_id, function_id),

  s3_output_name     text,
  output_function_id bigint,

  details       jsonb
                not null
                default '{}'::jsonb
);

create table s3_output (
  s3_output_name text unique,
  function_id    bigint
                 references function(function_id),
  unique (s3_output_name, function_id),

  s3_type_id     bigint references s3_type(s3_type_id),
  primary key    (s3_output_name, s3_type_id, function_id),

  details        jsonb
                 not null
                 default '{}'::jsonb
);

alter table s3_parameter add constraint s3_input_output foreign key (s3_output_name, s3_type_id, output_function_id) references s3_output(s3_output_name, s3_type_id, function_id);


create table s3_output_key (
  s3_output_name    text,
  s3_type_id        bigint
                    references s3_type(s3_type_id),
  function_id       bigint
                    references function(function_id),
  geospatial_key_id bigint
                    references geospatial_key(geospatial_key_id),
  temporal_key_id   bigint
                    references temporal_key(temporal_key_id)
);


-- TODO: This should, by some (undecided) means, be a relation between a ML Ops Model Function and an Airflow DAG
-- TODO: Airflow names may just be a concatenation of model_name + major + minor
-- TODO: The function must be of type 'model', add constraint
create table published_workflow (
  function_id bigint
              references function(function_id)
              primary key,

  workflow_name text not null,
  -- TODO: Is this how semantic versioning will work?
  major       int    not null,
  minor       serial not null,
  unique (workflow_name, major, minor),
  details     jsonb
              not null
              default '{}'::jsonb

);
