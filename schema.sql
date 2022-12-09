-- TODO: Cascading behaviors
-- TODO: Consider alternatives to constraint triggers
--       They aren't technically 100% serializable
--       Need to look into the importance of serializability
-- TODO: Identity columns instead of serials

-- Database Setup

drop schema if exists dem_local cascade;
create schema dem_local;
set schema 'dem_local';

create extension postgis with schema public;
create extension postgis_raster with schema public;
-- TODO: Fix this extension
-- create extension "postgres-json-schema" with schema public;

set search_path = dem_local, public;
create role read_and_write;
grant select, insert on all tables in schema dem_local to read_and_write;
grant usage on schema dem_local to read_and_write;

CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.last_updated = now();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Geometry Tables

create table geom (
  geom_id bigserial primary key,
  geom geometry(Geometry, 4326) not null,
  check (ST_IsValid(geom))
);

-- TODO: Enforce SRID like this:
--  ALTER xxxx ADD CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 28355)

-- TODO: Table for geometries that get 'repaired' with their 'IsValidMessage' and 'IsValidDetails'

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


create table parcel_group (
  parcel_group_id bigserial primary key,
  -- TODO: Add cycle detection constraint
  parent_parcel_group_id bigint
                        references parcel_group(parcel_group_id),
  unique (parcel_group_id, parent_parcel_group_id),

  name text,

  constraint roots_must_be_named check (
    not (parent_parcel_group_id is null and name is null)
  ),
  unique(parent_parcel_group_id, name),

  details jsonb
          not null
          default '{}'::jsonb,
  last_updated  timestamp without time zone
                not null
                default now()
);

CREATE UNIQUE INDEX unique_name_for_null_roots_idx on parcel_group (name) where parent_parcel_group_id is null;

create table parcel (
  parcel_id bigserial
           primary key,

  geom_id   bigint
            not null
            references geom(geom_id),
  parcel_group_id bigint
                  references parcel_group(parcel_group_id)
);


create table field (
  field_id bigserial primary key,
  parcel_id bigint
            references parcel(parcel_id),

  name text,

  created  timestamp without time zone
              not null
              default now(),

  details jsonb
          not null
          default '{}'::jsonb,
  last_updated  timestamp without time zone
                not null
                default now()
);

create table crop_type (
  crop_type_id bigserial
               primary key,

  species      text
               not null,

  cultivar     text,

  unique (species, cultivar),

  parent_id_1  bigint,
  parent_id_2  bigint,
  unique (parent_id_1, parent_id_2),
  check (parent_id_1 > parent_id_2),

  details     jsonb
              not null
              default '{}'::jsonb,

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


create table observation_type (
  observation_type_id bigserial primary key,
  type_name     text unique not null
);
ALTER TABLE observation_type
  ADD CONSTRAINT observation_type_lowercase_ck
  CHECK (type_name = lower(type_name));


create table planting (
  crop_type_id  bigint
                not null
                references crop_type(crop_type_id),
  field_id      bigint references field(field_id) not null,
  planted       timestamp without time zone not null,
  primary key(crop_type_id, field_id, planted),

  observation_type_id bigint
                      references observation_type(observation_type_id),

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


create table harvest (
  crop_type_id  bigint
                not null
                references crop_type(crop_type_id),
  field_id      bigint references field(field_id) not null,
  planted timestamp without time zone not null,

  foreign key (crop_type_id, field_id, planted) references planting(crop_type_id, field_id, planted),

  observation_type_id bigint
                      references observation_type(observation_type_id),

  primary key (crop_type_id, field_id, planted, observation_type_id),

  last_updated  timestamp without time zone
                not null
                default now(),
  details       jsonb
                not null
                default '{}'::jsonb
);

CREATE TRIGGER update_harvest_last_updated BEFORE UPDATE
ON planting FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


create table crop_stage (
  crop_stage_id bigserial primary key,
  crop_stage text unique
);

create table crop_progress (
  crop_type_id  bigint
                not null
                references crop_type(crop_type_id),
  field_id      bigint references field(field_id) not null,
  planted timestamp without time zone not null,

  observation_type_id bigint
                      references observation_type(observation_type_id),

  foreign key (crop_type_id, field_id, planted) references planting(crop_type_id, field_id, planted),

  crop_stage_id bigint references crop_stage(crop_stage_id) not null,

  primary key (crop_type_id, field_id, planted, crop_stage_id)
);


create table operation (
  operation_id bigserial primary key,

  parcel_id    bigint
               not null
               references parcel(parcel_id),
  observation_type_id bigint
                      references observation_type(observation_type_id),
  unique (parcel_id, observation_type_id),

  last_updated  timestamp without time zone
                not null
                default now(),

  details       jsonb
                not null
                default '{}'::jsonb
);


CREATE TRIGGER update_operation_last_updated BEFORE UPDATE
ON operation FOR EACH ROW EXECUTE PROCEDURE
update_last_updated_column();


-----------------
-- Type Tables --
-----------------

-- Raw Input Types


create table unit_type (
  unit_type_id   bigserial
                 primary key,
  observation_type_id  bigint
                 not null
                 references observation_type(observation_type_id),
  unit           text
                 not null,
  unique (observation_type_id, unit)
);
ALTER TABLE unit_type
  ADD CONSTRAINT unit_type_start_end_w_alphanumeric_ck
  CHECK (unit ~ '^[A-Za-z](.*[A-Za-z0-9])?$');



-- S3 Type
--   Matrix, Raster, etc
create table s3_type (
  s3_type_id   bigserial primary key,
  type_name    text not null unique
);
ALTER TABLE s3_type
  ADD CONSTRAINT s3_type_lowercase_ck
  CHECK (type_name = lower(type_name));

-- TODO: Consider using the 'citext' extension to enforce
--         case-insensitive uniqueness on 'driver'
create table s3_type_dataframe (
  s3_type_id   bigint
               primary key
               references s3_type(s3_type_id),
  driver       text not null,
  has_geometry boolean not null
);



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


create table function (
  -- TODO: UUID seems like the correct type but we need to understand function storage
  function_id   bigserial
                primary key,

  function_name text
                not null,

  -- These are development versions which are distinct from published versions
  major            int not null,
  minor            int not null,
  unique(function_name, major, minor),

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
-- Value Tables --
---------------------

create table geospatial_key (
  geospatial_key_id bigserial
                    primary key,
  geom_id    bigint
             not null
             references geom(geom_id),
  parcel_id   bigint
             references parcel(parcel_id)
);


create table temporal_key (
  temporal_key_id bigserial
                  primary key,
  start_date      date
                  not null,
  end_date        date
                  not null
);


create table observation_value (
  observation_value_id bigserial
                 primary key,

  parcel_id      bigint
                 not null
                 references parcel(parcel_id),
  unit_type_id   bigint
                 references unit_type(unit_type_id)
                 not null,

  acquired       timestamp without time zone
                 not null,

  quantity       float
                 not null,

  geom_id        bigint
                 references geom(geom_id),

  last_updated   timestamp without time zone
                 not null
                 default now(),

  details        jsonb
                 not null
                 default '{}'::jsonb
);


CREATE TRIGGER update_observation_last_updated BEFORE INSERT or UPDATE
ON observation_value FOR EACH ROW EXECUTE PROCEDURE


create table s3_object (
  s3_object_id bigserial primary key,
  s3_type_id   bigint references s3_type(s3_type_id),

  key          text not null,
  bucket_name  text not null,

  unique (s3_object_id, key, bucket_name)
);


create table s3_object_key (
  s3_object_id      bigint
                    references s3_object (s3_object_id),

  geospatial_key_id bigint
                    references geospatial_key(geospatial_key_id),
  temporal_key_id   bigint
                    references temporal_key(temporal_key_id),

  primary key(s3_object_id, geospatial_key_id, temporal_key_id)
);


----------------------
-- Parameter Tables --
----------------------

create table observation_parameter (
  function_id        bigint
                     references function(function_id),

  observation_type_id      bigserial
                     not null
                     references observation_type(observation_type_id),

  primary key(function_id, observation_type_id)

);

create table http_parameter (
  function_id  bigint
               references function(function_id),
  http_type_id bigint
               references http_type(http_type_id),
  primary key(function_id, http_type_id),
  last_updated  timestamp without time zone
                not null
                default now(),
  details      jsonb
               not null
               default '{}'::jsonb
);

create table s3_input_parameter (
  function_id   bigint
                references function(function_id),
  s3_type_id    bigint references s3_type(s3_type_id),
  primary key   (function_id, s3_type_id),

  last_updated  timestamp without time zone
                not null
                default now(),
  details       jsonb
                not null
                default '{}'::jsonb
);

create table s3_output_parameter (
  s3_output_parameter_name text,
  function_id              bigint
                           references function(function_id),
  unique (s3_output_parameter_name, function_id),

  -- TODO: 's3_type_id' shouldn't be a part of the pkey
  s3_type_id     bigint references s3_type(s3_type_id),
  primary key    (s3_output_parameter_name, s3_type_id, function_id),

  last_updated  timestamp without time zone
                not null
                default now(),
  details        jsonb
                 not null
                 default '{}'::jsonb
);

create type keyword_type as ENUM('STRING', 'INTEGER', 'FLOAT', 'JSON');

create table keyword_parameter (
  keyword_name text,
  keyword_type keyword_type,
  function_id bigint
              references function(function_id),
  primary key(keyword_name, function_id)
);


----------------------
-- Argument Tables --
----------------------

-- TODO: Details?
create table execution (
  execution_id bigserial primary key,

  function_id   bigint
                not null
                references function(function_id)
);

create table execution_key (
  execution_id bigint not null references execution(execution_id),
  geospatial_key_id bigint
                    references geospatial_key(geospatial_key_id),
  temporal_key_id   bigint
                    references temporal_key(temporal_key_id),

  primary key (execution_id, geospatial_key_id, temporal_key_id)
);


create table observation_argument (
  execution_id  bigint
                not null
                references execution(execution_id),

  function_id   bigint
                not null
                references function(function_id),
  observation_type_id bigserial
                not null
                references observation_type(observation_type_id),
  foreign key (function_id, observation_type_id)
    references observation_parameter(function_id, observation_type_id),

  primary key (execution_id, function_id, observation_type_id),

  -- TODO: Is this heuristic good enough?
  number_of_observations bigint
                         not null
);

create table http_argument (
  execution_id bigint
               not null
               references execution(execution_id),

  function_id  bigint
               not null
               references function(function_id),
  http_type_id bigint
               not null
               references http_type(http_type_id),

  foreign key (function_id, http_type_id)
    references http_parameter (function_id, http_type_id),

  primary key (execution_id, function_id, http_type_id),

  -- TODO: Is this heuristic good enough?
  number_of_observations bigint
                         not null
);

create table s3_input_argument (
  execution_id bigint
               not null
               references execution(execution_id),

  function_id bigint
              not null
              references function(function_id),
  s3_type_id  bigint
              not null
              references s3_type(s3_type_id),

  foreign key (function_id, s3_type_id)
    references s3_input_parameter (function_id, s3_type_id),

  primary key (execution_id, function_id, s3_type_id),

  s3_object_id bigint
               not null
               references s3_object(s3_object_id)

  --number_of_observations bigint
);

create table s3_output_argument (
  execution_id bigint
               not null
               references execution(execution_id),

  s3_output_parameter_name text
                           not null,
  function_id bigint
              not null
              references function(function_id),
  s3_type_id  bigint
              not null
              references s3_type(s3_type_id),

  foreign key (s3_output_parameter_name, function_id, s3_type_id)
    references s3_output_parameter (s3_output_parameter_name, function_id, s3_type_id),

  primary key (execution_id, function_id, s3_type_id),

  s3_object_id bigint
               not null
               references s3_object(s3_object_id)
);

create table keyword_argument (
  execution_id bigint
               not null
               references execution(execution_id),

  function_id bigint
              references function(function_id),
  keyword_name text,

  foreign key (keyword_name, function_id)
    references keyword_parameter(keyword_name, function_id),

  primary key (execution_id, keyword_name),

  value_number float,
  value_string text
  -- TODO: Eventually support a JSON type with accompanying JSON schema
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
  last_updated  timestamp without time zone
                not null
                default now(),
  details     jsonb
              not null
              default '{}'::jsonb

);


-- TODO: This needs to be factored out to its own repo
--       Maybe also with LocalValue and LocalType?
-- Grid Base Schema

create table node (
  node_id bigserial
          primary key,

  geom geometry(Geometry, 4326) not null,
  CONSTRAINT
  enforce_node_polygon CHECK (
    (
      geometrytype(geom) = 'POLYGON'::text OR
      geometrytype(geom) = 'MULTIPOLYGON'::text
    ) AND
    ST_IsValid(geom)
   )
  ,

  value float
);

CREATE INDEX CONCURRENTLY node_idx on node using SPGIST(geom);

-- Optional Raster
create table node_raster (
  node_id bigint references node(node_id),
  raster raster
);

create table root (
  root_id bigserial primary key,

  root_node_id bigint
               references node(node_id)
               not null,
  observation_type_id bigint
                references observation_type(observation_type_id)
                not null,

  time          timestamp without time zone
                not null,

  last_updated  timestamp without time zone
                not null
                default now(),
  details       jsonb
                not null
                default '{}'::jsonb
);

create table node_ancestry (
  parent_node_id bigint
                 references node(node_id),
  -- TODO: Contained-by-parent constraint

  node_id bigint references node(node_id) not null,

  primary key (parent_node_id, node_id)
);

