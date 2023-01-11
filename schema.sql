-- TODO: Cascading behaviors
-- TODO: Consider alternatives to constraint triggers
--       They aren't technically 100% serializable
--       Need to look into the importance of serializability
-- TODO: Identity columns instead of serials

-- Database Setup

drop schema if exists test_demeter cascade;
create schema test_demeter;
set schema 'test_demeter';

create extension if not exists postgis with schema public;
create extension if not exists postgis_raster with schema public;
-- TODO: Fix this extension
-- create extension "postgres-json-schema" with schema public;

set search_path = test_demeter, public;
create role read_and_write;
grant select, insert on all tables in schema test_demeter to read_and_write;
grant usage on schema test_demeter to read_and_write;

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


-----------------
-- Type Tables --
-----------------

-- Raw Input Types

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
  field_id   bigint
             references field(field_id)
);


create table temporal_key (
  temporal_key_id bigserial
                  primary key,
  date_start      date
                  not null,
  date_end        date
                  not null
                  -- default ('infinity'::timestamp at time zone 'utc')
);

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

-- S3 OBJECT

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

create table s3_input_parameter (
  function_id   bigint
                references function(function_id),
  s3_type_id    bigint references s3_type(s3_type_id),
  primary key   (function_id, s3_type_id),

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

create table s3_output_parameter (
  s3_output_parameter_name text,
  function_id              bigint
                           references function(function_id),
  unique (s3_output_parameter_name, function_id),

  -- TODO: 's3_type_id' shouldn't be a part of the pkey
  s3_type_id     bigint references s3_type(s3_type_id),
  primary key    (s3_output_parameter_name, s3_type_id, function_id),

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


-- TODO: This needs to be factored out to its own repo
--       Maybe also with Observation and ObservationType?
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

create table node_ancestry (
  parent_node_id bigint
                 references node(node_id),
  -- TODO: Contained-by-parent constraint

  node_id bigint references node(node_id) not null,

  primary key (parent_node_id, node_id)
);

