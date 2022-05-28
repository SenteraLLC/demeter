from datetime import time, date

from typing import TypedDict, Sequence, Dict, Optional, Any, List

from demeter import Owner

class HarvestPlanting(TypedDict):
  geo_id    : int
  completed : Optional[date]
  details   : Dict[str, Any]

class Planting(HarvestPlanting):
  pass

class Harvest(HarvestPlanting):
  pass

class CropProgress(TypedDict):
  geo_id : Optional[int]
  stage  : str
  day    : date

class Crop(TypedDict):
  species   : str
  cultivar  : str
  harvests  : Sequence[Harvest]
  plantings : Sequence[Planting]
  progress  : Sequence[CropProgress]

# TODO: Support raster
Raster = Any

class InputType(TypedDict):
  input    : str
  category : str

class InputData(TypedDict):
  geo_id   : int
  value    : float
  raster   : Raster
  acquired : date

class Input(TypedDict):
  type : InputType
  data : List[InputData]

InputGroup = Dict[str, List[Input]]

class DataField(TypedDict):
  owner  : Owner
  year   : int
  geo_id : int
  crops  : List[Crop]
  inputs       : List[Input]
  input_groups : List[InputGroup]


# TODO: Need to make a few variations of this function:
#       1) Search by owner
#       2) Search by geometry (probably where farms overlap?)
def get_field(cursor : Any, field_id : int) -> DataField:
  stmt = """
            with field_owner as (
              select F.field_id, F.year, F.geo_id, jsonb_agg(to_jsonb(O) - 'owner_id') as owner, jsonb_agg(to_jsonb(G) - 'geo_id') as grower
              from field F, owner O, grower G
              where F.owner_id = O.owner_id and F.grower_id = G.grower_id and
                    F.field_id = %(field_id)s
              group by F.field_id, F.year, F.geo_id

            ), crop as (
              select PCT.species, PCT.cultivar,
                     coalesce(
                       jsonb_agg(
                         jsonb_build_object(
                           'geo_id', P.geo_id,
                           'completed', P.completed,
                           'details', P.details
                        )
                      )
                       filter (where P.geo_id is not null), '[]'
                     ) as plantings,
                     coalesce(
                       jsonb_agg(
                         jsonb_build_object(
                           'geo_id', H.geo_id,
                           'completed', H.completed,
                           'details', H.details
                         )
                       )
                       filter (where H.geo_id is not null), '[]'
                     ) as harvests,
                     coalesce(
                       jsonb_agg(
                         jsonb_build_object(
                           'geo_id', CP.geo_id,
                           'stage', CS.crop_stage,
                           'day', CP.day
                         )
                       )
                       filter (where CP.geo_id is not null), '[]'
                    ) as progress
              from field_owner FO
              left join (planting P
                         inner join crop_type PCT on P.crop_type_id = PCT.crop_type_id
                        )
                on FO.field_id = P.field_id
              left join (harvest H
                         inner join crop_type HCT on H.crop_type_id = HCT.crop_type_id
                        )
                on FO.field_id = H.field_id
              left join (crop_progress CP
                         inner join crop_type CPCT on CP.crop_type_id = CPCT.crop_type_id
                         inner join crop_stage CS on CP.crop_stage_id = CS.crop_stage_id
                        )
                on P.field_id = CP.field_id and P.crop_type_id = CP.crop_type_id and P.geo_id = CP.planting_geo_id
              group by PCT.species, PCT.cultivar

            ), crops as (
              select jsonb_agg(
                       jsonb_build_object(
                         'species',   species,
                         'cultivar',  cultivar,
                         'plantings', plantings,
                         'harvests',  harvests,
                         'progress',  progress
                        )
                      ) as crops
              from crop
              group by species, cultivar

            ), all_inputs as (
              select I.input_id, I.input_group_id, IT.input, IT.category,
                     jsonb_build_object(
                       'input_id', I.input_id,
                       'geo_id', I.geo_id,
                       'quantity', IV.quantity,
                       'unit', UT.unit,
                       'acquired', I.acquired,
                       'details', IV.details
                     ) as data
              from field_owner FO, input I, geo G, input_type IT, input_value IV, unit_type UT
              where I.field_id = FO.field_id and I.geo_id = G.geo_id and
                    I.input_type_id = IT.input_type_id and I.input_value_id = IV.input_value_id and
                    IV.unit_type_id = UT.unit_type_id

            ), all_inputs_by_type as (
              select input_group_id,
              jsonb_build_object(
                       'type', jsonb_build_object('input', input, 'category', category),
                       'data', json_agg(data)
                     ) as input
              from all_inputs
              group by input_group_id, input, category

            ), non_group_inputs as (
              select json_agg(input) as inputs
              from all_inputs_by_type
              where input_group_id is null

            ), group_input as (
              select jsonb_build_object(
                       G.input_group_type_name,
                       jsonb_agg(
                         AI.input
                       )
                     ) as input_group
              from all_inputs_by_type AI, input_group IG, input_group_type G
              where AI.input_group_id = IG.input_group_id and IG.input_group_type_id = G.input_group_type_id
              group by G.input_group_type_name

            ), group_inputs as (
              select jsonb_agg(input_group) as input_groups
              from group_input

            )  select json_build_object(
                       'owner', FO.owner,
                       'year',  FO.year,
                       'geo',   FO.geo_id,
                       'crops', C.crops,
                       'inputs', NGI.inputs,
                       'inputs_groups', GI.input_groups
                     ) as field_output
              from field_owner FO, crops C, non_group_inputs NGI, group_inputs GI;
"""
  cursor.execute(stmt, {"field_id": field_id})
  result = cursor.fetchone()
  return result["field_output"]


import geopandas # type: ignore

# TODO: Add more filter support
def getInputsByField(cursor : Any,
                     field_id : int,
                     input_names : List[str],
                     group_names : List[str],
                    ) -> geopandas.GeoDataFrame:
  stmt = """
          select F.year, G.farm, O.owner, I.input_id, I.acquired, IV.quantity, U.unit, IGT.input_group_type_name, geo.geo
          from field F
          join grower     G on G.grower_id = F.grower_id
          join owner      O on O.owner_id  = F.owner_id
          join input      I on I.field_id  = F.field_id
          join input_type IT on IT.input_type_id = I.input_type_id
          join input_value IV on IV.input_value_id = I.input_value_id
          join unit_type U on IV.unit_type_id = U.unit_type_id
          left join input_group IG on IG.input_group_id = I.input_group_id
          left join input_group_type IGT on IGT.input_group_type_id = IG.input_group_type_id
          join geo on I.geo_id = geo.geo_id
          where F.field_id = %(field_id)s and (IT.input = any(%(input_names)s) or IGT.input_group_type_name = any(%(group_names)s));
         """

  result = geopandas.read_postgis(stmt, connection, "geo", params={"field_id": field_id, "input_names": input_names, "group_names": group_names})

  return result



import psycopg2
import psycopg2.extras
import json
import argparse

list_of_strings = lambda f : f.split(",")

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Load a field and all of its input data as JSON')
  parser.add_argument('--host', type=str, help='postgres hostname', required=True)
  parser.add_argument('--field_id', type=str, help='field id from mlops database', required=True)
  parser.add_argument('--features', type=list_of_strings, help='list of feature names, separated by commas', default = [], required=False)
  parser.add_argument('--feature_groups', type=list_of_strings, help='list of feature groups, separated by commas', default = [], required = False)
  args = parser.parse_args()

  options = "-c search_path=test_mlops"
  connection = psycopg2.connect(host=args.host, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  # This code gets a geopandas matrix for the given list of inputs from a field id
  if len(args.features) or len(args.feature_groups):
    result = getInputsByField(cursor, args.field_id, args.features, args.feature_groups)
    #result = getInputsByField(cursor, args.field_id, ["petiole", "nitrogen", "weather"], ["sentinel"])
    print(result)

  # This code gets a human-readable json structure. It could potentially be used for metadata
  data_field = get_field(cursor, args.field_id)
  print(json.dumps(data_field, indent = 2))


