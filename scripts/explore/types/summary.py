from typing import Optional, Any, Sequence, Dict
from demeter.db import TableId

from dataclasses import dataclass

import logging
logger = logging.getLogger()

from ..summary import Summary

@dataclass
class TypeSummary(Summary):
  local_type_id : TableId
  type_name : str
  type_category : Optional[str]

  local_group_id : Optional[TableId]
  group_name : Optional[str]
  group_category : Optional[str]

  #units : Sequence[str]
  unit_to_count : Dict[str, int]

  count : int


# TODO: Filter on fields
def getTypeSummaries(cursor : Any) -> Sequence[TypeSummary]:
  stmt = """with field_value as (
              select F.field_id,
                     V.local_value_id,
                     V.unit_type_id,
                     V.local_group_id,
                     V.acquired
              from test_mlops.field F
              join test_mlops.local_value V on F.field_id = V.field_id

            ), units as (
              select local_type_id,
                     jsonb_object_agg(
                       unit_type_id,
                       jsonb_build_object(
                         'unit', unit,
                         'count', unit_count,
                         'earliest', earliest,
                         'latest', latest
                       )
                     ) as id_to_unit
              from (
                select T.local_type_id,
                       U.unit_type_id,
                       U.unit,
                       min(FV.acquired)::date as earliest,
                       max(FV.acquired)::date as latest,
                       count(*) as unit_count
                from field_value FV
                natural join test_mlops.unit_type U
                natural join test_mlops.local_type T
                group by T.local_type_id, U.unit_type_id, U.unit
              ) x
              group by local_type_id

            ), groups as (
              select local_type_id,
                     jsonb_object_agg(
                       local_group_id,
                       jsonb_build_object(
                         'name', group_name,
                         'category', group_category,
                         'count', group_count,
                         'earliest', earliest,
                         'latest', latest
                       )
                     ) as id_to_group
              from (
                select US.local_type_id,
                       G.local_group_id,
                       G.group_name,
                       G.group_category,
                       min(FV.acquired)::date as earliest,
                       max(FV.acquired)::date as latest,
                       count(*) as group_count
                from field_value FV
                natural join units US
                natural join test_mlops.local_group G
                group by US.local_type_id, G.local_group_id

              ) x
              group by local_type_id

            ) select T.local_type_id,
                     T.type_name,
                     T.type_category,

                     (select sum((unit->'count')::bigint)
                      from jsonb_each(U.id_to_unit) as f(id, unit)
                     ) as value_count,

                     unit_to_count,
                     units,
                     jsonb_array_length(units) as unit_count,
                     groups,
                     jsonb_array_length(groups) as group_count

              from test_mlops.local_type T
              natural join units U
              natural left join groups GS,
              lateral (
                select jsonb_object_agg(
                         (unit->'unit')::text,
                         unit->'count'
                       ) as unit_to_count
                from jsonb_each(U.id_to_unit) as f(id, unit)
              ) x,
              lateral (
                select jsonb_agg(unit) as units
                from jsonb_object_keys(unit_to_count) as unit
              ) y,
              lateral (
                select jsonb_agg(g) as groups
                from jsonb_each(GS.id_to_group) as f(id, g)
              ) z
              order by T.type_name, T.type_category, value_count desc, unit_count desc
         """
  cursor.execute(stmt)

  results = cursor.fetchall()
  logger.warning("# RESULTS: %s",len(results))
  for r in results:
    logger.warning("R: %s",r)
  return [ r for r in results ]

