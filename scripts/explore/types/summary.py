from typing import Optional, Any, Sequence, Dict
from demeter.db import TableId

from dataclasses import dataclass

from datetime import datetime

from ..summary import Summary


@dataclass(frozen=True)
class UnitSummary(Summary):
  unit_type_id : TableId
  unit : str
  count : int
  earliest : Optional[datetime]
  latest : Optional[datetime]


@dataclass(frozen=True)
class LocalGroupSummary(Summary):
  local_group_id : TableId
  name : str
  category : Optional[str]
  count : int
  earliest : Optional[datetime]
  latest : Optional[datetime]


@dataclass(frozen=True)
class TypeSummary(Summary):
  local_type_id : TableId
  type_name : str
  type_category : Optional[str]

  value_count : int

  units : Sequence[UnitSummary]
  unit_count : int
  groups : Sequence[LocalGroupSummary]
  group_count : int


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
                         'unit_type_id', unit_type_id,
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
                         'local_group_id', local_group_id,
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
                     value_count,
                     units,
                     groups

              from test_mlops.local_type T
              natural join units U
              natural left join groups GS,
              lateral (
                select jsonb_agg(unit) as units,
                       sum((unit->'count')::int) as value_count
                from jsonb_each(U.id_to_unit) as f(id, unit)
              ) y,
              lateral (
                select coalesce(
                         jsonb_agg(grp),
                         '{}'::jsonb
                       ) as groups
                from jsonb_each(GS.id_to_group) as f(id, grp)
              ) z
              order by T.type_name, T.type_category, value_count desc, jsonb_array_length(units) desc
         """
  cursor.execute(stmt)

  results = cursor.fetchall()

  return [
    TypeSummary(
      local_type_id = r.local_type_id,
      type_name = r.type_name,
      type_category = r.type_category,

      value_count = r.value_count,

      units = r.units,
      unit_count = len(r.units),
      groups = r.groups,
      group_count = len(r.groups),
    ) for r in results
  ]
  return [ r for r in results ]
