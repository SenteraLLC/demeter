from typing import TypedDict, Optional, Any, Sequence, Dict
from typing import cast

from demeter.data import FieldGroup, Field
from demeter.db import TableId

from dataclasses import dataclass
from dataclasses import make_dataclass, asdict

from ..summary import Summary

#from joblib import Memory as cache

from joblib import Memory # type: ignore
location = '/tmp/demeter/'
memory = Memory(location, verbose=0)


@dataclass(frozen=True)
class FieldGroupSummary(Summary):
  field_group_id : TableId
  parent_field_group_id : Optional[TableId]

  name : str
  external_id : str
  depth : int

  field_group_ids : Sequence[TableId]
  fields : Sequence[Field]

  total_group_count : int
  group_count : int

  total_field_count : int
  field_count : int


@memory.cache(ignore=['cursor'])   # type: ignore
def getFieldGroupSummaries(cursor : Any,
                          ) -> Dict[TableId, FieldGroupSummary]:
  stmt = """
  with recursive descendent as (
    select *,
           0 as depth
    from test_mlops.field_group G
    where parent_field_group_id is null
    UNION ALL
    select G.*,
           depth + 1
    from descendent D
    join test_mlops.field_group G on D.field_group_id = G.parent_field_group_id

  ), fields as (
    select D.field_group_id,
           coalesce(
             jsonb_agg(to_jsonb(F.*))
               filter
               (where F.field_id is not null),
             '[]'
           ) as fields
    from descendent D
    left join test_mlops.field F on D.field_group_id = F.field_group_id
    group by D.field_group_id

  ), ancestor_counts as (
    select D.field_group_id as ancestor_field_group_id,
           D.parent_field_group_id,
           D.field_group_id,
           jsonb_array_length(F.fields) as field_count
    from descendent D
    join fields F on D.field_group_id = F.field_group_id
    where not exists (
      select *
      from descendent D2
      where D2.parent_field_group_id = D.field_group_id
    )
    UNION ALL
    select D.parent_field_group_id as ancestor_field_group_id,
           D.parent_field_group_id,
           D.field_group_id,
           AC.field_count
           from ancestor_counts AC
           join descendent D on AC.ancestor_field_group_id = D.field_group_id
           join fields F on D.field_group_id = F.field_group_id
  ), aggregate_counts as (
    select ancestor_field_group_id as field_group_id,
           sum(AC.field_count) filter (where ancestor_field_group_id = field_group_id) as field_count,

           sum(field_count) as total_field_count,
           count(*) as total_group_count
    from ancestor_counts AC
    group by ancestor_field_group_id

  ) select D.*,
           F.fields as fields,
           field_group_ids,
           AC.total_group_count,
           jsonb_array_length(field_group_ids) as group_count,
           AC.total_field_count,
           AC.field_count as field_count
    from descendent D
    join aggregate_counts AC on D.field_group_id = AC.field_group_id
    join fields F on D.field_group_id = F.field_group_id
    left join lateral (
      select coalesce(
               jsonb_agg(D2.field_group_id)
                 filter
                 (where D2.field_group_id is not null),
              '[]'
             ) as field_group_ids
      from descendent D2
      where D.field_group_id = D2.parent_field_group_id
      group by D.field_group_id
    ) children on true
    order by depth asc,
             AC.total_group_count,
             group_count,
             AC.total_field_count,
             AC.field_count
  """
  cursor.execute(stmt)
  results = cursor.fetchall()
  return {
    r.field_group_id : FieldGroupSummary(
                         field_group_id = r.field_group_id,
                         parent_field_group_id = r.parent_field_group_id,

                         name = r.name,
                         external_id = r.external_id,
                         depth = r.depth,

                         field_group_ids = r.field_group_ids or [],
                         fields = r.fields or [],

                         total_group_count = r.total_group_count or 0,
                         group_count = r.group_count or 0,

                         total_field_count = r.total_field_count or 0,
                         field_count = r.field_count or 0,

                       ) for r in results
  }


