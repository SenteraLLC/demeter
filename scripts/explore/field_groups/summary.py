from typing import TypedDict, Optional, Any, Sequence, Dict
from typing import cast

from demeter.data import FieldGroup
from demeter.db import TableId

from dataclasses import dataclass
from dataclasses import make_dataclass

from ..summary import Summary


@dataclass(frozen=True)
class FieldGroupSummary(Summary):
  field_group_id : TableId
  parent_field_group_id : Optional[TableId]

  name : str
  external_id : str
  depth : int

  immediate_field_count : int
  total_field_count : int

  child_field_group_ids : Sequence[TableId]
  child_field_group_count : int


# TODO: Filter on field groups
def getFieldGroupSummaries(cursor : Any,
                          ) -> Sequence[FieldGroupSummary]:
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

  ), field_counts as (
    select D.field_group_id,
           count(F.*) as field_count
    from descendent D
    left join test_mlops.field F on D.field_group_id = F.field_group_id
    group by D.field_group_id

  ), ancestor_counts as (
    select D.parent_field_group_id as ancestor_field_group_id,
           D.parent_field_group_id,
           D.field_group_id,
           FC.field_count
    from descendent D
    join field_counts FC on D.field_group_id = FC.field_group_id
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
           join descendent D on AC.parent_field_group_id = D.field_group_id
           join field_counts FC on D.field_group_id = FC.field_group_id

  ), aggregate_counts as (
    select ancestor_field_group_id as field_group_id,
           sum(field_count) as total_field_count
    from ancestor_counts AC
    group by ancestor_field_group_id

  ) select D.*,
           FC.field_count as immediate_field_count,
           child_field_group_ids,
           AC.total_field_count
    from descendent D
    join aggregate_counts AC on D.field_group_id = AC.field_group_id
    join field_counts FC on D.field_group_id = FC.field_group_id,
    lateral (
      select jsonb_agg(D2.field_group_id) as child_field_group_ids
      from descendent D2
      where D.field_group_id = D2.parent_field_group_id
      group by D.field_group_id
    ) children
    order by depth asc,
             coalesce(jsonb_array_length(child_field_group_ids), 0) desc, FC.field_count desc, total_field_count desc;
  """
  cursor.execute(stmt)
  results = cursor.fetchall()
  return [
    FieldGroupSummary(
      field_group_id = r.field_group_id,
      parent_field_group_id = r.parent_field_group_id,

      name = r.name,
      external_id = r.external_id,
      depth = r.depth,

      immediate_field_count = r.immediate_field_count,
      total_field_count = r.total_field_count,

      child_field_group_ids = r.child_field_group_ids,
      child_field_group_count = len(r.child_field_group_ids),
    ) for r in results
  ]
  out = cast(Sequence[FieldGroupSummary], [make_dataclass('FieldGroupSummary', r) for r in results])
  return out


