from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
)

from joblib import Memory  # type: ignore

from demeter.data import Field
from demeter.db import TableId

from ..summary import Summary

# TODO:  Support joblib from command-line
# from joblib import Memory as cache

# TODO: CLI arg
location = "/tmp/demeter/"
memory = Memory(location, verbose=0)


@dataclass(frozen=True)
class FieldGroupSummary(Summary):
    grouper_id: TableId
    parent_grouper_id: Optional[TableId]

    name: str
    external_id: str
    depth: int

    grouper_ids: Sequence[TableId]
    fields: Sequence[Field]

    total_group_count: int
    group_count: int

    total_field_count: int
    field_count: int


@memory.cache(ignore=["cursor"])  # type: ignore
def getFieldGroupSummaries(
    cursor: Any,
) -> Dict[TableId, FieldGroupSummary]:
    stmt = """
  with recursive descendent as (
    select *,
           0 as depth
    from test_demeter.grouper G
    where parent_grouper_id is null
    UNION ALL
    select G.*,
           depth + 1
    from descendent D
    join test_demeter.grouper G on D.grouper_id = G.parent_grouper_id

  ), fields as (
    select D.grouper_id,
           coalesce(
             jsonb_agg(to_jsonb(F.*))
               filter
               (where F.field_id is not null),
             '[]'
           ) as fields
    from descendent D
    left join test_demeter.field F on D.grouper_id = F.grouper_id
    group by D.grouper_id

  ), ancestor_counts as (
    select D.grouper_id as ancestor_grouper_id,
           D.parent_grouper_id,
           D.grouper_id,
           jsonb_array_length(F.fields) as field_count
    from descendent D
    join fields F on D.grouper_id = F.grouper_id
    where not exists (
      select *
      from descendent D2
      where D2.parent_grouper_id = D.grouper_id
    )
    UNION ALL
    select D.parent_grouper_id as ancestor_grouper_id,
           D.parent_grouper_id,
           D.grouper_id,
           AC.field_count
           from ancestor_counts AC
           join descendent D on AC.ancestor_grouper_id = D.grouper_id
           join fields F on D.grouper_id = F.grouper_id
  ), aggregate_counts as (
    select ancestor_grouper_id as grouper_id,
           sum(AC.field_count) filter (where ancestor_grouper_id = grouper_id) as field_count,

           sum(field_count) as total_field_count,
           count(*) as total_group_count
    from ancestor_counts AC
    group by ancestor_grouper_id

  ) select D.*,
           F.fields as fields,
           grouper_ids,
           AC.total_group_count,
           jsonb_array_length(grouper_ids) as group_count,
           AC.total_field_count,
           AC.field_count as field_count
    from descendent D
    join aggregate_counts AC on D.grouper_id = AC.grouper_id
    join fields F on D.grouper_id = F.grouper_id
    left join lateral (
      select coalesce(
               jsonb_agg(D2.grouper_id)
                 filter
                 (where D2.grouper_id is not null),
              '[]'
             ) as grouper_ids
      from descendent D2
      where D.grouper_id = D2.parent_grouper_id
      group by D.grouper_id
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
        r.grouper_id: FieldGroupSummary(
            grouper_id=r.grouper_id,
            parent_grouper_id=r.parent_grouper_id,
            name=r.name,
            external_id=r.external_id,
            depth=r.depth,
            grouper_ids=r.grouper_ids or [],
            fields=r.fields or [],
            total_group_count=r.total_group_count or 0,
            group_count=r.group_count or 0,
            total_field_count=r.total_field_count or 0,
            field_count=r.field_count or 0,
        )
        for r in results
    }
