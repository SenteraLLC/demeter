from typing import Any, List, Optional, Sequence, Dict, Set, Tuple, NamedTuple

from ... import db

from .generated import g, getMaybeFieldGroupId, insertFieldGroup, getFieldGroup
from .types import FieldGroup

from collections import OrderedDict

from dataclasses import dataclass

from typing import NamedTuple

def getFieldGroupAncestors(cursor : Any,
                           field_group : FieldGroup,
                          ) -> OrderedDict[db.TableId, Sequence[FieldGroup]]:
  stmt = """
    with recursive leaf as (
      select G.parent_field_group_id,
             G.field_group_id
      from field_group G
      where G.field_group_id = %(field_group)s

    ), ancestry as (
      select L.field_group_id as leaf_id,
             L.parent_field_group_id,
             L.field_group_id,
             0 as distance
      from leaf L
      UNION ALL
      select A.leaf_id,
             FG.parent_field_group_id,
             FG.field_group_id,
             distance + 1
      from ancestry A
      join field_group FG on FG.field_group_id = A.parent_field_group_id
    ) select A.leaf_id as field_group_id,
             jsonb_agg(to_jsonb(FG.*) order by A.distance asc) as leaf_to_root
      from ancestry A
      join field_group FG on FG.field_group_id = A.field_group_id
      group by A.leaf_id
  """
  cursor.execute(stmt, field_group())
  results = cursor.fetchall()
  return OrderedDict((r["leaf_id"], r["leaf_to_root"]) for r in results)


def getOrgFields(cursor : Any,
                 field_group_id : db.TableId,
                ) -> Dict[db.TableId, Set[db.TableId]]:
  stmt = """
    with recursive ancestry as (
      select parent_field_group_id,
             field_group_id,
             0 as depth
      from field_group
      where field_group_id = %(field_group_id)s
      UNION ALL
      select F.parent_field_group_id,
             F.field_group_id,
             A.depth + 1
      from ancestry A
      join field_group F on F.parent_field_group_id = A.field_group_id

   ), leaf as (
     select A1.*
     from ancestry A1
     where not exists (select * from ancestry A2 where A2.parent_field_group_id = A1.field_group_id)

   ) select L.field_group_id as leaf_field_group_id,
            L.depth,
            coalesce(jsonb_agg(F.field_id) filter (where F.field_id is not null), '[]'::jsonb) as field_ids
     from leaf L
     left join field F on F.field_group_id = L.field_group_id
     group by L.parent_field_group_id, L.field_group_id;
  """
  cursor.execute(stmt, {'field_group_id' : field_group_id})
  results = cursor.fetchall()
  depths = {r["depth"] for r in results}
  return { r.leaf_field_group_id : r.field_ids for r in results}


from datetime import datetime
from .types import Field, FieldGroup

# TODO: How to deal with Demeter table classes vs Demeter table result classes? (IE w/ and w/o TableId)
@dataclass(frozen=True)
class FieldSummary(db.Detailed):
  field_id    : db.TableId
  geom_id     : db.TableId
  name : str
  external_id    : Optional[str]
  field_group_id : Optional[db.TableId]
  created     : Optional[datetime] = None


@dataclass(frozen=True)
class FieldGroupFields():
  fields_by_depth : Dict[int, Sequence[FieldSummary]]
  ancestors : Sequence[FieldGroup]


def getFieldGroupFields(cursor : Any,
                        field_group_ids : Sequence[db.TableId],
                       ) -> OrderedDict[db.TableId, FieldGroupFields]:
  stmt = """
  with recursive ancestor as (
     select *,
            0 as distance
     from field_group
     where field_group_id = any(%(field_group_ids)s::bigint[])
     UNION ALL
     select G.*,
            distance + 1
     from ancestor A
     join field_group G on A.parent_field_group_id = G.field_group_id

  ), descendant as (
    select field_group_id as root_field_group_id,
           *,
           (select max(distance) from ancestor) as depth
    from ancestor
    where distance = 0
    UNION ALL
    select G.field_group_id as root_field_group_id,
           G.*,
           D.distance,
           depth + 1
    from descendant D
    join field_group G on D.field_group_id = G.parent_field_group_id

  ), field_group_fields as (
    select D.root_field_group_id,
           D.depth,
           coalesce(
             jsonb_agg(
               to_jsonb(F) order by F.last_updated desc
             ) filter(where F is not null),
             '[]'::jsonb
           ) as fields
    from descendant D
    left join field F on D.field_group_id = F.field_group_id
    group by D.root_field_group_id, D.depth

  ) select F.root_field_group_id as field_group_id,
           jsonb_object_agg(
             F.depth,
             F.fields
           ) as fields_by_depth,
           jsonb_agg(to_jsonb(A) order by A.field_group_id asc) as ancestors
           from field_group_fields F
           join ancestor A on A.field_group_id = F.root_field_group_id
           group by F.root_field_group_id
  """

  cursor.execute(stmt, {'field_group_ids' : field_group_ids})
  results = cursor.fetchall()

  return OrderedDict(
      (db.TableId(r.field_group_id),
       FieldGroupFields(
         fields_by_depth = r.fields_by_depth,
         ancestors = r.ancestors
       )
      )
      for r in results
  )


def searchFieldGroup(cursor : Any,
                     field_group : FieldGroup,
                     do_fuzzy_search : bool = False,
                    ) -> Sequence[FieldGroup]:
  search_part = "where name = %(name)s"
  if do_fuzzy_search:
    search_part = "where name like concat('%', %(name)s, '%')"

  stmt = f"""
    with candidate as (
      select *
      from field_group
      {search_part}

    ) select * from candidate;
  """
  cursor.execute(stmt, field_group())
  results = cursor.fetchall()
  return [r for r in results]
