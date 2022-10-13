from typing import Any, List, Optional, Sequence, Dict, Set, Tuple, NamedTuple

from ... import db

from .generated import g, getMaybeFieldGroupId, insertFieldGroup, getFieldGroup
from .types import FieldGroup

from collections import OrderedDict

def getFieldGroupAncestors(cursor : Any,
                           field_group : FieldGroup,
                          ) -> OrderedDict[db.TableId, Sequence[FieldGroup]]:
  stmt = """
    with recursive leaf as (
      select G.parent_field_group_id,
             G.field_group_id
      from test_mlops.field_group G
      where G.field_group_id = 64299

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
      join test_mlops.field_group FG on FG.field_group_id = A.parent_field_group_id
    ) select A.leaf_id,
             jsonb_agg(to_jsonb(FG.*) order by A.distance asc) as leaf_to_root
      from ancestry A
      join test_mlops.field_group FG on FG.field_group_id = A.field_group_id
      group by A.leaf_id
  """
  cursor.execute(stmt, field_group())
  results = cursor.fetchall()
  return OrderedDict((r["leaf_id"], r["leaf_to_root"]) for r in results)


def insertFieldGroupGreedy(cursor : Any,
                           field_group : FieldGroup,
                          ) -> db.TableId:
  id_to_ancestors = getFieldGroupAncestors(cursor,
                                           field_group,
                                          )
  ancestors = id_to_ancestors[field_group.field_group_id]
  if ancestors is None:
    raise Exception(f"Ancestors not found for Field Group: {field_group}")

  p_id = field_group.parent_field_group_id

  maybe_result = None
  for r in ancestors:
    if p_id is not None and p_id in r.lineage:
      if maybe_result is not None:
        raise Exception(f"Ambiguous field group: {field_group}")
      maybe_result = r

  if (result := maybe_result) is not None:
    l = result.lineage
    existing = l[-1] if len(l) else None
    if existing != p_id:
      print(f"Found more recent parent field group: {existing} is more recent than {p_id} [{l}]")
      # TODO: OK???
    field_group.parent_field_group_id = existing # type: ignore
  else:
    raise Exception(f"Bad field group search: {field_group}")

  if p_id and (getFieldGroup(cursor, p_id) is None):
    raise Exception(f"Bad parent field group id for: {field_group}")

  return insertFieldGroup(cursor, field_group)


insertOrGetFieldGroupGreedy = g.partialInsertOrGetId(getMaybeFieldGroupId, insertFieldGroupGreedy)



def getOrgFields(cursor : Any,
                 field_group_id : db.TableId,
                ) -> Dict[db.TableId, Set[db.TableId]]:
  stmt = """
    with recursive ancestry as (
      select parent_field_group_id,
             field_group_id,
             0 as depth
      from test_mlops.field_group
      where field_group_id = %(field_group_id)s
      UNION ALL
      select F.parent_field_group_id,
             F.field_group_id,
             A.depth + 1
      from ancestry A
      join test_mlops.field_group F on F.parent_field_group_id = A.field_group_id

   ), leaf as (
     select A1.*
     from ancestry A1
     where not exists (select * from ancestry A2 where A2.parent_field_group_id = A1.field_group_id)

   ) select L.field_group_id as leaf_field_group_id,
            L.depth,
            coalesce(jsonb_agg(F.field_id) filter (where F.field_id is not null), '[]'::jsonb) as field_ids
     from leaf L
     left join test_mlops.field F on F.field_group_id = L.field_group_id
     group by L.parent_field_group_id, L.field_group_id;
  """
  cursor.execute(stmt, {'field_group_id' : field_group_id})
  results = cursor.fetchall()
  depths = {r["depth"] for r in results}
  return { r.leaf_field_group_id : r.field_ids for r in results}



from typing import TypedDict
from typing import cast

from .types import Field, FieldGroup

class FieldGroupAncestry(TypedDict):
  fields_by_depth : Dict[int, Sequence[Field]]
  ancestors : Sequence[FieldGroup]


def getFieldsByFieldGroup(cursor : Any,
                          field_group_id : db.TableId,
                         ) -> FieldGroupAncestry:
  stmt = """
  with recursive ancestor as (
     select *,
            0 as distance
     from test_mlops.field_group
     where field_group_id = %(field_group_id)s
     UNION ALL
     select G.*,
            distance + 1
     from ancestor A
     join test_mlops.field_group G on A.parent_field_group_id = G.field_group_id

  ), descendant as (
    select *,
           (select max(distance) from ancestor) as depth
    from ancestor
    where distance = 0
    UNION ALL
    select G.*,
           D.distance,
           depth + 1
    from descendant D
    join test_mlops.field_group G on D.field_group_id = G.parent_field_group_id

  ) select jsonb_build_object(
             D.depth,
             coalesce(
               jsonb_agg(
                 to_jsonb(F) order by F.last_updated desc
               ),
               '[]'::jsonb
             )
           ) as fields_by_depth,
           jsonb_agg(to_jsonb(A) order by A.field_group_id asc) as ancestors
    from descendant D
    left join test_mlops.field F on D.field_group_id = F.field_group_id
    cross join ancestor A
    group by D.depth
  """

  cursor.execute(stmt, {'field_group_id' : field_group_id})
  results = cursor.fetchall()
  return cast(FieldGroupAncestry, results)



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



