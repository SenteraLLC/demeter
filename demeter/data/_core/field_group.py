from typing import Any, List, Optional, Sequence, Dict, Set

from ... import db

from .generated import g, getMaybeFieldGroupId, insertFieldGroup
from .types import FieldGroup


from dataclasses import dataclass

@dataclass
class FieldNameSearchResult:
  name : str
  external_id : Optional[str]
  field_group_id : db.TableId
  lineage : Sequence[db.TableId]



def searchFieldName(cursor : Any,
                    field_group : FieldGroup,
                   ) -> Sequence[FieldNameSearchResult]:
  stmt = """
    with recursive leaf as (
      select parent_field_group_id as parent_id,
             field_group_id,
             field_group_id as leaf_id
      from field_group
      where name = %(name)s

    ), ancestry as (
      select * from leaf
      UNION ALL
      select G.parent_field_group_id as parent_id,
             G.field_group_id,
             A.leaf_id
      from field_group G
      join ancestry A on G.field_group_id = A.parent_id

    ), ancestry_with_root as (
      select A.field_group_id as root_id,
             A.parent_id,
             A.field_group_id,
             A.leaf_id,
             0 as depth
      from ancestry A
      where A.parent_id is null
      UNION ALL
      select AWR.root_id,
             A.parent_id,
             A.field_group_id,
             A.leaf_id,
             depth + 1
      from ancestry_with_root AWR
      join ancestry A on AWR.field_group_id = A.parent_id and
                         AWR.leaf_id = A.leaf_id

    ), root_and_leaf as (
      select A.root_id,
             A.leaf_id,
             jsonb_agg(A.field_group_id order by A.depth asc) as lineage
      from ancestry_with_root A
      group by A.root_id, A.leaf_id

    ) select RF.lineage, to_jsonb(FG.*) as group
      from root_and_leaf RF
      join field_group FG on RF.leaf_id = FG.field_group_id
  """
  cursor.execute(stmt, field_group())
  results = cursor.fetchall()
  return [r for r in results]


def insertFieldGroupGreedy(cursor : Any,
                           field_group : FieldGroup,
                          ) -> db.TableId:
  search_results = searchFieldName(cursor,
                                   field_group,
                                  )
  p_id = field_group.parent_field_group_id

  maybe_result = None
  for r in search_results:
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

  return insertFieldGroup(cursor, field_group)


insertOrGetFieldGroupGreedy = g.partialInsertOrGetId(getMaybeFieldGroupId, insertFieldGroupGreedy)


def getFieldHeirarchy(cursor : Any,
                      field_id : db.TableId,
                     ) -> List[db.TableId]:
  stmt = """
    with recursive field_groups as (
      select field_id, G.field_group_id, G.parent_field_group_id, 0 as level
      from field F
      left join field_group G on coalesce(F.field_group_id,
                                          F.farm_id,
                                          F.grower_id
                                         ) = G.field_group_id
      where field_id = %(field_id)s
      UNION ALL
      select field_id, RG.parent_field_group_id as field_group_id, G.parent_field_group_id, level + 1
      from field_group G
      join field_groups RG on RG.parent_field_group_id = G.field_group_id
    ) select field_id, array_agg(field_group_id order by level asc) as root_to_leaf
      from field_groups group by field_id;
  """
  cursor.execute(stmt, {'field_id' : field_id})
  results = cursor.fetchall()
  return [db.TableId(i) for i in results.root_to_leaf]



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


