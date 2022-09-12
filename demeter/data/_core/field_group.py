from typing import Any, List, Optional, Sequence

from ... import db

from .generated import g, getMaybeFieldGroupId, insertFieldGroupStrict
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
      UNION
      select parent_field_group_id as parent_id,
             field_group_id,
             field_group_id as leaf_id
      from field_group
      where field_group_id = %(parent_field_group_id)s

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


def insertFieldGroup(cursor : Any,
                     field_group : FieldGroup,
                    ) -> db.TableId:
  fg = field_group
  search_results = searchFieldName(cursor,
                                   field_group,
                                  )
  if len(search_results) > 1:
    raise Exception(f"Ambiguous ancestry: {search_results}")

  elif len(search_results) == 1:
    r = search_results[0]
    l = r.lineage
    existing = l[-1] if len(l) else None
    p_id = field_group.parent_field_group_id
    if existing != p_id:
      print(f"Found more recent parent field group: {existing} is more recent than {p_id} [{l}]")
      # TODO: OK???
      field_group.parent_field_group_id = existing # type: ignore

  elif fg.parent_field_group_id is not None:
    raise Exception(f"Bad parent group id: {fg.parent_field_group_id}")

  return insertFieldGroupStrict(cursor, field_group)


insertOrGetFieldGroup = g.partialInsertOrGetId(getMaybeFieldGroupId, insertFieldGroup)


def getGroupHeirarchy(cursor : Any,
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



