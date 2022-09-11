from typing import Any, List

from ... import db

from .generated import g, getMaybeFieldGroupId, insertFieldGroupStrict
from .types import FieldGroup


def insertFieldGroup(cursor : Any,
                     field_group : FieldGroup,
                    ) -> db.TableId:
  stmt = """
    with recursive ancestry as (
      select field_group_id, parent_field_group_id, 0 as i
      from field_group
      where name = %(name)s
      UNION ALL
      select G.field_group_id, G.parent_field_group_id, i + 1
      from field_group G
      join ancestry A on G.field_group_id = A.parent_field_group_id
    ) select A1.parent_field_group_id
      from ancestry A1, ancestry A2
      where A1.i = 0 and
            A2.i > A1.i and
            A2.field_group_id = %(parent_field_group_id)s;
  """
  cursor.execute(stmt, field_group())
  results = cursor.fetchall()

  if len(results) > 1:
    ids = {r for r in results}
    raise Exception(f"Ambiguous ancestry: {ids}")
  elif len(results) == 1:
    parent_group_id = results[0]
    return db.TableId(parent_group_id)
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



