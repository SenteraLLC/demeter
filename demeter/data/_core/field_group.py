from typing import Any, Sequence, Dict, Set, Optional

from ... import db

from .types import Field, ParcelGroup
from .generated import insertParcelGroup

from collections import OrderedDict

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class FieldGroup:
    field_group_id: db.TableId
    name: str
    parent_field_group_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


def make_field_group(
    cursor: Any,
    name: str,
    parent_field_group_id: Optional[db.TableId] = None,
    created: Optional[datetime] = None,
) -> FieldGroup:
    n = name
    p = parent_field_group_id
    c = created
    parcel_group = ParcelGroup(
        name=n,
        parent_parcel_group_id=p,
        created=c,
    )
    maybe_field_group_id = insertParcelGroup(cursor, parcel_group)
    if (f := maybe_field_group_id) is not None:
        return FieldGroup(
            name=n,
            field_group_id=f,
            created=c,
        )


# TODO: Fix the inherited type, it shouldnt be a db.Table


@dataclass(frozen=True)
class FieldGroupFields(db.Table):
    fields_by_depth: Dict[int, Sequence[Field]]
    ancestors: Sequence[FieldGroup]


def getFieldGroupAncestors(
    cursor: Any,
    field_group: FieldGroup,
) -> OrderedDict[db.TableId, Sequence[FieldGroup]]:
    stmt = """
    with recursive leaf as (
      select G.parent_parcel_group_id,
             G.parcel_group_id
      from test_mlops.parcel_group G
      where G.parcel_group_id = %(parcel_group_id)s

    ), ancestry as (
      select L.parcel_group_id as leaf_id,
             L.parent_parcel_group_id,
             L.parcel_group_id,
             0 as distance
      from leaf L
      UNION ALL
      select A.leaf_id,
             FG.parent_parcel_group_id,
             FG.parcel_group_id,
             distance + 1
      from ancestry A
      join test_mlops.parcel_group FG on FG.parcel_group_id = A.parent_parcel_group_id
    ) select A.leaf_id as parcel_group_id,
             jsonb_agg(to_jsonb(FG.*) order by A.distance asc) as leaf_to_root
      from ancestry A
      join test_mlops.parcel_group FG on FG.parcel_group_id = A.parcel_group_id
      group by A.leaf_id
  """
    cursor.execute(stmt, {"parcel_group_id": field_group.field_group_id})
    results = cursor.fetchall()
    return OrderedDict((r["leaf_id"], r["leaf_to_root"]) for r in results)


def getOrgFields(
    cursor: Any,
    field_group_id: db.TableId,
) -> Dict[db.TableId, Set[db.TableId]]:
    stmt = """
    with recursive ancestry as (
      select parent_parcel_group_id,
             parcel_group_id,
             0 as depth
      from test_mlops.parcel_group
      where parcel_group_id = %(parcel_group_id)s
      UNION ALL
      select F.parent_parcel_group_id,
             F.parcel_group_id,
             A.depth + 1
      from ancestry A
      join test_mlops.parcel_group F on F.parent_parcel_group_id = A.parcel_group_id

   ), leaf as (
     select A1.*
     from ancestry A1
     where not exists (select * from ancestry A2 where A2.parent_parcel_group_id = A1.parcel_group_id)

   ) select L.parcel_group_id as leaf_parcel_group_id,
            L.depth,
            coalesce(jsonb_agg(F.parcel_id) filter (where F.parcel_id is not null), '[]'::jsonb) as parcel_ids
     from leaf L
     left join test_mlops.field F on F.parcel_group_id = L.parcel_group_id
     group by L.parent_parcel_group_id, L.parcel_group_id;
  """
    cursor.execute(stmt, {"parcel_group_id": field_group_id})
    results = cursor.fetchall()
    return {r.leaf_field_group_id: r.field_ids for r in results}


def getFields(
    cursor: Any,
    field_group_ids: Sequence[db.TableId],
) -> OrderedDict[db.TableId, FieldGroupFields]:
    stmt = """
  with recursive ancestor as (
     select *,
            0 as distance
     from test_mlops.parcel_group
     where parcel_group_id = any(%(parcel_group_ids)s::bigint[])
     UNION ALL
     select G.*,
            distance + 1
     from ancestor A
     join test_mlops.parcel_group G on A.parent_parcel_group_id = G.parcel_group_id

  ), descendant as (
    select parcel_group_id as root_parcel_group_id,
           *,
           (select max(distance) from ancestor) as depth
    from ancestor
    where distance = 0
    UNION ALL
    select G.parcel_group_id as root_parcel_group_id,
           G.*,
           D.distance,
           depth + 1
    from descendant D
    join test_mlops.parcel_group G on D.parcel_group_id = G.parent_parcel_group_id

  ), parcel_group_fields as (
    select D.root_parcel_group_id,
           D.depth,
           coalesce(
             jsonb_agg(
               to_jsonb(F) order by F.last_updated desc
             ) filter(where F is not null),
             '[]'::jsonb
           ) as fields
    from descendant D
    left join test_mlops.field F on D.parcel_group_id = F.parcel_group_id
    group by D.root_parcel_group_id, D.depth

  ) select F.root_parcel_group_id as parcel_group_id,
           jsonb_object_agg(
             F.depth,
             F.fields
           ) as fields_by_depth,
           jsonb_agg(to_jsonb(A) order by A.parcel_group_id asc) as ancestors
           from parcel_group_fields F
           join ancestor A on A.parcel_group_id = F.root_parcel_group_id
           group by F.root_parcel_group_id
  """

    cursor.execute(stmt, {"parcel_group_ids": field_group_ids})
    results = cursor.fetchall()

    return OrderedDict(
        (
            db.TableId(r.parcel_group_id),
            FieldGroupFields(fields_by_depth=r.fields_by_depth, ancestors=r.ancestors),
        )
        for r in results
    )


def searchFieldGroup(
    cursor: Any,
    field_group: FieldGroup,
    do_fuzzy_search: bool = False,
) -> Sequence[FieldGroup]:
    search_part = "where name = %(name)s"
    if do_fuzzy_search:
        search_part = "where name like concat('%', %(name)s, '%')"

    stmt = f"""
    with candidate as (
      select *
      from parcel_group
      {search_part}

    ) select * from candidate;
  """
    cursor.execute(stmt, {"name": field_group.name})
    results = cursor.fetchall()
    return [r for r in results]
