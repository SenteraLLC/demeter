from typing import Any, Optional, Sequence, Dict, Set, Tuple

from ... import db

from .generated import insertParcelGroup

from collections import OrderedDict
from datetime import datetime

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldGroup(db.Detailed):
    field_group_id: db.TableId
    name: str
    parent_field_group_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


# TODO: Temporary, replace with 'make_field_group' style functions
insertFieldGroup = insertParcelGroup


FieldGroupAncestors = Sequence[Tuple[db.TableId, FieldGroup]]


# TODO: Support multiple
def getFieldGroupAncestors(
    cursor: Any,
    field_group_id: db.TableId,
) -> FieldGroupAncestors:
    stmt = """
    with recursive field_group as (
      select FG.field_group_id as field_group_id,
             FG.parent_field_group_id,
             FG.field_group_id,
             0 as distance
      from field_group FG
      where FG.field_group_id = %(field_group_id)s

    ), ancestors as (
      select * from field_group FG
      UNION ALL
      select A.field_group_id,
             FG.parent_field_group_id,
             FG.field_group_id,
             distance + 1
      from ancestry A
      join field_group FG on FG.field_group_id = A.parent_field_group_id
    ) select A.field_group_id,
             jsonb_agg(to_jsonb(FG.*) order by A.distance asc) as ancestors,
      from ancestors A
      join descendents D on D.field_group_id = A.field_group_id
      join field_group FG on FG.field_group_id = A.field_group_id
      group by A.field_group_id
    """
    cursor.execute(stmt, {"field_group_id": field_group_id})
    results = cursor.fetchall()
    if len(results) != 1:
        raise Exception(f"Failed to get field group ancestors for: {field_group_id}")
    r = results[0]
    # _id = db.TableId(r["field_group_id"])
    ancestors = [_row_to_field_group(a) for a in r["ancestors"]]
    return ancestors


# TODO: Deprecated by 'getFieldGroupFields'
def getOrgFields(
    cursor: Any,
    field_group_id: db.TableId,
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
    cursor.execute(stmt, {"field_group_id": field_group_id})
    results = cursor.fetchall()
    return {r.leaf_field_group_id: r.field_ids for r in results}


# TODO: How to deal with Demeter table classes vs Demeter table result classes? (IE w/ and w/o TableId)


@dataclass(frozen=True)
class FieldSummary(db.Detailed):
    field_id: db.TableId
    geom_id: db.TableId
    name: str
    external_id: Optional[str]
    field_group_id: Optional[db.TableId]
    created: Optional[datetime] = None


@dataclass(frozen=True)
class FieldGroupFields:
    fields_by_depth: Dict[int, Sequence[FieldSummary]]
    ancestors: Sequence[FieldGroup]


def _row_to_field_group(
    row: Dict[str, Any],
    name: Optional[str] = None,
    id_name: str = "field_group_id",
) -> Tuple[db.TableId, FieldGroup]:
    r = row
    _id = r[id_name]
    parent_id_name = "_".join(["parent", id_name])
    f = FieldGroup(
        field_group_id=r["parcel_group_id"],
        name=r["name"],
        parent_field_group_id=r[parent_id_name],
        last_updated=r["last_updated"],
        details=r["details"],
    )
    return (_id, f)


def getFieldGroupFields(
    cursor: Any,
    field_group_ids: Sequence[db.TableId],
) -> OrderedDict[db.TableId, FieldGroupFields]:
    """Get the descendants of the provided field groups.
    Rename to 'get_descendant_fields' or 'get_descendants'?
    """
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

    cursor.execute(stmt, {"field_group_ids": field_group_ids})
    results = cursor.fetchall()

    return OrderedDict(
        (
            db.TableId(r.field_group_id),
            FieldGroupFields(fields_by_depth=r.fields_by_depth, ancestors=r.ancestors),
        )
        for r in results
    )


def searchFieldGroup(
    cursor: Any,
    field_group_name: str,
    parent_field_group_id: Optional[db.TableId] = None,
    ancestor_field_group_id: Optional[db.TableId] = None,
    do_fuzzy_search: bool = False,
) -> Optional[Tuple[db.TableId, FieldGroup]]:
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
    args: Dict[str, Any] = {"name": field_group_name}
    cursor.execute(stmt, args)
    results = cursor.fetchall()

    maybe_result: Optional[Tuple[db.TableId, FieldGroup]] = None
    for r in results:
        _id = r["field_group_id"]
        f = FieldGroup(
            field_group_id=r["parcel_group_id"],
            parent_field_group_id=r["parent_field_group_id"],
            name=r["name"],
            details=r["details"],
            last_updated=r["last_updated"],
        )

        if (p_id := parent_field_group_id) or (a_id := ancestor_field_group_id):
            ancestors = getFieldGroupAncestors(cursor, _id)
            ancestor_ids = [a[0] for a in ancestors]
            if p_id is not None:
                if p_id != ancestor_ids[0]:
                    continue
            if a_id is not None:
                if a_id not in ancestor_ids:
                    continue

        if maybe_result is not None:
            raise Exception(
                f"Ambiguous field group search: {field_group_name},{p_id},{a_id}"
            )

        _id = r["field_group_id"]
        maybe_result = (_id, f)

    return maybe_result
