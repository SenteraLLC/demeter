from dataclasses import dataclass
from typing import (  # Set,
    Any,
    Dict,
    Optional,
    Tuple,
)

from pandas import DataFrame
from pandas import concat as pd_concat

from demeter import db
from demeter.data._core.types import Field


@dataclass(frozen=True)
class Grouper(db.Detailed):
    """Arbitrary collection of Field, FieldTrial, plot, or other Grouper objects which allows demeter to represent any
    grouping of objects, which allows for a flexible organization scheme across customers.
    """

    name: str
    parent_group_id: Optional[db.TableId] = None


def _row_to_grouper(
    row: Dict[str, Any],
    id_name: str = "group_id",
) -> Tuple[db.TableId, Grouper]:
    """Takes a row of "grouper" table and returns field group ID and Grouper object.

    Row of table is given as a dictionary, which must contain the following keys:
    `id_name`, "parent_+`id_name`", "last_updated", and "details". Since this function
    is typically applied to rows from the Demeter table, these values are typically
    always available, so are NULL if no value exists."""

    msg1 = f"'{id_name}' is not a key within `row`"
    assert id_name in row.keys(), msg1

    r = row
    _id = r[id_name]
    parent_id_name = "_".join(["parent", id_name])

    msg2 = f"'{parent_id_name}' is not a key within `row`"
    assert parent_id_name in row.keys(), msg2

    f = Grouper(
        name=r["name"],
        parent_group_id=r[parent_id_name],
        last_updated=r["last_updated"],
        details=r["details"],
    )
    return (_id, f)


def getGrouperAncestors(
    cursor: Any,
    group_id: db.TableId,
) -> DataFrame:
    """Takes a `group_id` value and returns a dataframe of that Grouper's ancestors
    sorted by their distance from the given child."""
    stmt = """
    with recursive ancestry as (
      select root.*,
             0 as distance
      from grouper root
      where root.group_id = %(group_id)s
      UNION ALL
      select ancestor.*,
             distance + 1
      from ancestry descendant, grouper ancestor
      where descendant.parent_group_id = ancestor.group_id
    )
    select * from ancestry
    order by distance
    """
    cursor.execute(stmt, {"group_id": group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get field group ancestors for: {group_id}")

    df_results = DataFrame(results)
    ancestors = DataFrame(columns=["distance", "group_id", "grouper"])
    for _, row in df_results.iterrows():
        dist = row["distance"]
        fg_id, fg = _row_to_grouper(row.to_dict())

        this_data = {"distance": [dist], "group_id": [fg_id], "grouper": [fg]}
        ancestors = pd_concat(
            [ancestors, DataFrame(this_data)], ignore_index=True, axis=0
        )

    return ancestors


def getGrouperDescendants(
    cursor: Any,
    group_id: db.TableId,
) -> DataFrame:
    """Takes a `group_id` value and returns a dataframe of that Grouper's descendants
    sorted by their distance from the given parent."""
    stmt = """
    with recursive descendants as (
      select root.*,
             0 as distance
      from grouper root
      where root.group_id = %(group_id)s
      UNION ALL
      select descendant.*,
             distance + 1
      from descendants ancestor, grouper descendant
      where ancestor.group_id = descendant.parent_group_id
    )
    select * from descendants
    order by distance
    """
    cursor.execute(stmt, {"group_id": group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get field group descendants for: {group_id}")

    df_results = DataFrame(results)
    descendants = DataFrame(columns=["distance", "group_id", "grouper"])
    for _, row in df_results.iterrows():
        dist = row["distance"]
        fg_id, fg = _row_to_grouper(row.to_dict())

        this_data = {"distance": [dist], "group_id": [fg_id], "grouper": [fg]}
        descendants = pd_concat(
            [descendants, DataFrame(this_data)], ignore_index=True, axis=0
        )

    return descendants


def _row_to_field(
    row: Dict[str, Any],
) -> Tuple[db.TableId, Grouper]:
    r = row
    fld = Field(
        name=r["name"],
        geom_id=r["geom_id"],
        date_start=r["date_start"],
        date_end=r["date_end"],
        group_id=r["group_id"],
        details=r["details"],
        last_updated=r["last_updated"],
    )
    return fld


def getGrouperFields(
    cursor: Any,
    group_id: db.TableId,
    include_descendants: bool = True,
) -> DataFrame:
    """Takes a `group_id` value and returns a dataframe of all of the fields which
    directly belong to that Grouper if `include_descendants` = False or belong to the Grouper or
    one of its child organizations if `include_descendants` = True (default behavior).
    """
    stmt_descendants_true = """
    with recursive descendants as (
      select root.*
      from grouper root
      where root.group_id = %(group_id)s
      UNION ALL
      select descendant.*
      from descendants ancestor, grouper descendant
      where ancestor.group_id = descendant.parent_group_id
    )
    select * from field
    where group_id in (select group_id from descendants)
    """

    stmt_descendants_false = """
    select * from field
    where group_id = %(group_id)s
    """

    if include_descendants:
        cursor.execute(stmt_descendants_true, {"group_id": group_id})
    else:
        cursor.execute(stmt_descendants_false, {"group_id": group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get fields for Grouper: {group_id}")

    df_results = DataFrame(results)
    fields = DataFrame(columns=["field_id", "field"])
    for _, row in df_results.iterrows():
        fld = _row_to_field(row.to_dict())

        this_data = {"field_id": [row["field_id"]], "field": [fld]}
        fields = pd_concat([fields, DataFrame(this_data)], ignore_index=True, axis=0)

    return fields


# def searchGrouper(
#     cursor: Any,
#     grouper_name: str,
#     parent_group_id: Optional[db.TableId] = None,
#     ancestor_group_id: Optional[db.TableId] = None,
#     do_fuzzy_search: bool = False,
# ) -> Optional[Tuple[db.TableId, Grouper]]:
#     search_part = "where name = %(name)s"
#     if do_fuzzy_search:
#         search_part = "where name like concat('%', %(name)s, '%')"

#     stmt = f"""
#     with candidate as (
#       select *
#       from grouper
#       {search_part}

#     ) select * from candidate;
#     """
#     args: Dict[str, Any] = {"name": grouper_name}
#     cursor.execute(stmt, args)
#     results = cursor.fetchall()

#     maybe_result: Optional[Tuple[db.TableId, Grouper]] = None
#     for r in results:
#         _id = r["group_id"]
#         f = Grouper(
#             group_id=r["group_id"],
#             parent_group_id=r["parent_group_id"],
#             name=r["name"],
#             details=r["details"],
#             last_updated=r["last_updated"],
#         )

#         if (p_id := parent_group_id) or (a_id := ancestor_group_id):
#             ancestors = getGrouperAncestors(cursor, _id)
#             ancestor_ids = [a[0] for a in ancestors]
#             if p_id is not None:
#                 if p_id != ancestor_ids[0]:
#                     continue
#             if a_id is not None:
#                 if a_id not in ancestor_ids:
#                     continue

#         if maybe_result is not None:
#             raise Exception(
#                 f"Ambiguous field group search: {grouper_name},{p_id},{a_id}"
#             )

#         _id = r["group_id"]
#         maybe_result = (_id, f)

#     return maybe_result
