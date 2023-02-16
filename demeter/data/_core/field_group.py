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
class FieldGroup(db.Detailed):
    """Arbitrary collection of Field objects or other FieldGroup
    objects which allows demeter to represent field organization
    schemes for any customer."""

    name: str
    parent_field_group_id: Optional[db.TableId] = None


def _row_to_field_group(
    row: Dict[str, Any],
    id_name: str = "field_group_id",
) -> Tuple[db.TableId, FieldGroup]:
    """Takes a row of "field_group" table and returns field group ID and FieldGroup object.

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

    f = FieldGroup(
        name=r["name"],
        parent_field_group_id=r[parent_id_name],
        last_updated=r["last_updated"],
        details=r["details"],
    )
    return (_id, f)


def getFieldGroupAncestors(
    cursor: Any,
    field_group_id: db.TableId,
) -> DataFrame:
    """Takes a `field_group_id` value and returns a dataframe of that FieldGroup's ancestors
    sorted by their distance from the given child."""
    stmt = """
    with recursive ancestry as (
      select root.*,
             0 as distance
      from field_group root
      where root.field_group_id = %(field_group_id)s
      UNION ALL
      select ancestor.*,
             distance + 1
      from ancestry descendant, field_group ancestor
      where descendant.parent_field_group_id = ancestor.field_group_id
    )
    select * from ancestry
    order by distance
    """
    cursor.execute(stmt, {"field_group_id": field_group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get field group ancestors for: {field_group_id}")

    df_results = DataFrame(results)
    ancestors = DataFrame(columns=["distance", "field_group_id", "field_group"])
    for _, row in df_results.iterrows():
        dist = row["distance"]
        fg_id, fg = _row_to_field_group(row.to_dict())

        this_data = {"distance": [dist], "field_group_id": [fg_id], "field_group": [fg]}
        ancestors = pd_concat(
            [ancestors, DataFrame(this_data)], ignore_index=True, axis=0
        )

    return ancestors


def getFieldGroupDescendants(
    cursor: Any,
    field_group_id: db.TableId,
) -> DataFrame:
    """Takes a `field_group_id` value and returns a dataframe of that FieldGroup's descendants
    sorted by their distance from the given parent."""
    stmt = """
    with recursive descendants as (
      select root.*,
             0 as distance
      from field_group root
      where root.field_group_id = %(field_group_id)s
      UNION ALL
      select descendant.*,
             distance + 1
      from descendants ancestor, field_group descendant
      where ancestor.field_group_id = descendant.parent_field_group_id
    )
    select * from descendants
    order by distance
    """
    cursor.execute(stmt, {"field_group_id": field_group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get field group descendants for: {field_group_id}")

    df_results = DataFrame(results)
    descendants = DataFrame(columns=["distance", "field_group_id", "field_group"])
    for _, row in df_results.iterrows():
        dist = row["distance"]
        fg_id, fg = _row_to_field_group(row.to_dict())

        this_data = {"distance": [dist], "field_group_id": [fg_id], "field_group": [fg]}
        descendants = pd_concat(
            [descendants, DataFrame(this_data)], ignore_index=True, axis=0
        )

    return descendants


def _row_to_field(
    row: Dict[str, Any],
) -> Tuple[db.TableId, FieldGroup]:
    r = row
    fld = Field(
        name=r["name"],
        geom_id=r["geom_id"],
        date_start=r["date_start"],
        date_end=r["date_end"],
        field_group_id=r["field_group_id"],
        details=r["details"],
        last_updated=r["last_updated"],
    )
    return fld


def getFieldGroupFields(
    cursor: Any,
    field_group_id: db.TableId,
    include_descendants: bool = True,
) -> DataFrame:
    """Takes a `field_group_id` value and returns a dataframe of all of the fields which
    directly belong to that FieldGroup if `include_descendants` = False or belong to the FieldGroup or
    one of its child organizations if `include_descendants` = True (default behavior).
    """
    stmt_descendants_true = """
    with recursive descendants as (
      select root.*
      from field_group root
      where root.field_group_id = %(field_group_id)s
      UNION ALL
      select descendant.*
      from descendants ancestor, field_group descendant
      where ancestor.field_group_id = descendant.parent_field_group_id
    )
    select * from field
    where field_group_id in (select field_group_id from descendants)
    """

    stmt_descendants_false = """
    select * from field
    where field_group_id = %(field_group_id)s
    """

    if include_descendants:
        cursor.execute(stmt_descendants_true, {"field_group_id": field_group_id})
    else:
        cursor.execute(stmt_descendants_false, {"field_group_id": field_group_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get fields for FieldGroup: {field_group_id}")

    df_results = DataFrame(results)
    fields = DataFrame(columns=["field_id", "field"])
    for _, row in df_results.iterrows():
        fld = _row_to_field(row.to_dict())

        this_data = {"field_id": [row["field_id"]], "field": [fld]}
        fields = pd_concat([fields, DataFrame(this_data)], ignore_index=True, axis=0)

    return fields


# def searchFieldGroup(
#     cursor: Any,
#     field_group_name: str,
#     parent_field_group_id: Optional[db.TableId] = None,
#     ancestor_field_group_id: Optional[db.TableId] = None,
#     do_fuzzy_search: bool = False,
# ) -> Optional[Tuple[db.TableId, FieldGroup]]:
#     search_part = "where name = %(name)s"
#     if do_fuzzy_search:
#         search_part = "where name like concat('%', %(name)s, '%')"

#     stmt = f"""
#     with candidate as (
#       select *
#       from field_group
#       {search_part}

#     ) select * from candidate;
#     """
#     args: Dict[str, Any] = {"name": field_group_name}
#     cursor.execute(stmt, args)
#     results = cursor.fetchall()

#     maybe_result: Optional[Tuple[db.TableId, FieldGroup]] = None
#     for r in results:
#         _id = r["field_group_id"]
#         f = FieldGroup(
#             field_group_id=r["field_group_id"],
#             parent_field_group_id=r["parent_field_group_id"],
#             name=r["name"],
#             details=r["details"],
#             last_updated=r["last_updated"],
#         )

#         if (p_id := parent_field_group_id) or (a_id := ancestor_field_group_id):
#             ancestors = getFieldGroupAncestors(cursor, _id)
#             ancestor_ids = [a[0] for a in ancestors]
#             if p_id is not None:
#                 if p_id != ancestor_ids[0]:
#                     continue
#             if a_id is not None:
#                 if a_id not in ancestor_ids:
#                     continue

#         if maybe_result is not None:
#             raise Exception(
#                 f"Ambiguous field group search: {field_group_name},{p_id},{a_id}"
#             )

#         _id = r["field_group_id"]
#         maybe_result = (_id, f)

#     return maybe_result
