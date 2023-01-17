from collections import OrderedDict
from typing import (
    Any,
    NamedTuple,
    Optional,
    Tuple,
    cast,
)

from psycopg2.sql import Identifier, Placeholder

from .. import TableId
from .._generic_types import SK, S
from .._union_types import AnyIdTable
from .helpers import is_optional
from .tools import doPgFormat, doPgJoin


def getMaybeId(
    table_name: str,
    cursor: Any,
    table: AnyIdTable,
) -> Optional[TableId]:
    field_names = table.names()
    names_to_fields = OrderedDict({name: Identifier(name) for name in field_names})

    conditions = [
        doPgJoin(" = ", [Identifier(n), Placeholder(n)])
        for n in names_to_fields
        if not is_optional(table, n)
    ]

    table_id = "_".join([table_name, "id"])
    stmt = doPgFormat(
        "select {0} from {1} where {2}",
        Identifier(table_id),
        Identifier(table_name),
        doPgJoin(" and ", conditions),
    )
    args = table()
    cursor.execute(stmt, args)
    result = cursor.fetchone()
    if result is not None:
        return TableId(result[0])
    return None


def getMaybeTable(
    table_name: str,
    table_id_name: str,
    table_id: TableId,
    cursor: Any,
) -> Optional[NamedTuple]:
    condition = doPgJoin(" = ", [Identifier(table_id_name), Placeholder(table_id_name)])
    stmt = doPgFormat(
        "select * from {0} where {1}",
        Identifier(table_name),
        condition,
    )
    cursor.execute(stmt, {table_id_name: table_id})
    result = cursor.fetchone()
    if result is not None:
        return cast(NamedTuple, result)
    return None


def getMaybeTableByKey(
    table_name: str,
    cursor: Any,
    key: SK,
) -> Optional[Tuple[SK, S]]:
    key_parts = set(key.__dataclass_fields__.keys())
    conditions = [doPgJoin(" = ", [Identifier(k), Placeholder(k)]) for k in key_parts]
    stmt = doPgFormat(
        "select * from {0} where {1}",
        Identifier(table_name),
        doPgJoin(" and ", conditions),
    )
    cursor.execute(stmt, key())
    result = cursor.fetchone()
    if result is not None:
        return key, cast(S, result)
    return None
