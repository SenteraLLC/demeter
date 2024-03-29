from collections import OrderedDict
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
    Type,
    cast,
)

from psycopg2 import sql
from psycopg2.sql import (
    SQL,
    Composed,
    Identifier,
    Placeholder,
)

from demeter.db import TableId
from demeter.db._generic_types import (
    SK,
    GetId,
    GetTableByKey,
    I,
    ReturnId,
    ReturnKey,
    S,
)
from demeter.db._postgres.helpers import is_none, is_optional
from demeter.db._postgres.tools import doPgFormat, doPgJoin
from demeter.db._union_types import (
    AnyIdTable,
    AnyKeyTable,
    AnyTable,
)


# TODO: Add options for 'is_none' and 'is_optional'
def generateInsertStmt(
    table_name: str,
    table: AnyTable,
    return_key: Optional[Sequence[str]],
) -> Composed:
    stmt_template = "insert into {table} ({fields}) values({places})"

    names_to_fields = OrderedDict(
        {
            name: Identifier(name)
            for name in table.names()
            if not (is_optional(table, name) and is_none(table, name))
        }
    )

    to_interpolate: Dict[str, Any] = {
        "table": Identifier(table_name),
        "fields": doPgJoin(",", tuple(names_to_fields.values())),
        "places": doPgJoin(",", [Placeholder(n) for n in names_to_fields]),
    }
    if return_key is not None:
        key_names = [Identifier(k) for k in return_key]
        stmt_template += " returning {returning}"
        returning = doPgJoin(",", key_names)
        to_interpolate["returning"] = returning
    stmt = doPgFormat(stmt_template, **to_interpolate)

    return stmt


def insertAndReturnId(
    table_name: str,
    cursor: Any,
    table: AnyIdTable,
) -> TableId:
    table_id = table_name + "_id"
    return_key = [table_id]
    stmt = generateInsertStmt(table_name, table, return_key)
    cursor.execute(stmt, table())
    result = cursor.fetchone()
    return TableId(result[0])


def insertAndReturnKey(
    table_name: str,
    key: Type[SK],
) -> ReturnKey[S, SK]:
    return_key = [f for f in key.names()]

    def __impl(
        cursor: Any,
        table: AnyKeyTable,
    ) -> SK:
        stmt = generateInsertStmt(table_name, table, return_key)
        t = table()
        cursor.execute(stmt, t)
        result = cast(SK, cursor.fetchone())
        return result

    return __impl


def insertOrGetId(
    get_id: GetId[I],
    return_id: ReturnId[I],
    cursor: Any,
    table: I,
) -> TableId:
    maybe_type_id = get_id(cursor, table)
    if maybe_type_id is not None:
        return maybe_type_id
    return return_id(cursor, table)


def insertOrGetKey(
    get_by_key: GetTableByKey[SK, S],
    return_key: ReturnKey[S, SK],
    key_type: Type[SK],
    cursor: Any,
    table: S,
) -> SK:
    key_fields = set(key_type.__dataclass_fields__.keys())
    key_parts = {k: v for k, v in table().items() if k in key_fields}
    key = key_type(**key_parts)
    if get_by_key(cursor, key) is not None:
        return key
    k = return_key(cursor, table)
    return k


def generateInsertMany(
    table_name: str,
    field_names: Sequence[str],
    number_inserts: int,
) -> Composed:
    placeholder = doPgJoin(
        "", [SQL("("), doPgJoin(", ", [Placeholder()] * len(field_names)), SQL(")")]
    )
    values = doPgJoin(", ", [placeholder] * number_inserts)
    fields = doPgJoin(", ", tuple(map(sql.Identifier, field_names)))

    stmt = doPgFormat(
        "insert into {table_name} ({fields}) values{values}",
        table_name=Identifier(table_name),
        fields=fields,
        values=values,
    )
    return stmt
