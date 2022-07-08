from typing import Any, Union, Optional, Sequence, Dict, Type
from typing import cast

from collections import OrderedDict

from psycopg2 import sql
from psycopg2.sql import SQL, Composed, Identifier, Placeholder

from .. import TableId
from ..union_types import AnyTable, AnyIdTable, AnyKeyTable
from ..generic_types import ReturnKey, GetId, ReturnId, GetTableByKey
from ..generic_types import I, S, SK

from .helpers import is_none, is_optional
from .tools import PGJoin, PGFormat

# TODO: Add options for 'is_none' and 'is_optional'
def generateInsertStmt(table_name : str,
                       table      : AnyTable,
                       return_key : Optional[Sequence[str]],
                      ) -> Composed:
  stmt_template = "insert into {table} ({fields}) values({places})"

  names_to_fields = OrderedDict({name : Identifier(name)
                                 for name in table.names()
                                   if not (is_optional(table, name) and
                                      is_none(table, name))
                               })

  to_interpolate : Dict[str, Any] = {
    "table"  : Identifier(table_name),
    "fields" : PGJoin(",", tuple(names_to_fields.values())),
    "places" : PGJoin(",", [Placeholder(n) for n in names_to_fields]),
  }
  if return_key is not None:
    key_names = [Identifier(k) for k in return_key]
    stmt_template += " returning {returning}"
    returning = PGJoin(",", key_names)
    to_interpolate["returning"] = returning
  stmt = PGFormat(stmt_template, **to_interpolate)

  return stmt


def insertAndReturnId(table_name : str,
                      cursor     : Any,
                      table      : AnyIdTable,
                     ) -> TableId:
  table_id = table_name + "_id"
  return_key = [table_id]
  stmt = generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table())
  result = cursor.fetchone()
  return TableId(result[0])


def insertAndReturnKey(table_name : str,
                       key        : Type[SK],
                      ) -> ReturnKey[S, SK]:
  return_key = [f for f in key.names()]
  def __impl(cursor : Any,
             table  : AnyKeyTable,
            ) -> SK:
    stmt = generateInsertStmt(table_name, table, return_key)
    t = table()
    cursor.execute(stmt, t)
    result = cast(SK, cursor.fetchone())
    return result

  return __impl


def insertOrGetId(get_id    : GetId[I],
                  return_id : ReturnId[I],
                  cursor    : Any,
                  some_type : I,
                 ) -> TableId:
  maybe_type_id = get_id(cursor, some_type)
  if maybe_type_id is not None:
    return maybe_type_id
  return return_id(cursor, some_type)


def insertOrGetKey(get_by_key : GetTableByKey[SK, S],
                   return_key : ReturnKey[S, SK],
                   key_type   : Type[SK],
                   cursor     : Any,
                   key_table  : S,
                  ) -> SK:
  key_fields = set(key_type.__dataclass_fields__.keys())
  key_parts = { k : v for k, v in key_table().items() if k in key_fields }
  key = key_type(**key_parts)
  if (x := get_by_key(cursor, key)) is not None:
    return key
  k = return_key(cursor, key_table)
  return k


def generateInsertMany(table_name     : str,
                       field_names    : Sequence[str],
                       number_inserts : int,
                      ) -> Composed:
  placeholder = PGJoin("",
                       [SQL("("),
                        PGJoin(", ", [Placeholder()] * len(field_names)),
                        SQL(")")
                       ]
                      )
  values = PGJoin(", ", [placeholder] * number_inserts)
  fields = PGJoin(", ", tuple(map(sql.Identifier, field_names)))

  stmt = PGFormat("insert into {table_name} ({fields}) values{values}",
                  table_name = Identifier(table_name),
                  fields = fields,
                  values = values,
                 )
  return stmt


