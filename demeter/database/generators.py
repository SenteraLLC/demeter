from typing import TypedDict, Any, Literal, List, Tuple, Dict, Callable, Optional, Sequence, Type, TypeVar
from psycopg2 import sql, extras

from collections import OrderedDict

from typing import cast
from functools import partial

from ..types.core import Key

from .api_protocols import GetId, ReturnId, AnyIdTable, AnyKeyTable, AnyTypeTable
from .type_lookups import id_table_lookup, key_table_lookup


def _generateInsertStmt(table_name : str,
                        table      : AnyIdTable,
                        return_key : Optional[Sequence[str]],
                       ) -> sql.SQL:
  stmt_template = "insert into {table} ({fields}) values({places})"

  names_to_fields = OrderedDict({name : sql.Identifier(name) for name in table.keys()})

  to_interpolate : Dict[str, Any] = {
    "table"  : sql.Identifier(table_name),
    "fields" : sql.SQL(",").join(names_to_fields.values()),
    "places" : sql.SQL(",").join([sql.Placeholder(n) for n in names_to_fields.keys()]),
  }
  if return_key is not None:
    key_names = [sql.Identifier(k) for k in return_key]
    stmt_template += " returning {returning}"
    returning = sql.SQL(",").join(key_names)
    to_interpolate["returning"] = returning
  stmt = sql.SQL(stmt_template).format(**to_interpolate)
  return stmt


def generateInsertMany(table_name     : str,
                       field_names    : List[str],
                       number_inserts : int,
                      ) -> sql.SQL:
  placeholder = sql.SQL("").join([sql.SQL("("),
                                  sql.SQL(", ").join([sql.Placeholder()] * len(field_names)),
                                  sql.SQL(")")
                                 ]
                                )
  values = sql.SQL(", ").join(
                   [placeholder] * number_inserts
                  )
  fields = sql.SQL(", ").join(map(sql.Identifier, field_names))

  stmt = sql.SQL("insert into {table_name} ({fields}) values{values}").format(
                 table_name = sql.Identifier(table_name),
                 fields = fields,
                 values = values,
                )
  return stmt


def _insertAndReturnId(table_name : str,
                       cursor     : Any,
                       table      : AnyIdTable,
                      ) -> int:
  table_id = table_name + "_id"
  return_key = [table_id]
  stmt = _generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table.args())
  result = cursor.fetchone()
  return int(result[table_id])


def getInsertReturnIdFunction(table : Type[Any]) -> Callable[[Any, AnyIdTable], int]:
  table_name = id_table_lookup[table]
  return partial(_insertAndReturnId, table_name)


def _insertAndReturnKey(table_name : str,
                        key        : Key,
                        cursor     : Any,
                        table      : AnyIdTable,
                       ) -> Key:
  return_key = list(key.__annotations__)
  stmt = _generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table.args())
  result = cast(Key, cursor.fetchone())
  return result


def getInsertReturnKeyFunction(table : Type[Any]) -> Callable[[Any, AnyKeyTable], Key]:
  table_name, key = key_table_lookup[table]
  return partial(_insertAndReturnKey, table_name, key)

from typing import get_origin, get_args, Union

def is_optional(table, key):
    field = table.__annotations__[key]
    return get_origin(field) is Union and \
           type(None) in get_args(field)


def getMaybeId(table_name : str,
               cursor     : Any,
               table      : AnyIdTable,
              ) -> Optional[int]:
  field_names = cast(Sequence[str], table.keys())
  names_to_fields = OrderedDict({name: sql.Identifier(name) for name in field_names })
  def is_none(key : str) -> bool:
    args = cast(Dict[str, Any], table)
    return getattr(args, key) is None

  # TODO: This 'is_optional' filter might be a bad idea...
  conditions = [sql.SQL(' = ').join([sql.Identifier(n), sql.Placeholder(n)]) for n in names_to_fields if not is_none(n) and not is_optional(table, n) ]
  conditions.extend([sql.SQL('').join([sql.Identifier(n), sql.SQL(" is null")]) for n in names_to_fields if is_none(n) ])

  table_id = "_".join([table_name, "id"])
  stmt = sql.SQL("select {0} from {1} where {2}").format(
    sql.Identifier(table_id),
    sql.Identifier(table_name),
    sql.SQL(' and ').join(conditions),
  )
  cursor.execute(stmt, table.args())
  result = cursor.fetchone()
  if result is not None:
    return result[table_id]
  return None


def getMaybeIdFunction(table : Type[Any]) -> Callable[[Any, AnyIdTable], Optional[int]]:
  table_name = id_table_lookup[table]
  return partial(getMaybeId, table_name)


M = TypeVar('M', bound=AnyIdTable)

def getMaybeTableById(table_name    : str,
                      table_id_name : str,
                      cursor        : Any,
                      table_id      : int,
                     ) -> Optional[M]:
  condition = sql.SQL(' = ').join([sql.Identifier(table_id_name), sql.Placeholder(table_id_name)])
  stmt = sql.SQL("select * from {0} where {1}").format(
    sql.Identifier(table_name),
    condition,
  )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  if result is None:
    return None
  del result[table_id_name]
  return cast(M, result)


def getTableById(table_name    : str,
                 table_id_name : str,
                 cursor        : Any,
                 table_id      : int,
                ) -> M:
  maybe_table = getMaybeTableById(table_name, table_id_name, cursor, table_id)
  if maybe_table is None:
    raise Exception(f"No entry found for {table_id_name} = {table_id} in {table_name}")
  table = dict(maybe_table)
  return cast(M, table)


def getTableFunction(table : Type[Any],
                     table_id_name : Optional[str] = None
                    ) -> Callable[[Any, int], M]:
  table_name = id_table_lookup[table]
  if table_id_name is None:
    table_id_name = "_".join([table_name, "id"])
  return partial(getTableById, table_name, table_id_name)


N = TypeVar('N', bound=AnyKeyTable)

def getTableByKey(table_name : str,
                  key        : Sequence[str],
                  cursor     : Any,
                  table_id   : int,
                 ) -> N:
  table_id_name = "_".join([table_name, "id"])
  conditions = [sql.SQL(' = ').join([sql.Identifier(k), sql.Placeholder(k)]) for k in key]
  stmt = sql.SQL("select * from {0} where {1}").format(
    sql.Identifier(table_name),
    sql.SQL(' and ').join(conditions),
  )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  return cast(N, result)


def getTableKeyFunction(table : Type[Any]) -> Callable[[Any, int], N]:
  table_name, key = key_table_lookup[table]
  return partial(getTableByKey, table_name, key)


W = TypeVar('W', bound=AnyTypeTable)

def insertOrGetType(get_id    : GetId[W],
                    return_id : ReturnId[W],
                    cursor    : Any,
                    some_type : W,
                   ) -> int:
  maybe_type_id = get_id(cursor, some_type)
  if maybe_type_id is not None:
    return maybe_type_id
  return return_id(cursor, some_type)


