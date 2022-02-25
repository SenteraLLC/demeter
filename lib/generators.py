# TODO: Memoize
# TODO: Deal with the type invariance problem
# TODO: Custom exceptions?

from typing import TypedDict, Any, Literal, List, Tuple, Dict, Callable, Optional, Sequence, Type, TypeVar
from psycopg2 import sql, extras

from collections import OrderedDict

from typing import cast
from functools import partial

from . import types

def _generateInsertStmt(table_name : str,
                        table      : types.AnyIdTable,
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


def _insertAndReturnId(table_name : str,
                       cursor     : Any,
                       table      : types.AnyIdTable,
                      ) -> int:
  table_id = table_name + "_id"
  return_key = [table_id]
  stmt = _generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table)
  result = cursor.fetchone()
  return int(result[table_id])


def getInsertReturnIdFunction(table : Type[Any]) -> Callable[[Any, types.AnyIdTable], int]:
  table_name = types.id_table_lookup[table]
  return partial(_insertAndReturnId, table_name)


def _insertAndReturnKey(table_name : str,
                        key        : types.Key,
                        cursor     : Any,
                        table      : types.AnyIdTable,
                       ) -> types.Key:
  return_key = list(key.__annotations__)
  stmt = _generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table)
  result = cast(types.Key, cursor.fetchone())
  return result


def getInsertReturnKeyFunction(table : Type[Any]) -> Callable[[Any, types.AnyKeyTable], types.Key]:
  table_name, key = types.key_table_lookup[table]
  return partial(_insertAndReturnKey, table_name, key)


def getMaybeId(table_name : str,
               cursor     : Any,
               table      : types.AnyIdTable,
              ) -> Optional[int]:
  field_names = cast(Sequence[str], table.keys()) # type: ignore
  names_to_fields = OrderedDict({name: sql.Identifier(name) for name in field_names})
  def is_none(key : str):
    args = cast(Dict[str, Any], table)
    return args[key] is None
  conditions = [sql.SQL(' = ').join([sql.Identifier(n), sql.Placeholder(n)]) for n in names_to_fields if not is_none(n)]
  conditions.extend([sql.SQL('').join([sql.Identifier(n), sql.SQL(" is null")]) for n in names_to_fields if is_none(n)])

  table_id = "_".join([table_name, "id"])
  stmt = sql.SQL("select {0} from {1} where {2}").format(
    sql.Identifier(table_id),
    sql.Identifier(table_name),
    sql.SQL(' and ').join(conditions),
  )
  cursor.execute(stmt, table)
  result = cursor.fetchone()
  if result is not None:
    return result[table_id]
  return None


def getMaybeIdFunction(table : Type[Any]) -> Callable[[Any, types.AnyIdTable], Optional[int]]:
  table_name = types.id_table_lookup[table]
  return partial(getMaybeId, table_name)


M = TypeVar('M', bound=types.AnyIdTable)
def getTableById(table_name : str,
                 cursor     : Any,
                 table_id   : int,
                ) -> M:
  table_id_name = "_".join([table_name, "id"])
  condition = sql.SQL(' = ').join([sql.Identifier(table_id_name), sql.Placeholder(table_id_name)])
  stmt = sql.SQL("select * from {0} where {1}").format(
    sql.Identifier(table_name),
    condition,
  )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  del result[table_id_name]
  return cast(M, result)


def getTableFunction(table : Type[Any]) -> Callable[[Any, int], M]:
  table_name = types.id_table_lookup[table]
  return partial(getTableById, table_name)


N = TypeVar('N', bound=types.AnyKeyTable)

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
  table_name, key = types.key_table_lookup[table]
  return partial(getTableByKey, table_name, key)

### Data ###


