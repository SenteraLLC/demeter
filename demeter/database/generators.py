from typing import TypedDict, Any, Literal, List, Dict, Callable, Optional, Sequence, Type, TypeVar, Union
from psycopg2 import sql, extras

from collections import OrderedDict

from typing import cast
from functools import partial

from .types_protocols import TableKey
from .api_protocols import GetId, ReturnId, AnyIdTable, AnyKeyTable, AnyTypeTable, ReturnKey, ReturnSameKey, S, SK, AnyTable, I, S
from .type_lookups import id_table_lookup, key_table_lookup

from typing import get_origin, get_args, Union


# TODO: This 'is_optional' filter might be a bad idea...
def is_none(table : AnyTable, key : str) -> bool:
  return getattr(table, key) is None

def is_optional(table : AnyTable, key : str):
  field = table.__dataclass_fields__[key].type
  return get_origin(field) is Union and \
           type(None) in get_args(field)


def _generateInsertStmt(table_name : str,
                        table      : Union[AnyTable, AnyKeyTable],
                        return_key : Optional[Sequence[str]],
                       ) -> sql.SQL:
  stmt_template = "insert into {table} ({fields}) values({places})"

  names_to_fields = OrderedDict({name : sql.Identifier(name) for name in table.keys() if not (is_optional(table, name) and is_none(table, name)) })


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
                        key        : Type[SK],
                       ) -> ReturnKey[S, SK]:
  return_key = list(key.__dataclass_fields__.keys())

  def __impl(cursor : Any,
             table  : S,
            ) -> SK:
    stmt = _generateInsertStmt(table_name, table, return_key)
    cursor.execute(stmt, table.args())
    result = cast(SK, cursor.fetchone())
    return result

  return __impl


def getInsertReturnSameKeyFunction(table : Type[SK]) -> ReturnSameKey[SK]:
  table_name, key = key_table_lookup[table]
  key = cast(Type[SK], key)
  fn = cast(ReturnSameKey[SK], _insertAndReturnKey(table_name, key))
  return fn


def getInsertReturnKeyFunction(table : Type[S]) -> ReturnKey[S, SK]:
  table_name, key = key_table_lookup[table]
  fn = cast(ReturnKey[S, SK], _insertAndReturnKey(table_name, key))
  return fn



def getMaybeId(table_name : str,
               cursor     : Any,
               table      : AnyIdTable,
              ) -> Optional[int]:
  field_names = cast(Sequence[str], table.keys())
  names_to_fields = OrderedDict({name: sql.Identifier(name) for name in field_names })

  conditions = [sql.SQL(' = ').join([sql.Identifier(n), sql.Placeholder(n)]) for n in names_to_fields if not is_none(table, n) and not is_optional(table, n) ]
  # TODO: Is this worth supporting as an option somehow?
  #conditions.extend([sql.SQL('').join([sql.Identifier(n), sql.SQL(" is null")]) for n in names_to_fields if is_none(table, n) ])

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


def getMaybeTableById(table_type    : Type[I],
                      table_id_name : str,
                      cursor        : Any,
                      table_id      : int,
                     ) -> Optional[I]:
  table_name = id_table_lookup[table_type]
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
  return cast(I, table_type(**result))


def getTableById(table_type    : Type[I],
                 table_id_name : str,
                 cursor        : Any,
                 table_id      : int,
                ) -> I:
  table_name = id_table_lookup[table_type]
  maybe_table = getMaybeTableById(table_type, table_id_name, cursor, table_id)
  if maybe_table is None:
    raise Exception(f"No entry found for {table_id_name} = {table_id} in {table_name}")
  table = maybe_table
  return table


def getTableFunction(table : Type[Any],
                     table_id_name : Optional[str] = None
                    ) -> Callable[[Any, int], I]:
  table_name = id_table_lookup[table]
  if table_id_name is None:
    table_id_name = "_".join([table_name, "id"])
  return partial(getTableById, table, table_id_name)


def getTableByKey(table_name : str,
                  key        : Sequence[str],
                  cursor     : Any,
                  table_id   : int,
                 ) -> S:
  table_id_name = "_".join([table_name, "id"])
  conditions = [sql.SQL(' = ').join([sql.Identifier(k), sql.Placeholder(k)]) for k in key]
  stmt = sql.SQL("select * from {0} where {1}").format(
    sql.Identifier(table_name),
    sql.SQL(' and ').join(conditions),
  )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  return cast(S, result)


def getTableKeyFunction(table : Type[Any]) -> Callable[[Any, int], S]:
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


