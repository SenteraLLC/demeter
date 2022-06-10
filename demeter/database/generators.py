from typing import TypedDict, Any, Callable, Optional, Sequence, Type, TypeVar, Union, Dict, Mapping
from typing import cast, get_origin, get_args

from collections import OrderedDict
from functools import partial

from psycopg2 import sql
from psycopg2.sql import SQL, Composable, Composed, Identifier, Placeholder

from .types_protocols import TableKey
from .api_protocols import GetId, ReturnId, AnyIdTable, AnyKeyTable, AnyTypeTable, ReturnKey, ReturnSameKey, S, SK, AnyTable, I, S
from .type_lookups import id_table_lookup, key_table_lookup



# TODO: Add options for 'is_none' and 'is_optional'

def is_none(table : AnyTable, key : str) -> bool:
  return getattr(table, key) is None

def is_optional(table : AnyTable, key : str) -> bool:
  field = table.__dataclass_fields__[key].type
  return get_origin(field) is Union and \
           type(None) in get_args(field)

# NOTE: Psycopg2.sql method stubs are untyped
#   These 'overrides' will have to do for now
def PGJoin(sep : str, args : Sequence[Composable]) -> Composed:
  return cast(Composed, SQL(sep).join(args)) # type: ignore

def PGFormat(template : str,
             *args : Composable,
             **kwargs : Composable
            ) -> Composed:
  return cast(Composed, SQL(template).format(*args, **kwargs)) # type: ignore


def _generateInsertStmt(table_name : str,
                        table      : Union[AnyTable, AnyKeyTable],
                        return_key : Optional[Sequence[str]],
                       ) -> Composed:
  stmt_template = "insert into {table} ({fields}) values({places})"

  names_to_fields = OrderedDict({name : Identifier(name) for name in table.names() if not (is_optional(table, name) and is_none(table, name)) })


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


def generateInsertMany(table_name     : str,
                       field_names    : Sequence[str],
                       number_inserts : int,
                      ) -> Composed:
  placeholder = PGJoin("", [SQL("("),
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


def _insertAndReturnId(table_name : str,
                       cursor     : Any,
                       table      : AnyIdTable,
                      ) -> int:
  table_id = table_name + "_id"
  return_key = [table_id]
  stmt = _generateInsertStmt(table_name, table, return_key)
  cursor.execute(stmt, table())
  result = cursor.fetchone()
  return int(result[0])


def getInsertReturnIdFunction(table : Type[Any]) -> Callable[[Any, AnyIdTable], int]:
  table_name = id_table_lookup[table]
  return partial(_insertAndReturnId, table_name)


def _insertAndReturnKey(table_name : str,
                        key        : Type[SK],
                       ) -> ReturnKey[S, SK]:
  return_key = [f for f in key.names()]
  def __impl(cursor : Any,
             table  : S,
            ) -> SK:
    stmt = _generateInsertStmt(table_name, table, return_key)
    cursor.execute(stmt, table())
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
  field_names = table.names()
  names_to_fields = OrderedDict({name: Identifier(name) for name in field_names })

  conditions = [PGJoin(' = ', [Identifier(n), Placeholder(n)]) for n in names_to_fields if not is_none(table, n) and not is_optional(table, n) ]

  table_id = "_".join([table_name, "id"])
  stmt = PGFormat("select {0} from {1} where {2}",
                  Identifier(table_id),
                  Identifier(table_name),
                  PGJoin(' and ', conditions),
                 )
  cursor.execute(stmt, table())
  result = cursor.fetchone()
  if result is not None:
    return int(result[0])
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
  condition = PGJoin(' = ', [Identifier(table_id_name), Placeholder(table_id_name)])
  stmt = PGFormat("select * from {0} where {1}",
                  Identifier(table_name),
                  condition,
                 )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  if result is None:
    return None
  table_args = {k : v for k, v in result._asdict().items() if k != table_id_name}
  return cast(I, table_type(**table_args))


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
  conditions = [PGJoin(' = ', [Identifier(k), Placeholder(k)]) for k in key]
  stmt = PGFormat("select * from {0} where {1}",
                  Identifier(table_name),
                  PGJoin(' and ', conditions),
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


