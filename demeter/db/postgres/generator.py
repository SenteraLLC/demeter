from typing import Any, Callable, Optional, Sequence, Type, Union, Dict, Mapping
from typing import cast

from collections import OrderedDict
from functools import partial, wraps

from psycopg2 import sql
from psycopg2.sql import SQL, Composed, Identifier, Placeholder

from .. import TableId
from ..generic_types import ReturnKey, ReturnSameKey, ReturnId, GetId
from ..union_types import AnyIdTable
from ..lookup_types import TableLookup, KeyTableLookup

from .insert import insertAndReturnId, insertAndReturnKey, insertOrGetType
from .get import getMaybeId, getTable, getTableByKey

from .tools import PGJoin, PGFormat

from ..generic_types import I, S, SK, T


class SQLGenerator:
  def __init__(self,
               type_table_lookup : TableLookup = {},
               data_table_lookup : TableLookup = {},
               id_table_lookup   : TableLookup = {},
               key_table_lookup  : KeyTableLookup = {},
              ) -> None:
    self.type_table_lookup = type_table_lookup
    self.data_table_lookup = data_table_lookup
    self.id_table_lookup = id_table_lookup
    self.key_table_lookup = key_table_lookup


  def getInsertReturnIdFunction(self, table : Type[I]) -> ReturnId[I]:
    table_name = self.id_table_lookup[table]
    return partial(insertAndReturnId, table_name)


  def getInsertReturnSameKeyFunction(self, table : Type[SK]) -> ReturnSameKey[SK]:
    table_name, key = self.key_table_lookup[table]
    key = cast(Type[SK], key)
    fn = cast(ReturnSameKey[SK], insertAndReturnKey(table_name, key))
    return fn


  def getInsertReturnKeyFunction(self, table : Type[S]) -> ReturnKey[S, SK]:
    table_name, key = self.key_table_lookup[table]
    fn = cast(ReturnKey[S, SK], insertAndReturnKey(table_name, key))
    return fn


  def getMaybeIdFunction(self, t : Type[T]) -> Callable[[Any, AnyIdTable], Optional[TableId]]:
    table_name = self.id_table_lookup[t]
    return partial(getMaybeId, table_name)


  def getMaybeTableById(self,
                        table_type    : Type[I],
                        table_id_name : str,
                        cursor        : Any,
                        table_id      : TableId,
                       ) -> Optional[I]:
    table_name = self.id_table_lookup[table_type]
    table = getTable(table_name, table_id_name, table_id, cursor)
    if table is None:
      return None
    table_args = {k : v for k, v in table._asdict().items() if k != table_id_name}
    return cast(I, table_type(**table_args))


  def getTableById(self,
                   table_type    : Type[I],
                   table_id_name : str,
                   cursor        : Any,
                   table_id      : TableId,
                  ) -> I:
    maybe_table = self.getMaybeTableById(table_type, table_id_name, cursor, table_id)
    if maybe_table is None:
      table_name = self.id_table_lookup[table_type]
      raise Exception(f"No entry found for {table_id_name} = {table_id} in {table_name}")
    table = maybe_table
    return table


  def getTableFunction(self,
                       t : Type[I],
                       table_id_name : Optional[str] = None
                      ) -> Callable[[Any, TableId], I]:
    table_name = self.id_table_lookup[t]
    if table_id_name is None:
      table_id_name = "_".join([table_name, "id"])

    # This is a mess because mypy doesn't support partially-applied methods
    __impl_table_id_name = table_id_name

    @wraps(self.getTableById)
    def _impl(cursor : Any, table_id : TableId) -> I:
      return self.getTableById(t, __impl_table_id_name, cursor, table_id)

    return _impl


  def getTableKeyFunction(self,
                          t : Type[S],
                         ) -> Callable[[Any, TableId], S]:
    table_name, key = self.key_table_lookup[t]
    return partial(getTableByKey, table_name, key)


  def partialInsertOrGetId(self,
                           get_id    : GetId[I],
                           return_id : ReturnId[I],
                          ) -> ReturnId[I]:
    return partial(insertOrGetType, get_id, return_id)

