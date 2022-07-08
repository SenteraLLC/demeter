from typing import Any, Optional, NamedTuple, Sequence
from typing import cast

from collections import OrderedDict, namedtuple

from psycopg2.sql import Identifier, Placeholder

from .. import TableId
from ..union_types import AnyIdTable
from ..generic_types import S

from .tools import PGJoin, PGFormat
from .helpers import is_none, is_optional

def getMaybeId(table_name : str,
               cursor     : Any,
               table      : AnyIdTable,
              ) -> Optional[TableId]:
  field_names = table.names()
  names_to_fields = OrderedDict({name: Identifier(name) for name in field_names })

  conditions = [PGJoin(' = ', [Identifier(n), Placeholder(n)]) for n in names_to_fields if not is_none(table, n) and not is_optional(table, n) ]

  table_id = "_".join([table_name, "id"])
  stmt = PGFormat("select {0} from {1} where {2}",
                  Identifier(table_id),
                  Identifier(table_name),
                  PGJoin(' and ', conditions),
                 )
  args = table()
  cursor.execute(stmt, args)
  result = cursor.fetchone()
  if result is not None:
    return TableId(result[0])
  return None


def getTable(table_name : str,
             table_id_name : str,
             table_id : TableId,
             cursor : Any,
            ) -> NamedTuple:
  condition = PGJoin(' = ', [Identifier(table_id_name), Placeholder(table_id_name)])
  stmt = PGFormat("select * from {0} where {1}",
                  Identifier(table_name),
                  condition,
                 )
  cursor.execute(stmt, {table_id_name : table_id})
  result = cursor.fetchone()
  return cast(NamedTuple, result)


def getTableByKey(table_name : str,
                  key        : Sequence[str],
                  cursor     : Any,
                  table_id   : TableId,
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


