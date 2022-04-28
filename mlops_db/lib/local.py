from psycopg2.extensions import register_adapter, adapt

register_adapter(set, lambda s : adapt(list(s)))

from typing import List, Tuple, Any, Type, TypeVar, Iterable, Set
from typing import cast

from .types import LocalType, LocalValue, UnitType, Key
from .schema_api import getMaybeLocalTypeId

T = TypeVar('T')


def sqlToTypedDict(rows : List[Any],
                   some_type : Type,
                  ) -> List[T]:
  return cast(List[T], [{k : v for k, v in row.items() if k in some_type.__annotations__.keys()} for row in rows])



def _load(cursor     : Any,
          key        : Key,
          local_type_id : int,
         ) -> Iterable[Tuple[LocalValue, UnitType]]:
  stmt = """select *
            from local_value V, unit_type U
            where V.unit_type_id = U.unit_type_id and U.local_type_id = %s and
              V.geom_id = %s and V.field_id = %s and V.acquired > %s and V.acquired < %s
          """

  geom_id    = key["geom_id"]
  field_id   = key["field_id"]
  start_date = key["start_date"]
  end_date   = key["end_date"]

  args = (local_type_id, geom_id, field_id, start_date, end_date)

  cursor.execute(stmt, args)
  results = cursor.fetchall()

  local_value : List[LocalValue] = sqlToTypedDict(results, LocalValue)
  unit_type : List[UnitType] = sqlToTypedDict(results, UnitType)

  return zip(local_value, unit_type)


def load(cursor        : Any,
         keys          : List[Key],
         local_type_id : int,
        ) -> List[Tuple[LocalValue, UnitType]]:
    results : List[Tuple[LocalValue, UnitType]] = []
    for k in keys:
      partial_results = _load(cursor, k, local_type_id)

      results.extend(partial_results)
    return results
