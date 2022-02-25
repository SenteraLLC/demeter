from typing import List, Tuple, Any, Type, TypeVar
from typing import cast

from .types import GeoSpatialKey, TemporalKey, LocalType, LocalValue, UnitType

from .schema_api import getMaybeLocalTypeId

T = TypeVar('T')

def sqlToTypedDict(rows : List[Any],
                   some_type : Type,
                  ) -> List[T]:
  return cast(List[T], [{k : v for k, v in row.items() if k in some_type.__annotations__.keys()} for row in rows])

def _load(cursor : Any,
          geospatial_key : GeoSpatialKey,
          temporal_key   : TemporalKey,
          local_type     : LocalType,
         ) -> List[Tuple[LocalValue, UnitType]]:
  local_type_id = getMaybeLocalTypeId(cursor, local_type)
  stmt = """select *
            from local_value V, unit_type U
            where V.unit_type_id = U.unit_type_id and U.local_type_id = %s and
              V.geom_id = %s and V.field_id = %s and V.acquired > %s and V.acquired < %s

          """

  geom_id    = geospatial_key["geom_id"]
  field_id   = geospatial_key["field_id"]
  start_date = temporal_key["start_date"]
  end_date   = temporal_key["end_date"]

  args = (local_type_id, geom_id, field_id, start_date, end_date)

  cursor.execute(stmt, args)
  results = cursor.fetchall()

  local_value : List[LocalValue] = sqlToTypedDict(results, LocalValue)
  unit_type : List[UnitType] = sqlToTypedDict(results, UnitType)

  return list(zip(local_value, unit_type))
