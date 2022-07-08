from .types import LocalValue, UnitType, LocalType, LocalGroup

from . import lookups
from ...db import Generator
from ...db.generic_types import GetId, GetTable, ReturnId, ReturnSameKey, ReturnKey

g = Generator(type_table_lookup = lookups.type_table_lookup,
              data_table_lookup = lookups.data_table_lookup,
              id_table_lookup = lookups.id_table_lookup,
              key_table_lookup = lookups.key_table_lookup,
             )

getMaybeLocalValue   : GetId[LocalValue] = g.getMaybeIdFunction(LocalValue)
getMaybeUnitTypeId   : GetId[UnitType]   = g.getMaybeIdFunction(UnitType)
getMaybeLocalTypeId  : GetId[LocalType]  = g.getMaybeIdFunction(LocalType)
getMaybeLocalValueId  : GetId[LocalValue]  = g.getMaybeIdFunction(LocalValue)
getMaybeLocalGroupId : GetId[LocalGroup] = g.getMaybeIdFunction(LocalGroup)

getLocalType  : GetTable[LocalType] = g.getTableFunction(LocalType)

insertLocalValue : ReturnId[LocalValue] = g.getInsertReturnIdFunction(LocalValue)
insertUnitType   : ReturnId[UnitType]   = g.getInsertReturnIdFunction(UnitType)
insertLocalType  : ReturnId[LocalType]  = g.getInsertReturnIdFunction(LocalType)
insertLocalGroup : ReturnId[LocalGroup] = g.getInsertReturnIdFunction(LocalGroup)

insertOrGetUnitType = g.partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = g.partialInsertOrGetId(getMaybeLocalTypeId, insertLocalType)
insertOrGetLocalValue = g.partialInsertOrGetId(getMaybeLocalValueId, insertLocalValue)
insertOrGetLocalGroup = g.partialInsertOrGetId(getMaybeLocalGroupId, insertLocalGroup)

