from .types import Act, LocalType, LocalValue, UnitType
from . import lookups as _lookups

from ...db._generic_types import GetId, GetTable, ReturnId
from ...db import SQLGenerator

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
)


getMaybeLocalValue: GetId[LocalValue] = g.getMaybeIdFunction(LocalValue)
getMaybeUnitTypeId: GetId[UnitType] = g.getMaybeIdFunction(UnitType)
getMaybeLocalTypeId: GetId[LocalType] = g.getMaybeIdFunction(LocalType)
getMaybeLocalValueId: GetId[LocalValue] = g.getMaybeIdFunction(LocalValue)
getMaybeActId = g.getMaybeIdFunction(Act)

getLocalType: GetTable[LocalType] = g.getTableFunction(LocalType)

insertLocalValue: ReturnId[LocalValue] = g.getInsertReturnIdFunction(LocalValue)
insertUnitType: ReturnId[UnitType] = g.getInsertReturnIdFunction(UnitType)
insertLocalType: ReturnId[LocalType] = g.getInsertReturnIdFunction(LocalType)
insertAct = g.getInsertReturnIdFunction(Act)


insertOrGetUnitType = g.partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = g.partialInsertOrGetId(getMaybeLocalTypeId, insertLocalType)
insertOrGetLocalValue = g.partialInsertOrGetId(getMaybeLocalValueId, insertLocalValue)
insertOrGetAct = g.partialInsertOrGetId(getMaybeActId, insertAct)
