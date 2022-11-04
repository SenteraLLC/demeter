from .types import Operation, LocalGroup, LocalType, LocalValue, UnitType
from . import lookups as _lookups

from ...db._generic_types import GetId, GetTable, ReturnId
from ...db._postgres import SQLGenerator

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
getMaybeLocalGroupId: GetId[LocalGroup] = g.getMaybeIdFunction(LocalGroup)
getMaybeOperationId = g.getMaybeIdFunction(Operation)

getLocalType: GetTable[LocalType] = g.getTableFunction(LocalType)

insertLocalValue: ReturnId[LocalValue] = g.getInsertReturnIdFunction(LocalValue)
insertUnitType: ReturnId[UnitType] = g.getInsertReturnIdFunction(UnitType)
insertLocalType: ReturnId[LocalType] = g.getInsertReturnIdFunction(LocalType)
insertLocalGroup: ReturnId[LocalGroup] = g.getInsertReturnIdFunction(LocalGroup)
insertOperation = g.getInsertReturnIdFunction(Operation)


insertOrGetUnitType = g.partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = g.partialInsertOrGetId(getMaybeLocalTypeId, insertLocalType)
insertOrGetLocalValue = g.partialInsertOrGetId(getMaybeLocalValueId, insertLocalValue)
insertOrGetLocalGroup = g.partialInsertOrGetId(getMaybeLocalGroupId, insertLocalGroup)
insertOrGetOperation = g.partialInsertOrGetId(getMaybeOperationId, insertOperation)
