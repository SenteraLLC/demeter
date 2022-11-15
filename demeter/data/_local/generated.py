from .types import Operation, ObservationType, ObservationValue, UnitType
from . import lookups as _lookups

from ...db._generic_types import GetId, GetTable, ReturnId
from ...db import SQLGenerator

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
)


getMaybeObservationValue: GetId[ObservationValue] = g.getMaybeIdFunction(
    ObservationValue
)
getMaybeUnitTypeId: GetId[UnitType] = g.getMaybeIdFunction(UnitType)
getMaybeObservationTypeId: GetId[ObservationType] = g.getMaybeIdFunction(
    ObservationType
)
getMaybeObservationValueId: GetId[ObservationValue] = g.getMaybeIdFunction(
    ObservationValue
)
getMaybeOperationId = g.getMaybeIdFunction(Operation)

getObservationType: GetTable[ObservationType] = g.getTableFunction(ObservationType)

insertObservationValue: ReturnId[ObservationValue] = g.getInsertReturnIdFunction(
    ObservationValue
)
insertUnitType: ReturnId[UnitType] = g.getInsertReturnIdFunction(UnitType)
insertObservationType: ReturnId[ObservationType] = g.getInsertReturnIdFunction(
    ObservationType
)
insertOperation = g.getInsertReturnIdFunction(Operation)


insertOrGetUnitType = g.partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetObservationType = g.partialInsertOrGetId(
    getMaybeObservationTypeId, insertObservationType
)
insertOrGetObservationValue = g.partialInsertOrGetId(
    getMaybeObservationValueId, insertObservationValue
)
insertOrGetOperation = g.partialInsertOrGetId(getMaybeOperationId, insertOperation)
