from ...db import SQLGenerator
from ...db._generic_types import (
    GetId,
    GetTable,
    ReturnId,
)
from .._observation.types import (
    Observation,
    ObservationType,
    UnitType,
)
from . import lookups as _lookups

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
)

getUnitType: GetTable[UnitType] = g.getTableFunction(UnitType)
getObservationType: GetTable[ObservationType] = g.getTableFunction(ObservationType)
getObservation: GetTable[Observation] = g.getTableFunction(Observation)

getMaybeUnitTypeId: GetId[UnitType] = g.getMaybeIdFunction(UnitType)
getMaybeObservationTypeId: GetId[ObservationType] = g.getMaybeIdFunction(
    ObservationType
)
getMaybeObservationId: GetId[Observation] = g.getMaybeIdFunction(Observation)

insertObservation: ReturnId[Observation] = g.getInsertReturnIdFunction(Observation)
insertUnitType: ReturnId[UnitType] = g.getInsertReturnIdFunction(UnitType)
insertObservationType: ReturnId[ObservationType] = g.getInsertReturnIdFunction(
    ObservationType
)

insertOrGetUnitType: ReturnId[UnitType] = g.partialInsertOrGetId(
    getMaybeUnitTypeId, insertUnitType
)
insertOrGetObservationType: ReturnId[ObservationType] = g.partialInsertOrGetId(
    getMaybeObservationTypeId, insertObservationType
)
insertOrGetObservation: ReturnId[Observation] = g.partialInsertOrGetId(
    getMaybeObservationId, insertObservation
)
