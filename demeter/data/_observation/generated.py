from ...db import SQLGenerator
from ...db._generic_types import (
    GetId,
    GetTable,
    ReturnId,
)
from . import lookups as _lookups
from .types import (
    Observation,
    ObservationType,
    UnitType,
)

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
)


getMaybeObservation: GetId[Observation] = g.getMaybeIdFunction(Observation)
getMaybeUnitTypeId: GetId[UnitType] = g.getMaybeIdFunction(UnitType)
getMaybeObservationTypeId: GetId[ObservationType] = g.getMaybeIdFunction(
    ObservationType
)
getMaybeObservationId: GetId[Observation] = g.getMaybeIdFunction(Observation)


getObservationType: GetTable[ObservationType] = g.getTableFunction(ObservationType)

insertObservation: ReturnId[Observation] = g.getInsertReturnIdFunction(Observation)
insertUnitType: ReturnId[UnitType] = g.getInsertReturnIdFunction(UnitType)
insertObservationType: ReturnId[ObservationType] = g.getInsertReturnIdFunction(
    ObservationType
)

insertOrGetUnitType = g.partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetObservationType = g.partialInsertOrGetId(
    getMaybeObservationTypeId, insertObservationType
)
insertOrGetObservation = g.partialInsertOrGetId(
    getMaybeObservationId, insertObservation
)
