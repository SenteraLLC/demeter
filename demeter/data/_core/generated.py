"""Constructs insert functions for core types using `SQLGenerator`"""

from ...db import SQLGenerator
from ...db._generic_types import (
    GetId,
    GetTable,
    ReturnId,
)
from .._core import lookups as _lookups
from .._core.types import (
    Act,
    CropType,
    Field,
)
from .field_group import FieldGroup
from .st_types import GeoSpatialKey, TemporalKey

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
    # key_table_lookup=_lookups.key_table_lookup,
)

# core types
getMaybeFieldGroupId: GetId[FieldGroup] = g.getMaybeIdFunction(FieldGroup)
getMaybeFieldId: GetId[Field] = g.getMaybeIdFunction(Field)
getMaybeCropTypeId: GetId[CropType] = g.getMaybeIdFunction(CropType)
getMaybeActId: GetId[Act] = g.getMaybeIdFunction(Act)

getFieldGroup: GetTable[FieldGroup] = g.getTableFunction(FieldGroup)
getField: GetTable[Field] = g.getTableFunction(Field)
getCropType: GetTable[CropType] = g.getTableFunction(CropType)
getAct: GetTable[Act] = g.getTableFunction(Act)

insertFieldGroup: ReturnId[FieldGroup] = g.getInsertReturnIdFunction(FieldGroup)
insertField: ReturnId[Field] = g.getInsertReturnIdFunction(Field)
insertCropType: ReturnId[CropType] = g.getInsertReturnIdFunction(CropType)
insertAct: ReturnId[Act] = g.getInsertReturnIdFunction(Act)


insertOrGetFieldGroup: ReturnId[FieldGroup] = g.partialInsertOrGetId(
    getMaybeFieldGroupId, insertFieldGroup
)
insertOrGetField: ReturnId[Field] = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetCropType: ReturnId[CropType] = g.partialInsertOrGetId(
    getMaybeCropTypeId, insertCropType
)
insertOrGetAct: ReturnId[Act] = g.partialInsertOrGetId(getMaybeActId, insertAct)


# spatiotemporal types
getMaybeGeoSpatialKeyId: GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId: GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)

## Redundant with `getMaybeGeom()` in `geom.py`
# getGeom: GetTable[Geom] = g.getTableFunction(Geom)

insertGeoSpatialKey: ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(
    GeoSpatialKey
)
insertTemporalKey: ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)

insertOrGetGeoSpatialKey = g.partialInsertOrGetId(
    getMaybeGeoSpatialKeyId, insertGeoSpatialKey
)
insertOrGetTemporalKey = g.partialInsertOrGetId(
    getMaybeTemporalKeyId, insertTemporalKey
)
