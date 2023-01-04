"""Constructs insert functions for core types using `SQLGenerator`"""
from .._core import lookups as _lookups

from ...db._generic_types import GetId, GetTable, ReturnId
from ...db import SQLGenerator

from .._core.types import (
    CropType,
    Field,
    Act,
)

from .st_types import (
    GeoSpatialKey,
    TemporalKey,
    Geom,
)

from .field_group import FieldGroup

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

getFieldGroup: GetTable[FieldGroup] = g.getTableFunction(FieldGroup)
getField: GetTable[Field] = g.getTableFunction(Field)

insertFieldGroup: ReturnId[FieldGroup] = g.getInsertReturnIdFunction(FieldGroup)
insertField: ReturnId[Field] = g.getInsertReturnIdFunction(Field)
insertCropType: ReturnId[CropType] = g.getInsertReturnIdFunction(CropType)

insertOrGetFieldGroup: ReturnId[FieldGroup] = g.partialInsertOrGetId(
    getMaybeFieldGroupId, insertFieldGroup
)
insertOrGetField: ReturnId[Field] = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetCropType: ReturnId[CropType] = g.partialInsertOrGetId(
    getMaybeCropTypeId, insertCropType
)


# act types
getMaybeActId: GetId[Act] = g.getMaybeIdFunction(Act)
insertAct: ReturnId[Act] = g.getInsertReturnIdFunction(Act)
insertOrGetAct: ReturnId[Act] = g.partialInsertOrGetId(getMaybeActId, insertAct)


# spatiotemporal types
getMaybeGeoSpatialKeyId: GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId: GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)

getGeom: GetTable[Geom] = g.getTableFunction(Geom)

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

# getMaybeReportTypeId: GetId[ReportType] = g.getMaybeIdFunction(ReportType)
# insertReportType: ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)

# insertPlanting = g.getInsertReturnKeyFunction(Planting, PlantingKey)
# insertHarvest = g.getInsertReturnKeyFunction(Harvest, PlantingKey)

# getPlanting = g.getTableByKeyFunction(Planting, PlantingKey)
# getHarvest = g.getTableByKeyFunction(Harvest, PlantingKey)

# insertOrGetPlanting = g.partialInsertOrGetKey(PlantingKey, getPlanting, insertPlanting)
# insertOrGetHarvest = g.partialInsertOrGetKey(PlantingKey, getHarvest, insertHarvest)
