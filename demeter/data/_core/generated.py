from .._core import lookups as _lookups

from ...db._generic_types import GetId, GetTable, GetTableByKey, ReturnId
from ...db import SQLGenerator

from .types import (
    CropProgress,
    CropProgressKey,
    CropStage,
    CropType,
    Field,
    Planting,
    PlantingKey,
    Harvest,
    ReportType,
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
    key_table_lookup=_lookups.key_table_lookup,
)

getMaybeFieldGroupId = g.getMaybeIdFunction(FieldGroup)
getMaybeFieldId = g.getMaybeIdFunction(Field)
getMaybeCropTypeId: GetId[CropType] = g.getMaybeIdFunction(CropType)
getMaybeCropStageId: GetId[CropStage] = g.getMaybeIdFunction(CropStage)
getMaybeReportTypeId: GetId[ReportType] = g.getMaybeIdFunction(ReportType)

getFieldGroup: GetTable[FieldGroup] = g.getTableFunction(FieldGroup)
getField: GetTable[Field] = g.getTableFunction(Field)

insertFieldGroup: ReturnId[FieldGroup] = g.getInsertReturnIdFunction(FieldGroup)
insertField: ReturnId[Field] = g.getInsertReturnIdFunction(Field)
insertCropType: ReturnId[CropType] = g.getInsertReturnIdFunction(CropType)
insertCropStage = g.getInsertReturnIdFunction(CropStage)
insertReportType: ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)

insertPlanting = g.getInsertReturnKeyFunction(Planting, PlantingKey)
insertHarvest = g.getInsertReturnKeyFunction(Harvest, PlantingKey)
insertCropProgress = g.getInsertReturnKeyFunction(CropProgress, CropProgressKey)

insertOrGetFieldGroup = g.partialInsertOrGetId(getMaybeFieldGroupId, insertFieldGroup)
insertOrGetField = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetCropType = g.partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = g.partialInsertOrGetId(getMaybeCropStageId, insertCropStage)

getPlanting = g.getTableByKeyFunction(Planting, PlantingKey)
getHarvest = g.getTableByKeyFunction(Harvest, PlantingKey)
getCropProgress: GetTableByKey[CropProgressKey, CropProgress] = g.getTableByKeyFunction(
    CropProgress, CropProgressKey
)

insertOrGetPlanting = g.partialInsertOrGetKey(PlantingKey, getPlanting, insertPlanting)
insertOrGetHarvest = g.partialInsertOrGetKey(PlantingKey, getHarvest, insertHarvest)
insertOrGetCropProgress = g.partialInsertOrGetKey(
    CropProgressKey, getCropProgress, insertCropProgress
)

# spatiotemporal
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
