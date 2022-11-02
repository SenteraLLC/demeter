from . import lookups as _lookups

from ...db._generic_types import GetId, GetTable, GetTableByKey, ReturnId
from ...db._postgres import SQLGenerator

from .types import (
    CropProgress,
    CropProgressKey,
    CropStage,
    CropType,
    Parcel,
    ParcelGroup,
    Geom,
    GeoSpatialKey,
    Planting,
    PlantingKey,
    ReportType,
    TemporalKey,
)

g = SQLGenerator(
    "demeter.data",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
    key_table_lookup=_lookups.key_table_lookup,
)

getMaybeParcelId = g.getMaybeIdFunction(Parcel)
getMaybeParcelGroupId = g.getMaybeIdFunction(ParcelGroup)
getMaybeGeoSpatialKeyId: GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId: GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)
getMaybeCropTypeId: GetId[CropType] = g.getMaybeIdFunction(CropType)
getMaybeCropStageId: GetId[CropStage] = g.getMaybeIdFunction(CropStage)
getMaybeReportTypeId: GetId[ReportType] = g.getMaybeIdFunction(ReportType)

getParcel: GetTable[Parcel] = g.getTableFunction(Parcel)
getParcelGroup: GetTable[ParcelGroup] = g.getTableFunction(ParcelGroup)
getGeom: GetTable[Geom] = g.getTableFunction(Geom)

insertParcel: ReturnId[Parcel] = g.getInsertReturnIdFunction(Parcel)
insertParcelGroup: ReturnId[ParcelGroup] = g.getInsertReturnIdFunction(ParcelGroup)
insertGeoSpatialKey: ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(
    GeoSpatialKey
)
insertTemporalKey: ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)
insertCropType: ReturnId[CropType] = g.getInsertReturnIdFunction(CropType)
insertCropStage = g.getInsertReturnIdFunction(CropStage)
insertReportType: ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)

insertPlanting = g.getInsertReturnKeyFunction(Planting, PlantingKey)
insertCropProgress = g.getInsertReturnKeyFunction(CropProgress, CropProgressKey)

insertOrGetGeoSpatialKey = g.partialInsertOrGetId(
    getMaybeGeoSpatialKeyId, insertGeoSpatialKey
)
insertOrGetTemporalKey = g.partialInsertOrGetId(
    getMaybeTemporalKeyId, insertTemporalKey
)
insertOrGetParcel = g.partialInsertOrGetId(getMaybeParcelId, insertParcel)
insertOrGetParcelGroup = g.partialInsertOrGetId(
    getMaybeParcelGroupId, insertParcelGroup
)
insertOrGetCropType = g.partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = g.partialInsertOrGetId(getMaybeCropStageId, insertCropStage)

getPlanting = g.getTableByKeyFunction(Planting, PlantingKey)
getCropProgress: GetTableByKey[CropProgressKey, CropProgress] = g.getTableByKeyFunction(
    CropProgress, CropProgressKey
)

insertOrGetPlanting = g.partialInsertOrGetKey(PlantingKey, getPlanting, insertPlanting)
insertOrGetCropProgress = g.partialInsertOrGetKey(
    CropProgressKey, getCropProgress, insertCropProgress
)
