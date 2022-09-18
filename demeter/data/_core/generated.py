
from ...db._postgres import SQLGenerator

from . import lookups as _lookups
g = SQLGenerator("demeter.data",
                 type_table_lookup = _lookups.type_table_lookup,
                 data_table_lookup = _lookups.data_table_lookup,
                 id_table_lookup = _lookups.id_table_lookup,
                 key_table_lookup = _lookups.key_table_lookup,
                )

from ...db._generic_types import GetId, GetTable, ReturnId, ReturnKey, GetTableByKey

from .types import Field, FieldGroup, GeoSpatialKey, TemporalKey, Geom, Coordinates, Polygon, MultiPolygon, Point, Line
from .types import CropType, CropStage, ReportType, Planting, Act, CropProgress, PlantingKey, CropProgressKey


getMaybeFieldId = g.getMaybeIdFunction(Field)
getMaybeFieldGroupId          : GetId[FieldGroup]      = g.getMaybeIdFunction(FieldGroup)
getMaybeGeoSpatialKeyId  : GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId    : GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)
getMaybeCropTypeId   : GetId[CropType]   = g.getMaybeIdFunction(CropType)
getMaybeCropStageId  : GetId[CropStage]  = g.getMaybeIdFunction(CropStage)
getMaybeReportTypeId : GetId[ReportType] = g.getMaybeIdFunction(ReportType)

getField      : GetTable[Field]    = g.getTableFunction(Field)
getFieldGroup      : GetTable[FieldGroup]    = g.getTableFunction(FieldGroup)
getGeom       : GetTable[Geom]     = g.getTableFunction(Geom)

insertField          : ReturnId[Field]      = g.getInsertReturnIdFunction(Field)
insertFieldGroup    : ReturnId[FieldGroup]      = g.getInsertReturnIdFunction(FieldGroup)
insertGeoSpatialKey : ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(GeoSpatialKey)
insertTemporalKey : ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)
insertCropType   : ReturnId[CropType]   = g.getInsertReturnIdFunction(CropType)
insertCropStage = g.getInsertReturnIdFunction(CropStage)
insertReportType : ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)
insertAct  = g.getInsertReturnIdFunction(Act)

insertPlanting = g.getInsertReturnKeyFunction(Planting, PlantingKey)
insertCropProgress = g.getInsertReturnKeyFunction(CropProgress, CropProgressKey)

insertOrGetGeoSpatialKey = g.partialInsertOrGetId(getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = g.partialInsertOrGetId(getMaybeTemporalKeyId, insertTemporalKey)
insertOrGetField = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetFieldGroup = g.partialInsertOrGetId(getMaybeFieldGroupId, insertFieldGroup)
insertOrGetCropType = g.partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = g.partialInsertOrGetId(getMaybeCropStageId, insertCropStage)

getPlanting = g.getTableByKeyFunction(Planting, PlantingKey)
getCropProgress : GetTableByKey[CropProgressKey, CropProgress] = g.getTableByKeyFunction(CropProgress, CropProgressKey)

insertOrGetPlanting = g.partialInsertOrGetKey(PlantingKey, getPlanting, insertPlanting)
insertOrGetCropProgress = g.partialInsertOrGetKey(CropProgressKey, getCropProgress, insertCropProgress)


