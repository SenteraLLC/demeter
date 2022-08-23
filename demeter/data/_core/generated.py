
from ...db._postgres import SQLGenerator

from . import lookups as _lookups
g = SQLGenerator("demeter.data",
                 type_table_lookup = _lookups.type_table_lookup,
                 data_table_lookup = _lookups.data_table_lookup,
                 id_table_lookup = _lookups.id_table_lookup,
                 key_table_lookup = _lookups.key_table_lookup,
                )

from ...db._generic_types import GetId, GetTable, ReturnId, ReturnKey, GetTableByKey

from .types import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom, Coordinates, Polygon, MultiPolygon, Point, Line
from .types import CropType, CropStage, ReportType, Planting, Harvest, CropProgress, PlantingKey, HarvestKey, CropProgressKey


getMaybeFieldId = g.getMaybeIdFunction(Field)
getMaybeOwnerId          : GetId[Owner]      = g.getMaybeIdFunction(Owner)
getMaybeGrowerId         : GetId[Grower]      = g.getMaybeIdFunction(Grower)
getMaybeGeoSpatialKeyId  : GetId[GeoSpatialKey] = g.getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId    : GetId[TemporalKey] = g.getMaybeIdFunction(TemporalKey)
getMaybeCropTypeId   : GetId[CropType]   = g.getMaybeIdFunction(CropType)
getMaybeCropStageId  : GetId[CropStage]  = g.getMaybeIdFunction(CropStage)
getMaybeReportTypeId : GetId[ReportType] = g.getMaybeIdFunction(ReportType)

getField      : GetTable[Field]    = g.getTableFunction(Field)
getOwner      : GetTable[Owner]    = g.getTableFunction(Owner)
getGeom       : GetTable[Geom]     = g.getTableFunction(Geom)
print("GET GEOM ANNOS: ",getGeom.__annotations__)

insertField          : ReturnId[Field]      = g.getInsertReturnIdFunction(Field)
insertOwner          : ReturnId[Owner]      = g.getInsertReturnIdFunction(Owner)
insertGrower         : ReturnId[Grower]     = g.getInsertReturnIdFunction(Grower)
insertGeoSpatialKey : ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(GeoSpatialKey)
insertTemporalKey : ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)
insertCropType   : ReturnId[CropType]   = g.getInsertReturnIdFunction(CropType)
insertCropStage = g.getInsertReturnIdFunction(CropStage)
insertReportType : ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)

#insertPlanting     : ReturnKey[Planting, PlantingKey] = g.getInsertReturnKeyFunction(Planting)
#insertHarvest      : ReturnKey[Harvest, HarvestKey] = g.getInsertReturnKeyFunction(Harvest)
#insertCropProgress = getInsertReturnKeyFunction3(CropProgress, CropProgressKey, _lookups.key_table_lookup2)
#reveal_type(g.getInsertReturnKeyFunction)
insertPlanting = g.getInsertReturnKeyFunction(Planting, PlantingKey)
print("PLANTING ANNOS: ",insertPlanting.__annotations__)
insertHarvest  = g.getInsertReturnKeyFunction(Harvest, HarvestKey)
insertCropProgress = g.getInsertReturnKeyFunction(CropProgress, CropProgressKey)


insertOrGetGeoSpatialKey = g.partialInsertOrGetId(getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = g.partialInsertOrGetId(getMaybeTemporalKeyId, insertTemporalKey)
insertOrGetOwner = g.partialInsertOrGetId(getMaybeOwnerId, insertOwner)
insertOrGetGrower = g.partialInsertOrGetId(getMaybeGrowerId, insertGrower)
insertOrGetField = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetCropType = g.partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = g.partialInsertOrGetId(getMaybeCropStageId, insertCropStage)

getPlanting = g.getTableByKeyFunction(Planting, PlantingKey)
getCropProgress : GetTableByKey[CropProgressKey, CropProgress] = g.getTableByKeyFunction(CropProgress, CropProgressKey)

insertOrGetPlanting = g.partialInsertOrGetKey(PlantingKey, getPlanting, insertPlanting)
insertOrGetCropProgress = g.partialInsertOrGetKey(CropProgressKey, getCropProgress, insertCropProgress)

