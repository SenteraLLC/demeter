from .types import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom, Coordinates, Polygon, MultiPolygon, Point, Line
from .types import CropType, CropStage, ReportType, Planting, Harvest, CropProgress, PlantingKey, HarvestKey, CropProgressKey

from ...db.generic_types import GetId, GetTable, ReturnId, ReturnSameKey, ReturnKey

from .lookups import *
from ...db import Generator

g = Generator(type_table_lookup = type_table_lookup,
              data_table_lookup = data_table_lookup,
              id_table_lookup = id_table_lookup,
              key_table_lookup = key_table_lookup,
             )

getMaybeFieldId          : GetId[Field]      = g.getMaybeIdFunction(Field)
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

insertField          : ReturnId[Field]      = g.getInsertReturnIdFunction(Field)
insertOwner          : ReturnId[Owner]      = g.getInsertReturnIdFunction(Owner)
insertGrower         : ReturnId[Grower]     = g.getInsertReturnIdFunction(Grower)
insertGeoSpatialKey : ReturnId[GeoSpatialKey] = g.getInsertReturnIdFunction(GeoSpatialKey)
insertTemporalKey : ReturnId[TemporalKey] = g.getInsertReturnIdFunction(TemporalKey)
insertCropType   : ReturnId[CropType]   = g.getInsertReturnIdFunction(CropType)
insertCropStage  : ReturnId[CropStage]  = g.getInsertReturnIdFunction(CropStage)
insertReportType : ReturnId[ReportType] = g.getInsertReturnIdFunction(ReportType)

insertPlanting     : ReturnKey[Planting, PlantingKey] = g.getInsertReturnKeyFunction(Planting)
insertHarvest      : ReturnKey[Harvest, HarvestKey] = g.getInsertReturnKeyFunction(Harvest)
insertCropProgress : ReturnKey[CropProgress, CropProgressKey] = g.getInsertReturnKeyFunction(CropProgress)

insertOrGetGeoSpatialKey = g.partialInsertOrGetId(getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = g.partialInsertOrGetId(getMaybeTemporalKeyId, insertTemporalKey)
insertOrGetOwner = g.partialInsertOrGetId(getMaybeOwnerId, insertOwner)
insertOrGetGrower = g.partialInsertOrGetId(getMaybeGrowerId, insertGrower)
insertOrGetField = g.partialInsertOrGetId(getMaybeFieldId, insertField)
insertOrGetCropType = g.partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = g.partialInsertOrGetId(getMaybeCropStageId, insertCropStage)

