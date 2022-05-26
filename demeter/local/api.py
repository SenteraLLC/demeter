from functools import partial

from ..database.api_protocols import GetId, GetTable, ReturnId, ReturnKey
from ..database.generators import getMaybeIdFunction, getTableFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction, insertOrGetType

from .types import LocalValue, UnitType, LocalType, CropType, CropStage, ReportType, LocalGroup
from .types import  Planting, Harvest, CropProgress, PlantingKey, HarvestKey, CropProgressKey


getMaybeLocalValue   : GetId[LocalValue] = getMaybeIdFunction(LocalValue)
getMaybeUnitTypeId   : GetId[UnitType]   = getMaybeIdFunction(UnitType)
getMaybeLocalTypeId  : GetId[LocalType]  = getMaybeIdFunction(LocalType)
getMaybeLocalValueId  : GetId[LocalValue]  = getMaybeIdFunction(LocalValue)
getMaybeCropTypeId   : GetId[CropType]   = getMaybeIdFunction(CropType)
getMaybeCropStageId  : GetId[CropStage]  = getMaybeIdFunction(CropStage)
getMaybeReportTypeId : GetId[ReportType] = getMaybeIdFunction(ReportType)
getMaybeLocalGroupId : GetId[LocalGroup] = getMaybeIdFunction(LocalGroup)

getLocalType  : GetTable[LocalType] = getTableFunction(LocalType)

insertLocalValue : ReturnId[LocalValue] = getInsertReturnIdFunction(LocalValue)
insertUnitType   : ReturnId[UnitType]   = getInsertReturnIdFunction(UnitType)
insertLocalType  : ReturnId[LocalType]  = getInsertReturnIdFunction(LocalType)
insertCropType   : ReturnId[CropType]   = getInsertReturnIdFunction(CropType)
insertCropStage  : ReturnId[CropStage]  = getInsertReturnIdFunction(CropStage)
insertReportType : ReturnId[ReportType] = getInsertReturnIdFunction(ReportType)
insertLocalGroup : ReturnId[LocalGroup] = getInsertReturnIdFunction(LocalGroup)

insertPlanting     : ReturnKey[Planting, PlantingKey] = getInsertReturnKeyFunction(Planting) # type: ignore
insertHarvest      : ReturnKey[Harvest, HarvestKey] = getInsertReturnKeyFunction(Harvest) # type: ignore
insertCropProgress : ReturnKey[CropProgress, CropProgressKey] = getInsertReturnKeyFunction(CropProgress) # type: ignore

insertOrGetUnitType = partial(insertOrGetType, getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = partial(insertOrGetType, getMaybeLocalTypeId, insertLocalType)
insertOrGetLocalValue = partial(insertOrGetType, getMaybeLocalValueId, insertLocalValue)
insertOrGetCropType = partial(insertOrGetType, getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = partial(insertOrGetType, getMaybeCropStageId, insertCropStage)
insertOrGetLocalGroup = partial(insertOrGetType, getMaybeLocalGroupId, insertLocalGroup)

