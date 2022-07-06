from ...db.generic_types import GetId, GetTable, ReturnId, ReturnKey
from ...db.type_to_sql import getMaybeIdFunction, getTableFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction, partialInsertOrGetId

from . import types

from .types import LocalValue, UnitType, LocalType, CropType, CropStage, ReportType, LocalGroup
from .types import Planting, Harvest, CropProgress, PlantingKey, HarvestKey, CropProgressKey


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

insertPlanting     : ReturnKey[Planting, PlantingKey] = getInsertReturnKeyFunction(Planting)
insertHarvest      : ReturnKey[Harvest, HarvestKey] = getInsertReturnKeyFunction(Harvest)
insertCropProgress : ReturnKey[CropProgress, CropProgressKey] = getInsertReturnKeyFunction(CropProgress)

insertOrGetUnitType = partialInsertOrGetId(getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = partialInsertOrGetId(getMaybeLocalTypeId, insertLocalType)
insertOrGetLocalValue = partialInsertOrGetId(getMaybeLocalValueId, insertLocalValue)
insertOrGetCropType = partialInsertOrGetId(getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = partialInsertOrGetId(getMaybeCropStageId, insertCropStage)
insertOrGetLocalGroup = partialInsertOrGetId(getMaybeLocalGroupId, insertLocalGroup)


#from ...db.generic_types import TypeToFunction
#
## TODO: Incomplete
#TYPE_TO_INSERT_FN : TypeToFunction = { # type: ignore
#  LocalType  : insertOrGetLocalType,
#  UnitType   : insertOrGetUnitType,
#  LocalValue : insertOrGetLocalValue,
#}
