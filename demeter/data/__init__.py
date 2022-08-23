from ._core import *
from ._local import *

from . import _core as core
from . import _local as local

__all__ = [
  # core
  'getMaybeFieldId',
  'getMaybeOwnerId',
  'getMaybeGrowerId',
  'getMaybeGeoSpatialKeyId',
  'getMaybeTemporalKeyId',

  'getField',
  'getOwner',
  'getGeom',

  'insertField',
  'insertOwner',
  'insertGrower',
  'insertGeoSpatialKey',
  'insertTemporalKey',

  'insertOrGetGeoSpatialKey',
  'insertOrGetTemporalKey',
  'insertOrGetOwner',
  'insertOrGetGrower',
  'insertOrGetField',

  'getMaybeDuplicateGeom',
  'insertOrGetGeom',

  # core types
  'Point',
  'Line',
  'Polygon',
  'MultiPolygon',
  'Coordinates',
  'Geom',
  'Owner',
  'Grower',
  'Field',
  'GeoSpatialKey',
  'TemporalKey',
  'Key',
  'KeyIds',

  # local
  'getMaybeLocalValue',
  'getMaybeUnitTypeId',
  'getMaybeLocalTypeId',
  'getMaybeLocalValueId',
  'getMaybeCropTypeId',
  'getMaybeCropStageId',
  'getMaybeReportTypeId',
  'getMaybeLocalGroupId',

  'getLocalType',

  'insertLocalValue',
  'insertUnitType',
  'insertLocalType',
  'insertCropType',
  'insertCropStage',
  'insertReportType',
  'insertLocalGroup',

  'insertPlanting',
  'insertHarvest',
  'insertCropProgress',

  'insertOrGetUnitType',
  'insertOrGetLocalType',
  'insertOrGetLocalValue',
  'insertOrGetCropType',
  'insertOrGetCropStage',
  'insertOrGetLocalGroup',

  # local types
  'LocalValue',

  'LocalType',
  'UnitType',
  'ReportType',
  'LocalGroup',
  'CropType',
  'CropStage',

  'PlantHarvestKey',
  'PlantingKey',
  'HarvestKey',
  'CropProgressKey',

  'Planting',
  'Harvest',
  'CropProgress',

]

