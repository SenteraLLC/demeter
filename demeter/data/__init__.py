from ._core import *
from ._local import *

from . import _core as core
from . import _local as local

__all__ = [
  # core
  'getMaybeFieldId',
  'getMaybeFieldGroupId',
  'getMaybeGeoSpatialKeyId',
  'getMaybeTemporalKeyId',

  'getField',
  'getGeom',

  'insertField',
  'insertFieldGroup',
  'insertFieldGroupStrict',
  'insertGeoSpatialKey',
  'insertTemporalKey',

  'insertOrGetGeoSpatialKey',
  'insertOrGetTemporalKey',
  'insertOrGetFieldGroup',
  'insertOrGetFieldGroupStrict',
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
  'FieldGroup',
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

