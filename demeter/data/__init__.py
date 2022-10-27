from ._core import *
from ._local import *

from . import _core as core
from . import _local as local

__all__ = [
  # core
  'getMaybeFieldId',
  'getMaybeGeoSpatialKeyId',
  'getMaybeTemporalKeyId',

  'getField',
  'getGeom',

  'insertField',
  'insertGeoSpatialKey',
  'insertTemporalKey',

  'insertOrGetGeoSpatialKey',
  'insertOrGetTemporalKey',
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
  'insertAct',
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

  'PlantingKey',
  'CropProgressKey',

  'Planting',
  'Act',
  'CropProgress',

]

