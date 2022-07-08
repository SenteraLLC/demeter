from ...db.lookup_types import TableLookup, KeyTableLookup

from .types import *

type_table_lookup : TableLookup = {
  CropType   : "crop_type",
  CropStage  : "crop_stage",
  ReportType : "report_type",
}

data_table_lookup : TableLookup = {
  Geom          : "geom",
  Owner         : "owner",
  Grower        : "grower",
  Field         : "field",
  GeoSpatialKey : "geospatial_key",
  TemporalKey   : "temporal_key",
}

from ...db.lookup_types import sumMappings

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : KeyTableLookup = {
  Planting     : ("planting", PlantingKey),
  Harvest      : ("harvest",  HarvestKey),
  CropProgress : ("crop_progress", CropProgressKey),
}

