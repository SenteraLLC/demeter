from ...db._lookup_types import TableLookup

from .types import *

type_table_lookup : TableLookup = {
  CropType   : "crop_type",
  CropStage  : "crop_stage",
  ReportType : "report_type",
}

data_table_lookup : TableLookup = {
  Geom          : "geom",
  Field         : "field",
  FieldGroup    : "field_group",
  GeoSpatialKey : "geospatial_key",
  TemporalKey   : "temporal_key",
}

from ...db._lookup_types import sumMappings

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : TableLookup = {
  Planting     : "planting",
  Harvest      : "harvest",
  CropProgress : "crop_progress",
}

