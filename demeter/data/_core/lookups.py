from ...db._lookup_types import sumMappings
from ...db._lookup_types import TableLookup

from .types import (
    CropType,
    CropStage,
    ReportType,
    Geom,
    Parcel,
    ParcelGroup,
    GeoSpatialKey,
    TemporalKey,
    Planting,
    CropProgress,
)

type_table_lookup: TableLookup = {
    CropType: "crop_type",
    CropStage: "crop_stage",
    ReportType: "report_type",
}

data_table_lookup: TableLookup = {
    Geom: "geom",
    Parcel: "parcel",
    ParcelGroup: "parcel_group",
    GeoSpatialKey: "geospatial_key",
    TemporalKey: "temporal_key",
}


id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    Planting: "planting",
    CropProgress: "crop_progress",
}
