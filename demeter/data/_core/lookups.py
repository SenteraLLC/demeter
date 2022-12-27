from ...db._lookup_types import TableLookup, sumMappings
from .field_group import FieldGroup
from .st_types import (
    Geom,
    GeoSpatialKey,
    TemporalKey,
)
from .types import (  # ReportType,
    Act,
    CropType,
    Field,
    Harvest,
    Planting,
)

type_table_lookup: TableLookup = {
    CropType: "crop_type",
    # ReportType: "report_type",
}

data_table_lookup: TableLookup = {
    Geom: "geom",
    FieldGroup: "field_group",
    Field: "field",
    GeoSpatialKey: "geospatial_key",
    TemporalKey: "temporal_key",
    Act: "act",
}

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup: TableLookup = {
    Planting: "planting",
    Harvest: "harvest",
}
