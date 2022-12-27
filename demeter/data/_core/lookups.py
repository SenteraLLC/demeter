from ...db._lookup_types import sumMappings
from ...db._lookup_types import TableLookup

from .types import (
    CropType,
    # ReportType,
    Field,
    Planting,
    Harvest,
    Act,
)

from .st_types import (
    GeoSpatialKey,
    TemporalKey,
    Geom,
)

from .field_group import FieldGroup

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
