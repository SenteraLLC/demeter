"""Use `TableLookup` objects to map types to SQL table names"""
from ...db._lookup_types import TableLookup, sumMappings
from .._core.types import (  # ReportType,
    Act,
    CropType,
    Field,
    FieldTrial,
)
from .field_group import FieldGroup, FieldTrialGroup
from .st_types import (
    Geom,
    GeoSpatialKey,
    TemporalKey,
)

type_table_lookup: TableLookup = {
    CropType: "crop_type",
    # ReportType: "report_type",
}

data_table_lookup: TableLookup = {
    Geom: "geom",
    FieldGroup: "field_group",
    Field: "field",
    FieldTrialGroup: "field_trial_group",
    FieldTrial: "field_trial",
    GeoSpatialKey: "geospatial_key",
    TemporalKey: "temporal_key",
    Act: "act",
}

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

# key_table_lookup: TableLookup = {
#     # Planting: "planting",
#     # Harvest: "harvest",
# }
