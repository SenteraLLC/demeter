"""Use `TableLookup` objects to map types to SQL table names"""
from ...db._lookup_types import TableLookup, sumMappings
from .._core.types import (  # ReportType,
    Act,
    App,
    CropType,
    Field,
    FieldTrial,
    NutrientSource,
    Organization,
    Plot,
)
from .grouper import Grouper
from .st_types import (
    Geom,
    GeoSpatialKey,
    TemporalKey,
)

type_table_lookup: TableLookup = {
    CropType: "crop_type",
    NutrientSource: "nutrient_type",
    # ReportType: "report_type",
}

data_table_lookup: TableLookup = {
    Geom: "geom",
    Organization: "organization",
    Grouper: "grouper",
    Field: "field",
    FieldTrial: "field_trial",
    Plot: "plot",
    GeoSpatialKey: "geospatial_key",
    TemporalKey: "temporal_key",
    Act: "act",
    App: "app",
}

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

# key_table_lookup: TableLookup = {
#     # Planting: "planting",
#     # Harvest: "harvest",
# }
