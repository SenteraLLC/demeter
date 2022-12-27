from ...db._lookup_types import TableLookup, sumMappings
from .types import (
    Observation,
    ObservationType,
    UnitType,
)

type_table_lookup: TableLookup = {
    UnitType: "unit_type",
    ObservationType: "observation_type",
}

data_table_lookup: TableLookup = {
    Observation: "observation",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)
