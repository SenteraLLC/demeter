from .types import UnitType, ObservationType, Observation

from ...db._lookup_types import TableLookup
from ...db._lookup_types import sumMappings

type_table_lookup: TableLookup = {
    UnitType: "unit_type",
    ObservationType: "observation_type",
}

data_table_lookup: TableLookup = {
    Observation: "observation",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)
