from .types import UnitType, ObservationType, ObservationValue, Operation

from ...db._lookup_types import TableLookup
from ...db._lookup_types import sumMappings

type_table_lookup: TableLookup = {
    UnitType: "unit_type",
    ObservationType: "local_type",
}

data_table_lookup: TableLookup = {
    ObservationValue: "local_value",
    Operation: "operation",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)
