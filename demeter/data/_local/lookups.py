from .types import UnitType, ObservationType, ObservationValue, Act

from ...db._lookup_types import TableLookup
from ...db._lookup_types import sumMappings

type_table_lookup: TableLookup = {
    UnitType: "unit_type",
    ObservationType: "local_type",
}

data_table_lookup: TableLookup = {
    ObservationValue: "local_value",
    Act: "act",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)
