from .types import UnitType, LocalType, LocalGroup, LocalValue, Operation

from ...db._lookup_types import TableLookup
from ...db._lookup_types import sumMappings

type_table_lookup: TableLookup = {
    UnitType: "unit_type",
    LocalType: "local_type",
    LocalGroup: "local_group",
}

data_table_lookup: TableLookup = {
    LocalValue: "local_value",
    Operation: "operation",
}

id_table_lookup: TableLookup = sumMappings(type_table_lookup, data_table_lookup)
