from ...db.lookup_types import TableLookup, KeyTableLookup

from .types import *

type_table_lookup : TableLookup = {
  UnitType   : "unit_type",
  LocalType  : "data_type",
  LocalGroup : "data_group",
}

data_table_lookup : TableLookup = {
  LocalValue    : "data_value",
}

from ...db.lookup_types import sumMappings

id_table_lookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : KeyTableLookup = { }

