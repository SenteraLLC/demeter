from ...db.lookup_types import TableLookup, KeyTableLookup

from .types import *

type_table_lookup : TableLookup = {
  UnitType   : "unit_type",
  LocalType  : "local_type",
  LocalGroup : "local_group",
}

data_table_lookup : TableLookup = {
  LocalValue    : "local_value",
}

from ...db.lookup_types import sumMappings

id_table_lookup : TableLookup = sumMappings(type_table_lookup, data_table_lookup)

key_table_lookup : KeyTableLookup = { }

