from . import api_protocols
from . import type_lookups

assert(set(api_protocols.AnyTypeTable.__args__) == set(type_lookups.type_table_lookup.keys())) # type: ignore

assert(set(api_protocols.AnyDataTable.__args__) == set(type_lookups.data_table_lookup.keys())) # type: ignore

assert(set(api_protocols.AnyKeyTable.__args__) == set(type_lookups.key_table_lookup.keys())) # type: ignore

