from typing import (
    Dict,
    Mapping,
    Type,
)

from . import Table

# Mapping from defined Demeter types (which must inherit from `db.Table`
# to SQL table names (strings)

TableLookup = Mapping[Type[Table], str]


## FIXME: Couldn't the input and output types of `sumMappings` just be `TableLookup` objects?
def sumMappings(*ms: Mapping[Type[Table], str]) -> Mapping[Type[Table], str]:
    """Simple concatenation of passed `TableLookup` objects"""
    out: Dict[Type[Table], str] = {}
    for m in ms:
        out.update(m.items())
    return out
