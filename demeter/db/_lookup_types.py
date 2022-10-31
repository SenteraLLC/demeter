from typing import Dict, Mapping, Tuple, Type, cast

from . import Table, TableKey

TableLookup = Mapping[Type[Table], str]


def sumMappings(*ms: Mapping[Type[Table], str]) -> Mapping[Type[Table], str]:
    out: Dict[Type[Table], str] = {}
    for m in ms:
        out.update(m.items())
    return out
