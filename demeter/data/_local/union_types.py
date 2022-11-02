from typing import Union

from .types import LocalValue, LocalType, UnitType, LocalGroup, Act

AnyDataTable = Union[LocalValue, Act]

AnyTypeTable = Union[LocalType, UnitType, LocalGroup]

AnyTable = Union[AnyDataTable, AnyTypeTable]
