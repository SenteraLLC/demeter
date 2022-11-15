from typing import Union

from .types import LocalValue, LocalType, UnitType, Act

AnyDataTable = Union[LocalValue, Act]

AnyTypeTable = Union[LocalType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
