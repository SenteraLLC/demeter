from typing import Union

from .types import LocalValue, LocalType, UnitType, LocalGroup, Operation

AnyDataTable = Union[LocalValue, Operation]

AnyTypeTable = Union[LocalType, UnitType, LocalGroup]

AnyTable = Union[AnyDataTable, AnyTypeTable]
