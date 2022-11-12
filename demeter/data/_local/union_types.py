from typing import Union

from .types import *

AnyDataTable = Union[LocalValue]

AnyTypeTable = Union[LocalType, UnitType, LocalGroup]

AnyTable = Union[AnyDataTable, AnyTypeTable]
