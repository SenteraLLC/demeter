from typing import Union

from . import local
from . import core

AnyTypeTable = Union[local.AnyTypeTable, core.AnyTypeTable]

AnyDataTable = Union[local.AnyDataTable, core.AnyDataTable, ]

AnyKeyTable = Union[core.AnyKeyTable, ]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyKeyTable, AnyIdTable, ]

