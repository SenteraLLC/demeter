from typing import Union

from ._local import union_types as local
from ._core import union_types as core

AnyTypeTable = Union[local.AnyTypeTable, core.AnyTypeTable]

AnyDataTable = Union[local.AnyDataTable, core.AnyDataTable, ]

AnyKeyTable = Union[core.AnyKeyTable, ]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyKeyTable, AnyIdTable, ]

