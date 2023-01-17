from typing import Union

from ._core import union_types as core
from ._observation import union_types as observation

AnyTypeTable = Union[observation.AnyTypeTable, core.AnyTypeTable]

AnyDataTable = Union[
    observation.AnyDataTable,
    core.AnyDataTable,
]

# AnyKeyTable = Union[
#     core.AnyKeyTable,
# ]

AnyIdTable = Union[
    AnyTypeTable,
    AnyDataTable,
]

AnyTable = Union[
    AnyTypeTable,
    AnyDataTable,
    # AnyKeyTable,
    AnyIdTable,
]
