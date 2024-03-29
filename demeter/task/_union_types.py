from typing import Union

from ._function import union_types as function
from ._inputs import union_types as inputs

AnyTypeTable = Union[inputs.AnyTypeTable, function.AnyTypeTable]

AnyDataTable = Union[inputs.AnyDataTable, function.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.S3SubType]

AnyKeyTable = Union[
    inputs.AnyKeyTable,
    function.AnyKeyTable,
]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]
