from typing import Optional, Callable, TypeVar, Any, Union

from ..types import local, inputs, function, execution, core

from .types_protocols import TableKey

AnyTypeTable = Union[local.AnyTypeTable, inputs.AnyTypeTable, function.FunctionType]

AnyDataTable = Union[local.AnyDataTable, inputs.S3Object, function.Function, execution.Execution, core.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.S3SubType]

AnyKeyTable = Union[local.AnyKeyTable,
                    inputs.S3ObjectKey,
                    inputs.S3TypeDataFrame,
                    function.AnyKeyTable,
                    execution.AnyKeyTable,
                   ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]


I = TypeVar('I', bound=AnyIdTable)
GetId = Callable[[Any, I], Optional[int]]
GetTable = Callable[[Any, int], I]
ReturnId = Callable[[Any, I], int]

S = TypeVar('S', bound=AnyKeyTable)
SK = TypeVar('SK', bound=TableKey)
ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]
