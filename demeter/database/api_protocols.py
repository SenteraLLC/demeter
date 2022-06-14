from typing import Optional, Callable, TypeVar, Any, Union

from ..types import local, inputs, function, execution, core

from .types_protocols import Table, TableKey, TableId

T = TypeVar('T', bound=Table)

AnyTypeTable = Union[local.AnyTypeTable, inputs.AnyTypeTable, function.FunctionType]

AnyDataTable = Union[local.AnyDataTable, inputs.S3Object, inputs.S3Output, function.Function, execution.Execution, core.AnyDataTable]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, inputs.S3SubType]

AnyKeyTable = Union[local.AnyKeyTable,
                    inputs.S3ObjectKey,
                    inputs.S3TypeDataFrame,
                    function.AnyKeyTable,
                    execution.AnyKeyTable,
                   ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

SomeKey = Union[TableKey, TableId]

R = TypeVar('R')
InsertFn = Callable[[Any, T], R]

I = TypeVar('I', bound=AnyIdTable)
GetId = Callable[[Any, I], Optional[TableId]]
GetTable = Callable[[Any, TableId], I]
ReturnId = Callable[[Any, I], TableId]
#ReturnId = InsertFn[I, TableId]

S = TypeVar('S', bound=AnyKeyTable)
SK = TypeVar('SK', bound=SomeKey)

ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]

