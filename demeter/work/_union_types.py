from typing import Union

from ..db import TypeTable
from ._types.arguments import (
    Execution,
    HTTPArgument,
    KeywordArgument,
    ObservationArgument,
    S3InputArgument,
    S3OutputArgument,
)
from ._types.execution import ExecutionKey

AnyKeyTable = Union[
    ObservationArgument,
    HTTPArgument,
    S3InputArgument,
    S3OutputArgument,
    KeywordArgument,
    ExecutionKey,
]


class _Ignore(TypeTable):
    pass


AnyTypeTable = Union[_Ignore]

AnyDataTable = Union[Execution]

AnyIdTable = Union[AnyTypeTable, AnyDataTable]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]
