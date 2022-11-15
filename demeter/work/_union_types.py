from typing import Union

from ..db import TypeTable
from ._types import *

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
