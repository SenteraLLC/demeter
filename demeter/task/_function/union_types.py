from typing import Union

from .._function.types import (
    Function,
    FunctionType,
    HTTPParameter,
    KeywordParameter,
    ObservationParameter,
    S3InputParameter,
    S3OutputParameter,
)

AnyParameter = Union[
    ObservationParameter,
    HTTPParameter,
    S3InputParameter,
    S3OutputParameter,
    KeywordParameter,
]

AnyDataTable = Union[Function, AnyParameter]
AnyTypeTable = FunctionType
AnyKeyTable = AnyParameter
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
