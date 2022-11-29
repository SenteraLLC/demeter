from typing import Union

from .types import *

AnyParameter = Union[
    LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter
]

AnyDataTable = Union[Function, AnyParameter]
AnyTypeTable = Union[FunctionType]
AnyKeyTable = Union[AnyParameter]
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
