from typing import Union

from .types import *

AnyDataTable = Union[S3Output, S3Object, S3ObjectKey]
AnyKeyTable = Union[S3ObjectKey, S3TypeDataFrame]
AnyTypeTable = Union[HTTPType, S3Type, LocalType, S3TypeDataFrame]
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]

