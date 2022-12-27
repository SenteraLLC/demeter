from typing import Union

from demeter.data import ObservationType

from .._inputs.types import (
    HTTPType,
    S3Object,
    S3ObjectKey,
    S3Output,
    S3Type,
    S3TypeDataFrame,
)

AnyDataTable = Union[S3Output, S3Object, S3ObjectKey]
AnyKeyTable = Union[S3ObjectKey, S3TypeDataFrame]
AnyTypeTable = Union[HTTPType, S3Type, ObservationType, S3TypeDataFrame]
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
