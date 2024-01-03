from typing import Union

from .._observation.types import (
    S3,
    Observation,
    ObservationType,
    UnitType,
)

AnyDataTable = Union[S3, Observation]

AnyTypeTable = Union[ObservationType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
