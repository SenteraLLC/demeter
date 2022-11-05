from typing import Union

from .types import ObservationValue, ObservationType, UnitType, Operation

AnyDataTable = Union[ObservationValue, Operation]

AnyTypeTable = Union[ObservationType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
