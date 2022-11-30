from typing import Union

from .types import ObservationValue, ObservationType, UnitType, Act

AnyDataTable = Union[ObservationValue, Act]

AnyTypeTable = Union[ObservationType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
