from typing import Union

from .types import Observation, ObservationType, UnitType, Act

AnyDataTable = Union[Observation, Act]

AnyTypeTable = Union[ObservationType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
