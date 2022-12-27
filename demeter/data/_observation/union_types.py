from typing import Union

from .types import Observation, ObservationType, UnitType

AnyDataTable = Observation

AnyTypeTable = Union[ObservationType, UnitType]

AnyTable = Union[AnyDataTable, AnyTypeTable]
