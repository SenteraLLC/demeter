from typing import Union

from .field_group import FieldGroup
from .st_types import (
    Geom,
    GeoSpatialKey,
    Key,
    TemporalKey,
)
from .types import (  # ReportType,
    Act,
    CropType,
    Field,
    Harvest,
    Planting,
)

AnyDataTable = Union[Geom, Field, FieldGroup, GeoSpatialKey, TemporalKey, Key, Act]

AnyTypeTable = CropType

AnyKeyTable = Union[Planting, Harvest]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
