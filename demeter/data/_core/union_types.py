from typing import Union

from .._core.types import (  # ReportType,
    Act,
    CropType,
    Field,
    FieldTrial,
)
from .field_group import FieldGroup
from .st_types import (
    Geom,
    GeoSpatialKey,
    Key,
    TemporalKey,
)

AnyDataTable = Union[
    Geom, Field, FieldTrial, FieldGroup, GeoSpatialKey, TemporalKey, Key, Act
]

AnyTypeTable = CropType

# AnyKeyTable = Union[Planting, Harvest]

AnyTable = Union[
    AnyDataTable,
    AnyTypeTable,
    # AnyKeyTable,
]
