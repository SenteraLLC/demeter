from typing import Union

from .._core.types import (
    CropType,
    # ReportType,
    Field,
    Act,
)

from .st_types import (
    Geom,
    GeoSpatialKey,
    TemporalKey,
    Key,
)

from .field_group import (
    FieldGroup,
)

AnyDataTable = Union[Geom, Field, FieldGroup, GeoSpatialKey, TemporalKey, Key, Act]

AnyTypeTable = CropType

# AnyKeyTable = Union[Planting, Harvest]

AnyTable = Union[
    AnyDataTable,
    AnyTypeTable,
    # AnyKeyTable,
]
