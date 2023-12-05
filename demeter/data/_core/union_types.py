from typing import Union

from .._core.types import (  # ReportType,
    Act,
    CropType,
    Field,
    FieldTrial,
    Plot,
)
from .field_group import FieldGroup, FieldTrialGroup
from .st_types import (
    Geom,
    GeoSpatialKey,
    Key,
    TemporalKey,
)

AnyDataTable = Union[
    Geom,
    Field,
    FieldTrial,
    Plot,
    FieldGroup,
    FieldTrialGroup,
    GeoSpatialKey,
    TemporalKey,
    Key,
    Act,
]

AnyTypeTable = CropType

# AnyKeyTable = Union[Planting, Harvest]

AnyTable = Union[
    AnyDataTable,
    AnyTypeTable,
    # AnyKeyTable,
]
