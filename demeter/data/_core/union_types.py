from typing import Union

from .types import (
    CropType,
    CropStage,
    ReportType,
    Geom,
    Parcel,
    ParcelGroup,
    Field,
    GeoSpatialKey,
    TemporalKey,
    Planting,
    CropProgress,
    Key,
)

from .field_group import (
    FieldGroup,
)

AnyDataTable = Union[
    Geom, Parcel, ParcelGroup, Field, FieldGroup, GeoSpatialKey, TemporalKey, Key
]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
