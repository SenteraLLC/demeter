from typing import Union

from .types import (
    CropType,
    CropStage,
    ReportType,
    Geom,
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

AnyDataTable = Union[Geom, Field, FieldGroup, GeoSpatialKey, TemporalKey, Key]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
