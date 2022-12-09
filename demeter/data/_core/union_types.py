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
    Harvest,
    CropProgress,
    Key,
)

from .field_group import (
    FieldGroup,
)

AnyDataTable = Union[Geom, Field, FieldGroup, GeoSpatialKey, TemporalKey, Key]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, Harvest, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
