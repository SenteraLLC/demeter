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

AnyDataTable = Union[Geom, Field, GeoSpatialKey, TemporalKey, Key]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
