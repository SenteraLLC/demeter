from typing import Union

from .types import *

AnyDataTable = Union[Geom, Field, FieldGroup, GeoSpatialKey, TemporalKey, Act, Key]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]
