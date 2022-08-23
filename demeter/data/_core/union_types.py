from typing import Union

from .types import *

AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey, Key]

AnyTypeTable = Union[CropType, CropStage, ReportType]

AnyKeyTable = Union[Planting, Harvest, CropProgress]

AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]

