import json
from dataclasses import InitVar, asdict, dataclass, field
from datetime import date, datetime
from typing import Literal, Mapping, Optional, Tuple, Union

from ... import db

Point = Tuple[float, float]
Line = Tuple[Point, ...]
Polygon = Line
MultiPolygon = Tuple[Polygon, ...]
# Postgis won't accept a lone Polygon
Coordinates = Union[Point, Line, MultiPolygon]


# TODO: Replace these with their proper geospatial library equivalents


@dataclass(frozen=True)
class CRS:
    type: Literal["name"]  # noqa
    properties: Mapping[Literal["name"], str]


@dataclass(frozen=True)
class GeomImpl:
    type: str  # noqa
    coordinates: Coordinates
    crs: CRS


@dataclass(frozen=True)
class Geom(db.Table):
    crs_name: InitVar[str]
    type: InitVar[str]  # noqa
    coordinates: InitVar[Coordinates]

    geom: str = field(init=False)

    def __post_init__(
        self,
        crs_name: str,
        type: str,  # noqa
        coordinates: Coordinates,
    ) -> None:
        crs = CRS(
            type="name",
            properties={"name": crs_name},
        )
        impl = GeomImpl(type=type, coordinates=coordinates, crs=crs)
        geom = json.dumps(impl, default=asdict)
        object.__setattr__(self, "geom", geom)


@dataclass(frozen=True)
class Field(db.Detailed):
    geom_id: db.TableId
    name : str
    field_group_id : Optional[db.TableId] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class GeoSpatialKey(db.Table):
    geom_id: db.TableId
    field_id: Optional[db.TableId]


@dataclass(frozen=True)
class TemporalKey(db.Table):
    start_date: date
    end_date: date


@dataclass(frozen=True)
class KeyIds(db.Table):
    geospatial_key_id: db.TableId
    temporal_key_id: db.TableId


@dataclass(frozen=True, order=True)
class Key(KeyIds, GeoSpatialKey, TemporalKey):
    pass


@dataclass(frozen=True)
class CropType(db.TypeTable, db.Detailed):
    species: str
    cultivar: Optional[str]
    parent_id_1: Optional[db.TableId] = None
    parent_id_2: Optional[db.TableId] = None


@dataclass(frozen=True)
class CropStage(db.TypeTable):
    crop_stage: str


@dataclass(frozen=True)
class PlantingKey(db.TableKey):
    crop_type_id: db.TableId
    field_id: db.TableId
    planted: datetime


@dataclass(frozen=True)
class Planting(PlantingKey, db.Detailed):
    local_type_id: Optional[db.TableId]


@dataclass(frozen=True)
class Harvest(PlantingKey, db.Detailed):
    local_type_id: Optional[db.TableId]


@dataclass(frozen=True)
class CropProgressKey(PlantingKey):
    crop_stage_id: db.TableId


@dataclass(frozen=True)
class CropProgress(CropProgressKey, db.Detailed):
    local_type_id: Optional[db.TableId]


@dataclass(frozen=True)
class ReportType(db.TypeTable):
    report: str
