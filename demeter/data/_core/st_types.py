from datetime import date
from typing import Optional, Union

from shapely.geometry import LineString, Point, Polygon
from shapely.geometry import MultiLineString, MultiPoint, MultiPolygon
from shapely.geometry import GeometryCollection

from dataclasses import dataclass
from ... import db


@dataclass(frozen=True)
class Geom(db.Table):
    """Spatial object that is relevant to other data within demeter"""

    geom: Union[
        LineString,
        Point,
        Polygon,
        MultiLineString,
        MultiPoint,
        MultiPolygon,
        GeometryCollection,
    ]


@dataclass(frozen=True)
class GeoSpatialKey(db.Table):
    geom_id: db.TableId
    field_id: Optional[db.TableId]


@dataclass(frozen=True)
class KeyIds(db.Table):
    geospatial_key_id: db.TableId
    temporal_key_id: db.TableId


@dataclass(frozen=True)
class TemporalKey(db.Table):
    start_date: date
    end_date: date


@dataclass(frozen=True, order=True)
class Key(KeyIds, GeoSpatialKey, TemporalKey):
    pass
