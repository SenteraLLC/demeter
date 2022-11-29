from dataclasses import InitVar, dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from shapely.geometry import Polygon as Poly  # type: ignore

from demeter.data import Geom, Polygon
from demeter.db import Detailed, SelfKey, Table, TableId


@dataclass(frozen=True)
class Node(Table):
    polygon: InitVar[Poly]
    value: float
    geom: str = field(init=False)

    def __post_init__(self, polygon: Poly) -> None:
        coords: Polygon = tuple(polygon.exterior.coords)
        # TODO: Too hacky? Maybe composition is better
        g = Geom(
            crs_name="urn:ogc:def:crs:EPSG::4326",
            type="Polygon",
            coordinates=(coords,),
        )
        object.__setattr__(self, "geom", g.geom)


@dataclass(frozen=True)
class NodeRaster(Table):
    node_id: TableId
    # TODO: What Python type for raster?
    raster: str


@dataclass(frozen=True)
class NodeRasterCell(Table):
    # TODO: I think that we can calculate this key instead of using NodeRaster
    node_id: TableId
    value: float


@dataclass(frozen=True)
class Root(Detailed):
    local_type_id: TableId
    root_node_id: TableId
    time: datetime


@dataclass(frozen=True)
class Ancestry(SelfKey):
    parent_node_id: TableId
    node_id: TableId


from demeter.db._lookup_types import TableLookup

id_table_lookup: TableLookup = {
    Root: "root",
    Node: "node",
}

key_table_lookup: TableLookup = {
    Ancestry: "node_ancestry",
}
