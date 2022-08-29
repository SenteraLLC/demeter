from dataclasses import dataclass, field
from dataclasses import InitVar

from demeter.db import Table, TableId, Detailed
from demeter.data import Polygon, Geom

@dataclass(frozen=True)
class Node(Table):
  polygon : InitVar[Polygon]
  value   : float
  geom    : str = field(init=False)

  def __post_init__(self, polygon : Polygon) -> None:
    g = Geom(
      crs_name = "urn:ogc:def:crs:EPSG::4326",
      type = "MultiPolygon",
      coordinates = (polygon, ),
      container_geom_id = None,
    )
    geom = g.geom


@dataclass(frozen=True)
class NodeRaster(Table):
  node_id : TableId
  # TODO: What Python type for raster?
  raster : str


@dataclass(frozen=True)
class NodeRasterCell(Table):
  # TODO: I think that we can calculate this key instead of using NodeRaster
  node_id : TableId
  value : float


@dataclass(frozen=True)
class Root(Detailed):
  geom_id : TableId
  local_type_id : TableId


@dataclass(frozen=True)
class Ancestry(Table):
  root_id : TableId
  parent_node_id : TableId
  node_id : TableId


from demeter.db._lookup_types import TableLookup

id_table_lookup : TableLookup = {
  Root : 'root',
  Node : 'node',
  Ancestry : 'node_ancestry',
}


