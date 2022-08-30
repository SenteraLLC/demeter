from typing import List, Tuple

from dataclasses import dataclass, field
from dataclasses import InitVar

from demeter.db import Table, SelfKey, TableId, Detailed
from demeter.data import Polygon, Geom

from shapely.geometry import Polygon as Poly # type: ignore

@dataclass(frozen=True)
class Node(Table):
  polygon : InitVar[Poly]
  value   : float
  geom    : str = field(init=False)

  def __post_init__(self, polygon : Poly) -> None:
    coords : Polygon = tuple(polygon.exterior.coords)
    # TODO: Too hacky? Maybe composition is better
    g = Geom(
      crs_name = "urn:ogc:def:crs:EPSG::4326",
      type = "Polygon",
      coordinates = (coords, ),
      container_geom_id = None,
    )
    print("G IS: ",g)
    object.__setattr__(self, 'geom', g.geom)


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


# TODO: I think ancestry should be optional, note this somewhere
@dataclass(frozen=True)
class Ancestry(SelfKey):
  root_id : TableId
  parent_node_id : TableId
  node_id : TableId


from demeter.db._lookup_types import TableLookup

id_table_lookup : TableLookup = {
  Root : 'root',
  Node : 'node',
}

key_table_lookup : TableLookup = {
  Ancestry : 'node_ancestry',
}

