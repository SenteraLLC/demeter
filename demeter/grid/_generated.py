from typing import Type
from typing import cast

from ..db._postgres import SQLGenerator
from ._types import Root, Ancestry, Node
from ._types import key_table_lookup, id_table_lookup

g = SQLGenerator("demeter.data",
                 id_table_lookup = id_table_lookup,
                 key_table_lookup = key_table_lookup,
                )

from demeter.db._generic_types import GetId, GetTable, ReturnId, ReturnSameKey

getMaybeRootId = g.getMaybeIdFunction(Root)
getMaybeNodeId = g.getMaybeIdFunction(Node)

getMaybeAncestry = g.getTableByKeyFunction(Ancestry, Ancestry)

getRoot  : GetTable[Root] = g.getTableFunction(Root)
# TODO: getGeom needs to be fixed similarly
getAncestry = g.getTableByKeyFunction(Ancestry, Ancestry)

from typing import Any
from shapely import wkb
from shapely.geometry import Polygon as Poly # type: ignore
from demeter.db import TableId

def getNodePolygon(cursor : Any, node_id : TableId) -> Poly:
  stmt = "select geom from node where node_id = %(node_id)s"
  args = {"node_id": node_id}
  cursor.execute(stmt, args)
  result = cursor.fetchone()
  return wkb.loads(result.geom, hex=True)


insertRoot : ReturnId[Root] = g.getInsertReturnIdFunction(Root)
#insertNode : ReturnId[Node] = g.getInsertReturnIdFunction(Node)
insertAncestry : ReturnSameKey[Ancestry] = g.getInsertReturnSameKeyFunction(Ancestry)

from typing import Any

# TODO: Warn the user when the geometry is modified by ST_MakeValid
def insertNode(cursor   : Any,
               node     : Node,
              ) -> TableId:
  stmt = """insert into node(geom, value)
            values(ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)), %(value)s)
            returning node_id"""
  cursor.execute(stmt, node())
  result = cursor.fetchone()
  return TableId(result.node_id)


insertOrGetRoot = g.partialInsertOrGetId(getMaybeRootId, insertRoot)
# TODO: I don't believe that there is a need for these two
#insertOrGetNode = g.partialInsertOrGetId(getMaybeNodeId, insertNode)
#insertOrGetAncestry = g.partialInsertOrGetKey(Ancestry, getAncestry, insertAncestry)



