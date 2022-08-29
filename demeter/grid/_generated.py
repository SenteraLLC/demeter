from ..db._postgres import SQLGenerator
from ._types import Root, Ancestry, Node, id_table_lookup

g = SQLGenerator("demeter.data",
                 id_table_lookup = id_table_lookup,
                )

from demeter.db._generic_types import GetId, GetTable, ReturnId

getMaybeRoot = g.getMaybeIdFunction(Root)
getMaybeNode = g.getMaybeIdFunction(Node)
getMaybeAncestry = g.getMaybeIdFunction(Ancestry)

getRoot  : GetTable[Root] = g.getTableFunction(Root)
getNode  : GetTable[Node] = g.getTableFunction(Node)
getAncestry  : GetTable[Ancestry] = g.getTableFunction(Ancestry)

insertRoot : ReturnId[Root] = g.getInsertReturnIdFunction(Root)
insertNode : ReturnId[Node] = g.getInsertReturnIdFunction(Node)
insertAncestry : ReturnId[Ancestry] = g.getInsertReturnIdFunction(Ancestry)


