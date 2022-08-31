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
getNode  : GetTable[Node] = g.getTableFunction(Node)
getAncestry = g.getTableByKeyFunction(Ancestry, Ancestry)

insertRoot : ReturnId[Root] = g.getInsertReturnIdFunction(Root)
insertNode : ReturnId[Node] = g.getInsertReturnIdFunction(Node)
insertAncestry : ReturnSameKey[Ancestry] = g.getInsertReturnSameKeyFunction(Ancestry)

insertOrGetRoot = g.partialInsertOrGetId(getMaybeRootId, insertRoot)
insertOrGetNode = g.partialInsertOrGetId(getMaybeNodeId, insertNode)
insertOrGetAncestry = g.partialInsertOrGetKey(Ancestry, getAncestry, insertAncestry)

