from functools import partial, wraps
from typing import (
    Any,
    Callable,
    Optional,
    Type,
    TypeVar,
    cast,
)

from .. import TableId
from .._generic_types import (
    SK,
    GetId,
    GetTableByKey,
    I,
    ReturnId,
    ReturnKey,
    ReturnSameKey,
    S,
    T,
)
from .._lookup_types import TableLookup
from .._union_types import AnyIdTable
from .get import getMaybeId, getMaybeTable, getMaybeTableByKey
from .insert import insertAndReturnId, insertAndReturnKey, insertOrGetId, insertOrGetKey

C = TypeVar("C")


class SQLGenerator:
    def __init__(
        self,
        module_name: str = "demeter",
        type_table_lookup: TableLookup = {},
        data_table_lookup: TableLookup = {},
        id_table_lookup: TableLookup = {},
        key_table_lookup: TableLookup = {},
    ) -> None:
        self.module_name = module_name
        self.type_table_lookup = type_table_lookup
        self.data_table_lookup = data_table_lookup
        self.id_table_lookup = id_table_lookup
        self.key_table_lookup = key_table_lookup

    def _fix_annotations(
        self,
        c: C,
        maybe_table_name: Optional[str] = None,
        maybe_return_name: Optional[str] = None,
    ) -> C:
        try:
            c.__annotations__
        except AttributeError:
            c.__annotations__ = {}
        if t := maybe_table_name:
            c.__annotations__["table"] = self.module_name + "." + t
        if r := maybe_return_name:
            c.__annotations__["return"] = self.module_name + "." + r
        return c

    def getInsertReturnIdFunction(self, table: Type[I]) -> ReturnId[I]:
        table_name = self.id_table_lookup[table]
        return self._fix_annotations(
            partial(insertAndReturnId, table_name), table.__name__
        )

    def getInsertReturnSameKeyFunction(self, table: Type[SK]) -> ReturnSameKey[SK]:
        table_name = self.key_table_lookup[table]
        fn = cast(ReturnSameKey[SK], insertAndReturnKey(table_name, table))
        n = table.__name__
        return self._fix_annotations(fn, n, n)

    def getInsertReturnKeyFunction(
        self, table: Type[S], key: Type[SK]
    ) -> ReturnKey[S, SK]:
        table_name = self.key_table_lookup[table]
        fn = cast(ReturnKey[S, SK], insertAndReturnKey(table_name, key))
        return self._fix_annotations(fn, table.__name__, key.__name__)

    def getMaybeIdFunction(
        self, table: Type[T]
    ) -> Callable[[Any, AnyIdTable], Optional[TableId]]:
        table_name = self.id_table_lookup[table]
        return self._fix_annotations(partial(getMaybeId, table_name), table.__name__)

    def getMaybeTableById(
        self,
        table_type: Type[I],
        table_id_name: str,
        cursor: Any,
        table_id: TableId,
    ) -> Optional[I]:
        table_name = self.id_table_lookup[table_type]
        table = getMaybeTable(table_name, table_id_name, table_id, cursor)
        if table is None:
            return None
        table_args = {k: v for k, v in table._asdict().items() if k != table_id_name}
        return cast(I, table_type(**table_args))

    def getTableById(
        self,
        table_type: Type[I],
        table_id_name: str,
        cursor: Any,
        table_id: TableId,
    ) -> I:
        maybe_table = self.getMaybeTableById(
            table_type, table_id_name, cursor, table_id
        )
        if maybe_table is None:
            table_name = self.id_table_lookup[table_type]
            raise Exception(
                f"No entry found for {table_id_name} = {table_id} in {table_name}"
            )
        table = maybe_table
        return table

    def getTableFunction(
        self, table: Type[I], table_id_name: Optional[str] = None
    ) -> Callable[[Any, TableId], I]:
        table_name = self.id_table_lookup[table]
        if table_id_name is None:
            table_id_name = "_".join([table_name, "id"])

        # This is a mess because mypy doesn't support partially-applied methods
        __impl_table_id_name = table_id_name

        @wraps(self.getTableById)
        def _impl(cursor: Any, table_id: TableId) -> I:
            return self.getTableById(table, __impl_table_id_name, cursor, table_id)

        return self._fix_annotations(_impl, None, table.__name__)

    def getTableByKeyFunction(
        self,
        table: Type[S],
        key_table: Type[SK],
    ) -> GetTableByKey[SK, S]:
        table_name = self.key_table_lookup[table]
        gtbk = cast(GetTableByKey[SK, S], getMaybeTableByKey)
        return self._fix_annotations(
            partial(gtbk, table_name), key_table.__name__, table.__name__
        )

    def partialInsertOrGetId(
        self,
        get_id: GetId[I],
        return_id: ReturnId[I],
    ) -> ReturnId[I]:
        return partial(insertOrGetId, get_id, return_id)

    def partialInsertOrGetKey(
        self,
        key_type: Type[SK],
        get_key: GetTableByKey[SK, S],
        return_key: ReturnKey[S, SK],
    ) -> ReturnKey[S, SK]:
        rk = cast(ReturnKey[S, SK], insertOrGetKey)
        return partial(rk, get_key, return_key, key_type)
