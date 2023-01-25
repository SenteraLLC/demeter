from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..data import _union_types as data

    # from ..grid import _union_types as grid
    # from ..task import _union_types as task
    # from ..work import _union_types as work
else:

    class DummyModule:
        def __getattribute__(self, k: str) -> None:
            return None

    # grid = data = task = work = DummyModule()
    data = DummyModule()


from typing import Union

AnyTypeTable = data.AnyTypeTable
# Union[data.AnyTypeTable, task.AnyTypeTable, work.AnyTypeTable]

AnyDataTable = data.AnyDataTable
# Union[data.AnyDataTable, task.AnyDataTable, work.AnyDataTable]

AnyIdTable = data.AnyIdTable
# Union[data.AnyIdTable, task.AnyIdTable, work.AnyIdTable, grid.AnyIdTable]

AnyKeyTable = None
# Union[
#     # data.AnyKeyTable,
#     task.AnyKeyTable,
#     work.AnyKeyTable,
#     grid.AnyKeyTable,
# ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable]
# , AnyKeyTable]
