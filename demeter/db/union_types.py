from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from ..data import union_types as data
  from ..task import union_types as task
  from ..work import union_types as work

else:
  class DummyModule:
    def __getattribute__(self, k : str) -> None:
      return None
  data = task = work = DummyModule()


from typing import Union

AnyTypeTable = Union[data.AnyTypeTable, task.AnyTypeTable, work.AnyTypeTable]

AnyDataTable = Union[data.AnyDataTable, task.AnyDataTable, work.AnyDataTable]

AnyIdTable = Union[data.AnyIdTable, task.AnyIdTable, work.AnyIdTable]

AnyKeyTable = Union[data.AnyKeyTable, task.AnyKeyTable, work.AnyKeyTable]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

