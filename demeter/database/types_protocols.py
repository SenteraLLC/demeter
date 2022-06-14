from typing import Optional, Any, TypeVar, Sequence, Set, Tuple, Union, NewType
from typing import cast

from abc import ABC
from datetime import datetime
from dataclasses import dataclass
from dataclasses import field, fields
import json
from collections import OrderedDict

from .details import JSON

D = TypeVar('D')

@dataclass(frozen=True)
class Table(ABC):
  def names(self) -> Sequence[str]:
    return [f.name for f in fields(self)]

  def __call__(self) -> OrderedDict[str, Any]:
    out = [(f.name, getattr(self, f.name)) for f in fields(self)]
    return OrderedDict(out)

  def items(self) -> Sequence[Tuple[str, Any]]:
    return list(self().items())

  def get(self, k : str) -> D:
    return cast(D, self.__getattribute__(k))

  class Encoder(json.JSONEncoder):
    def default(self, obj : Any) -> Any:
      return obj() if isinstance(obj, Table) \
                   else json.JSONEncoder.default(self, obj)


# TODO: Make an alias for the partially applied dataclass
#       Waiting on Python 3.11 feature: dataclass transforms
#       For now, we have to copy-paste these decorators

@dataclass(frozen=True)
class Updateable(Table):
  last_updated : Optional[datetime]

@dataclass(frozen=True)
class Detailed(Updateable):
  details : Optional[JSON] = field(hash=False)

@dataclass(frozen=True)
class TypeTable(Table):
  pass

@dataclass(frozen=True)
class TableKey(Table):
  @classmethod
  def names(cls) -> Sequence[str]:
    return [f.name for f in fields(cls)]

TableId = NewType('TableId', int)

SomeKey = Union[TableKey, TableId]

@dataclass(frozen=True)
class SelfKey(TableKey):
  pass

