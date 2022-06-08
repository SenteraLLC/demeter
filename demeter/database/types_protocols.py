from typing import Optional, Mapping, Any, Union, TypeVar, Sequence, Iterator

from datetime import datetime
from dataclasses import dataclass
from dataclasses import fields

import json

from .details import IncompleteHashableJSON, HashableJSON

from collections import OrderedDict

@dataclass(frozen=True)
class Table():
  @classmethod
  def names(cls) -> Sequence[str]:
    return [f.name for f in fields(cls)]

  def __call__(self) -> OrderedDict[str, Any]:
    out = [(f.name, getattr(self, f.name)) for f in fields(self)]
    return OrderedDict(out)

  class Encoder(json.JSONEncoder):
    def default(self, obj : Any):
      return obj() if isinstance(obj, Table) \
                   else json.JSONEncoder.default(self, obj)

T = TypeVar('T', bound=Table)


# TODO: Make an alias for the partially applied dataclass
#       Waiting on Python 3.11 feature: dataclass transforms
#       For now, we have to copy-paste these decorators

@dataclass(frozen=True)
class Updateable(Table):
  last_updated : Optional[datetime]


@dataclass(frozen=True)
class Detailed(Updateable):
  details : Optional[IncompleteHashableJSON]

  def __post_init__(self):
    details = HashableJSON(self.details)


@dataclass(frozen=True)
class TypeTable(Table):
  pass


@dataclass(frozen=True)
class TableKey(Table):
  pass

@dataclass(frozen=True)
class SelfKey(TableKey):
  pass

# TODO: Consider using typing.NewType for serial keys
AnyKey = Union[TableKey, int]

