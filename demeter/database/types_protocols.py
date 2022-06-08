from typing import Optional, Mapping, Any, Union, Set, Iterator, Tuple, TypeVar, Type

from datetime import datetime
from dataclasses import InitVar
from dataclasses import dataclass, field
from dataclasses import asdict

import json

from .details import IncompleteHashableJSON, HashableJSON

from collections import OrderedDict

@dataclass(frozen=True)
class Table():
  def keys(self) -> Set[str]:
    return set(self.__dataclass_fields__.keys())

  def items(self) -> Iterator[Tuple[str, Any]]:
    keys = self.keys()
    return ((k, self[k]) for k in keys)

  def args(self) -> Mapping[str, Any]:
    return dict(self.items())

  # TODO: This should be handled by dataclasses.fields(...)
  def __getitem__(self, k : str) -> Any:
    # TODO: Allows bad attributes to get None back
    #       Also prevents infinite recursion on __getitem__
    return getattr(self, k)

  def __iter__(self):
    raise TypeError

  class Encoder(json.JSONEncoder):
    DEFAULT = json.JSONEncoder.default

    def default(self, obj):
      if isinstance(obj, HashableJSON):
        _impl = obj()
        return _impl
      elif isinstance(obj, Table):
        items = asdict(obj)
        return items

      return self.DEFAULT(obj)


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

