from typing import Optional, Mapping, Any, Union, Set, Iterator, Tuple, TypeVar, Type

from datetime import datetime
from dataclasses import InitVar
from dataclasses import dataclass, field

import json

from .details import JsonRootObject, HashableJsonContainer

class Table():
  def keys(self) -> Set[str]:
    # TODO: Need a protocol
    return set(self.__dataclass_fields__.keys()) # type: ignore

  def items(self) -> Iterator[Tuple[str, Any]]:
    keys = self.keys()
    return ((k, self[k]) for k in keys)

  def args(self) -> Mapping[str, Any]:
    return dict(self.items())

  def __getitem__(self, k : str) -> Any:
    # TODO: Allows bad attributes to get None back
    #       Also prevents infinite recursion on __getitem__
    return getattr(self, k)

  def __iter__(self):
    raise TypeError


# TODO: Make an alias for the partially applied dataclass
#       Waiting on Python 3.11 feature: dataclass transforms
#       For now, we have to copy-paste these decorators

@dataclass(frozen=True)
class Updateable(Table):
  last_updated : Optional[datetime]

Details = JsonRootObject

@dataclass(frozen=True)
class Detailed(Updateable, HashableJsonContainer):
  details  : InitVar[Optional[Details]]


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

