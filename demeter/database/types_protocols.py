from typing import Optional, Mapping, Any, Union, Set, Iterator, Tuple, TypeVar, Type

from datetime import datetime
from dataclasses import dataclass

import json

from .details import Details

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


T = TypeVar('T', bound=Table, covariant=True)

class TableEncoder(json.JSONEncoder):

  def default(self, obj : Any):
    if isinstance(obj, Table):
      t = obj
      return t.args()

    elif isinstance(obj, Details):
      d = obj
      return d.details

    return super().default(obj)


@dataclass(frozen=True)
class Updateable(Table):
  last_updated : Optional[datetime]


@dataclass(frozen=True)
class Detailed(Updateable):
  details : Optional[Details]


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

