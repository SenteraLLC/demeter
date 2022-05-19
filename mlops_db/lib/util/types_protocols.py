from typing import Optional, Dict, Any, Union, Set, Iterator, Tuple

from datetime import datetime

from dataclasses import dataclass

class Table():
  def keys(self) -> Set[str]:
    return set(self.__annotations__.keys())

  def items(self) -> Iterator[Tuple[str, Any]]:
    keys = self.keys()
    return ((k, getattr(self, k)) for k in keys)

  def args(self) -> Dict[str, Any]:
    return dict(self.items())

  # TODO: Allows bad attributes to get None back
  def __getitem__(self, k : str) -> Any:
    return getattr(self, k)

  def __iter__(self):
    raise TypeError


@dataclass
class Updateable(Table):
  last_updated : Optional[datetime]

Details = Optional[Dict[str, Any]]

@dataclass
class Detailed(Updateable):
  details : Details

@dataclass
class TypeTable(Table):
  pass

@dataclass
class TableKey(Table):
  pass

# TODO: Consider using typing.NewType for serial keys
AnyKey = Union[TableKey, int]

