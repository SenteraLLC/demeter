from typing import Optional, Dict, Any, Union


from datetime import datetime

from dataclasses import dataclass

from typing import Set, Iterator, Tuple


class Table():
  def keys(self) -> Set[str]:
    return set(self.__annotations__.keys())

  def items(self) -> Iterator[Tuple[str, Any]]:
    keys = self.keys()
    return ((k, getattr(self, k)) for k in keys)

  def args(self) -> Dict[str, Any]:
    return dict(self.items())

  #def __getitem__(self, k : str) -> Any:
  #  return getattr(self, k)


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

