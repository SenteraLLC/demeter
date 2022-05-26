from typing import Callable, Union, Sequence, Mapping, Any, Tuple, Type, Generic, Iterator

from dataclasses import dataclass

from .find_fn import FindFn, GetTypeIterator
from .write_fn import WriteFn
from .get_fn import GetFn
from .imports import I

# TODO: Allow (almost) any combination
ThreeArgs = Callable[[Iterator[I], FindFn, WriteFn], None]
FourArgs = Callable[[Iterator[I], FindFn, WriteFn, GetFn], None]
FiveArgs = Callable[[Iterator[I], FindFn, WriteFn, GetFn, Any], None]

ImportTransformation = Union[ThreeArgs[I], FourArgs[I], FiveArgs[I]]

@dataclass
class ImportPlan(Generic[I]):
  get_type_iterator : GetTypeIterator
  steps             : Sequence[Tuple[Type[I] ,ImportTransformation]]
  args              : Mapping

