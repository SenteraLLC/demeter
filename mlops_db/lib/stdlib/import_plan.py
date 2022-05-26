from typing import Callable, Union, Sequence, Mapping, Any, Tuple, Type, Generic

from dataclasses import dataclass

from .next_fn import NextFn
from .find_fn import FindFn, GetTypeIterator
from .write_fn import WriteFn
from .get_fn import GetFn
from .imports import I

# TODO: Allow (almost) any combination
ThreeArgs = Callable[[NextFn[I], FindFn, WriteFn], None]
FourArgs = Callable[[NextFn[I], FindFn, WriteFn, GetFn], None]
FiveArgs = Callable[[NextFn[I], FindFn, WriteFn, GetFn, Any], None]

ImportTransformation = Union[ThreeArgs[I], FourArgs[I], FiveArgs[I]]

@dataclass
class ImportPlan(Generic[I]):
  get_type_iterator : GetTypeIterator
  steps             : Sequence[Tuple[Type[I] ,ImportTransformation[I]]]
  args              : Mapping

