from typing import Callable, Union, Sequence, Mapping, Any

from dataclasses import dataclass

from .next_fn import NextFn
from .find_fn import FindFn, GetTypeIterator
from .write_fn import WriteFn
from .get_fn import GetFn

# TODO: Allow (almost) any combination
ThreeArgs = Callable[[NextFn, FindFn, WriteFn], None]
FourArgs = Callable[[NextFn, FindFn, WriteFn, GetFn], None]
FiveArgs = Callable[[NextFn, FindFn, WriteFn, GetFn, Any], None]

ImportTransformation = Union[ThreeArgs, FourArgs, FiveArgs]

@dataclass
class ImportPlan():
  get_type_iterator : GetTypeIterator
  steps             : Sequence[ImportTransformation]
  args              : Mapping

