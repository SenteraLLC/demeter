from typing import Callable, Union, Sequence, Mapping, Any

from dataclasses import dataclass

from .next_fn import NextFn
from .find_fn import FindFn, GetTypeIterator
from .write_fn import WriteFn


FourArgs = Callable[[NextFn, FindFn, WriteFn, Any], None]

ThreeArgs = Callable[[NextFn, FindFn, WriteFn], None]

ImportTransformation = Union[ThreeArgs, FourArgs]


@dataclass
class ImportPlan():
  get_type_iterator : GetTypeIterator
  steps             : Sequence[ImportTransformation]
  args              : Mapping

