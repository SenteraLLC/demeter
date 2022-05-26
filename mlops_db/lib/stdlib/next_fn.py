from typing import Iterator, Generic
from typing import cast

from .imports import I

class NextFn(Generic[I]):
  def __init__(self, it : Iterator[I]):
    self.it = it

  # TODO: Memo messes with dataclass generics
  #       It might work in Python v3.10
  #@memo(maxsize=1)
  def __call__(self) -> I:
    return cast(I, next(self.it))


