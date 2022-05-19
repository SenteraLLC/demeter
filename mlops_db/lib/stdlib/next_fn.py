from typing import Dict, Type, Iterator
from typing import cast

#from functools import lru_cache as memo

# TODO: Remove
#TypeToIterator = Dict[Type[Import], Iterator[Import]]
from .imports import I

TypeToIterator = Dict[Type[I], Iterator[I]]

class NextFn(object):
  def __init__(self, type_to_iterator : TypeToIterator):
    self.type_to_iterator = type_to_iterator

  # TODO: Memo messes with dataclass generics
  #       It might work in Python v3.10
  #@memo(maxsize=1)
  def __call__(self, typ : Type[I]) -> I:
    return cast(I, next(self.type_to_iterator[typ]))


