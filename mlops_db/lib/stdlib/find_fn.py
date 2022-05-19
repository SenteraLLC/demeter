from typing import Type, Callable, Iterator, Optional, Iterator
from typing import cast

# from functools import lru_cache as memo

from .imports import I


# TODO: Remove when we can show this works
#GetTypeIterator = Callable[[Type[Import]], Iterator[Import] ]
GetTypeIterator = Callable[[Type[I]], Iterator[I] ]

MatchFn = Callable[[I], bool]


class FindFn(object):
  def __init__(self, get_type_iterator : GetTypeIterator):
    self.get_type_iterator = get_type_iterator

  #@memo(maxsize=2048)
  def __call__(self,
               other_type : Type[I],
               match_fn : MatchFn[I]
              ) -> Optional[I]:
    it = self.get_type_iterator(other_type)
    other_it : Iterator[I] = cast(Iterator[I], it)
    for row in other_it:
      if match_fn(row):
        return cast(Optional[I], row)
    return cast(Optional[I], None)


