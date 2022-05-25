from typing import Generic, Optional, TypeVar, Any, Callable
from typing import cast

class UnsetFutureException(Exception):
  pass


from ..util.types_protocols import Table


from typing import Awaitable, Generator, AsyncIterator

T = TypeVar('T', bound=Table)
R = TypeVar('R')
from .exceptions import NotNullViolationException

from functools import wraps

InsertFn = Callable[[Any, T], R]

def FutureInsert(t : T) -> Callable[[Any, InsertFn[T, R]], Awaitable[R] ]:
  #@wraps(future_result)
  async def doInsert(cursor : Any, insert_fn : InsertFn[T, R]) -> R:
    return insert_fn(cursor, t)
  return doInsert

