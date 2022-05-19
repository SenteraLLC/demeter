from typing import Generic, Optional, TypeVar, Any, Callable
from typing import cast

class UnsetFutureException(Exception):
  pass


# TODO: Should be a generic container
#class Futureable():
#  def __init__(self):
#    self.maybe_future : Optional['Future'] = None
#    self.is_active = False
#
#  def hasFuture(self) -> bool:
#    return self.is_active
#
#  def getFuture(self) -> 'Future':
#    if not self.is_active:
#      raise Exception
#    return cast('Future', self.maybe_future)
#
#  def getMaybeFuture(self) -> Optional['Future']:
#    return self.maybe_future
#
#  def setFuture(self, future : 'Future'):
#    self.maybe_future = future
#    self.is_active = True


#T = TypeVar('T', bound=Futureable)
T = TypeVar('T')
K = TypeVar('K')  # TODO: Union[TableKey, int]


# TODO: Can probably be turned into Awaitable type
class Deferred(Generic[T]):
  def __init__(self, fn : Callable[[], T]):
    self.fn = fn
    self.maybe_result : Optional[T] = None

  def __call__(self) -> T:
    r = self.maybe_result
    if r is not None:
      return r
    r = self.fn()
    self.maybe_result = r
    return r

  def toFuture(self):
    pass


class Future(Generic[K, T], Deferred[K]):
  def __init__(self, v : k):
    # TODO: Hack
    super().__init__(lambda : None) # type: ignore
    self.key = k
    #self.value.setFuture(self)
    self.was_set = False

  def set(self, result : T) -> None:
    self.maybe_result = result
    self.was_set = True

  def get(self) -> Optional[T]:
    if not self.was_set:
      raise UnsetFutureException()
    result = self.maybe_result
    if isinstance(result, Deferred):
      return result()
    return result

  def __call__(self) -> T:
    if self.maybe_result is None:
      raise UnsetFutureException()
    return cast(K, self.get())


class FutureRequired(Future[T, K]):
  def get(self) -> K:
    if self.maybe_result is None:
      raise UnsetFutureException
    return cast(K, super().get())



