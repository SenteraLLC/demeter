from typing import Generic, Optional, TypeVar
from typing import cast

from abc import ABC, abstractmethod

V = TypeVar('V')
E = TypeVar('E')

class ExpectedBase(Generic[V, E], ABC):
  @abstractmethod
  def __init__(self):
    self.maybe_value : Optional[V] = None
    self.maybe_error : Optional[E] = None
    self.has_error = False

  def __bool__(self) -> bool:
    return not self.has_error

  def error(self) -> E:
    raise Exception("No error exists.")


class UnexpectedTemplate(ExpectedBase[V, E]):
  def __init__(self, error : E):
    super().__init__()
    self.maybe_error = error
    self.has_error = True

  def error(self) -> E:
    return cast(E, self.maybe_error)

Unexpected = UnexpectedTemplate[V, str]


class ExpectedTemplate(ExpectedBase[V, E]):
  def __init__(self, value : V):
    super().__init__()
    self.maybe_value = value

  def __call__(self) -> V:
    return cast(V, self.maybe_value)

Expected = ExpectedTemplate[V, str]

