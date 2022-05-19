from typing import Protocol, TypeVar, Any
from typing import cast

# TODO: Is this too sneaky?
#       Allows protocols to be used for dicts
class Import(Protocol):
  def __getattr__(self, attr : str): ...

I = TypeVar('I', bound=Import)

from typing import Optional
class ImportWrapper(dict):
  def __getattr__(self, attr : str):
    return self.get(attr)

def WrapImport(i : I) -> I:
  return cast(I, ImportWrapper(cast(Any, i)))

