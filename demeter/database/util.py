from typing import Protocol, KeysView, ItemsView, ValuesView, TypeVar, Dict
from typing import cast

KT_co = TypeVar("KT_co", covariant=True)
VT_co = TypeVar("VT_co", covariant=True)

# TODO: Simplify
class CovariantMapping(Protocol[KT_co, VT_co]):  # type: ignore
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def keys(self) -> KeysView[KT_co]: ...
    def items(self) -> ItemsView[KT_co, VT_co]: ...
    def values(self) -> ValuesView[VT_co]: ...
    def __getitem__(self, k : KT_co) -> VT_co: ...  # type: ignore


def sumCovariantMappings(*ms : CovariantMapping[KT_co, VT_co]) -> CovariantMapping[KT_co, VT_co]:
  out : Dict[KT_co, VT_co] = {}
  for m in ms:
    out.update(m)
  return cast(CovariantMapping[KT_co, VT_co], out)
