from typing import Union, Mapping, Optional, TypeVar, NewType, Protocol
from collections import OrderedDict

import json

from dataclasses import dataclass, field


J = TypeVar('J')
_JSON_VALUE = Optional[Union[int, float, str, None, J]]
#_JsonObject = Mapping[str, _JSON_VALUE[J]]
_JsonObject = OrderedDict[str, _JSON_VALUE[J]]

# Mypy doesn't have recursive types yet

JSON_MAX_DEPTH_T = NewType('JSON_MAX_DEPTH_T', int)
JsonDepth4     = _JsonObject[JSON_MAX_DEPTH_T]
JsonDepth3     = _JsonObject[JsonDepth4]
JsonDepth2     = _JsonObject[JsonDepth3]
JsonRootObject = _JsonObject[JsonDepth2]


class HashableJsonObject:
  def __init__(self, root : JsonRootObject):
    self.root = root

  def __hash__(self) -> int:
    return hash(frozenset(self.root))

  def __call__(self) -> JsonRootObject:
    return self.root

@dataclass(frozen=True)
class HashableJsonContainer:
  _impl : Optional[HashableJsonObject] = field(init=False)

  def __post_init__(self, *args : JsonRootObject):
    l = len(args)
    if l <= 0:
      raise Exception("Container has no InitVar")
    elif l > 1:
      raise Exception("Only one JSON field supported.")
    first = args[0]
    object.__setattr__(self, '_impl', HashableJsonObject(first))

  def __call__(self) -> HashableJsonObject:
    if self._impl is None:
      raise Exception("Null Json Container")
    return self._impl


