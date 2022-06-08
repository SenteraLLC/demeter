from typing import Union, Mapping, Optional, TypeVar, NewType, Generic, Literal
from collections import OrderedDict

import json

from dataclasses import dataclass, field


J = TypeVar('J')
_JsonPrimitive = Union[int, float, str, bool, None]
_JsonValue = Optional[Union[_JsonPrimitive, J]]
_JsonObject = OrderedDict[str, _JsonValue[J]]

# Mypy doesn't have recursive types yet

JSON_MAX_DEPTH_T = NewType('JSON_MAX_DEPTH_T', int)
JsonDepth4     = _JsonObject[JSON_MAX_DEPTH_T]
JsonDepth3     = _JsonObject[JsonDepth4]
JsonDepth2     = _JsonObject[JsonDepth3]
JsonRootObject = _JsonObject[JsonDepth2]

class HashableJSON:
  def __init__(self, root : JsonRootObject) -> None:
    self._root = root

  def __call__(self) -> JsonRootObject:
    return self._root

  def __hash__(self) -> int:
    return hash(frozenset(self._root))

IncompleteHashableJSON = Union[JsonRootObject, HashableJSON]

class HashablePair(HashableJSON):
  def __init__(self,
               k : str,
               v : _JsonPrimitive
              ):
    self._root = OrderedDict({k : v})

