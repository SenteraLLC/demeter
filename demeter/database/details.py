from typing import Union, Mapping, Optional, TypeVar, NewType, Generic, Literal, Dict, Any
from typing import cast

import json
from collections import OrderedDict

from psycopg2.extras import Json

V = TypeVar('V')
_JsonPrimitive = Union[int, float, str, bool, None]
_JsonValue = Optional[Union[_JsonPrimitive, V]]
_JsonObject = Dict[str, _JsonValue[V]]
_JsonObjectOrdered = OrderedDict[str, _JsonValue[V]]

# Mypy doesn't have recursive types yet

JSON_MAX_DEPTH_T = NewType('JSON_MAX_DEPTH_T', bool)

JsonDepth4 = _JsonObject[JSON_MAX_DEPTH_T]
JsonDepth3 = _JsonObject[JsonDepth4]
JsonDepth2 = _JsonObject[JsonDepth3]
JsonRoot  = _JsonObject[JsonDepth2]

_JsonDepth4 = _JsonObjectOrdered[JSON_MAX_DEPTH_T]
_JsonDepth3 = _JsonObjectOrdered[_JsonDepth4]
_JsonDepth2 = _JsonObjectOrdered[_JsonDepth3]
_JsonRoot  = _JsonObjectOrdered[_JsonDepth2]

def _orderDict(root : JsonRoot) -> _JsonRoot:
  return OrderedDict(
           (k, _orderDict(v)) if isinstance(v, dict) # type: ignore
           else
             (k, v)
           for k, v in root.items()
         )

class HashableJSON(Json):
  def __init__(self, root : 'JSON') -> None:
    self._old : JsonRoot
    self._root : _JsonRoot
    if isinstance(root, HashableJSON):
      self._old = root._old
      self._root = root._root
    else:
      self._old = root
      self._root = _orderDict(root)

  def dumps(self, obj : Any) -> Any:
    return json.dumps(self._root)

  def __call__(self) -> JsonRoot:
    return self._old

  def __hash__(self) -> int:
    return hash(frozenset(self._root))

JSON = Union[JsonRoot, HashableJSON]

