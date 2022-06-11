from typing import Union, Mapping, Optional, NewType, TypeVar

V = TypeVar('V')
_JsonPrimitive = Union[int, float, str, bool, None]
_JsonValue = Optional[Union[_JsonPrimitive, V]]
_JsonObject = Mapping[str, _JsonValue[V]]

# Mypy doesn't have recursive types yet

JSON_MAX_DEPTH_T = NewType('JSON_MAX_DEPTH_T', bool)

JsonDepth4 = _JsonObject[JSON_MAX_DEPTH_T]
JsonDepth3 = _JsonObject[JsonDepth4]
JsonDepth2 = _JsonObject[JsonDepth3]
JSON = _JsonObject[JsonDepth2]

