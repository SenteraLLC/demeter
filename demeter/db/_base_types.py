from typing import Optional, Any, TypeVar, Sequence, Set, Union, NewType
from typing import cast


from abc import ABC
from datetime import datetime
from dataclasses import dataclass
from dataclasses import field, fields
import json
from collections import OrderedDict

from . import _json_type as json_type

D = TypeVar("D")


@dataclass(frozen=True)
class Table:
    def names(self) -> Sequence[str]:
        return [f.name for f in fields(self)]

    # TODO: Simplify this and the register_adapter for Table and OrderedDict
    def __call__(self) -> OrderedDict[str, Any]:
        out = [(f.name, getattr(self, f.name)) for f in fields(self)]
        return OrderedDict(out)

    def get(self, k: str) -> D:
        return cast(D, self.__getattribute__(k))


#  class Encoder(json.JSONEncoder):
#    def default(self, obj : Any) -> Any:
#      return obj() if isinstance(obj, Table) \
#                   else json.JSONEncoder.default(self, obj)


# TODO: Make an alias for the partially applied dataclass
#       Waiting on Python 3.11 feature: dataclass transforms
#       For now, we have to copy-paste these decorators


@dataclass(frozen=True)
class Detailed(Table):
    last_updated: datetime = field(
        default_factory=datetime.now, hash=False, kw_only=True
    )
    details: json_type.JSON = field(
        default_factory=lambda: json_type.EMPTY_JSON, hash=False, kw_only=True
    )


@dataclass(frozen=True)
class TypeTable(Table):
    pass


@dataclass(frozen=True)
class TableKey(Table):
    @classmethod
    def names(cls) -> Sequence[str]:
        return [f.name for f in fields(cls)]


TableId = NewType("TableId", int)


@dataclass(frozen=True)
class SelfKey(TableKey):
    pass


SomeKey = Union[SelfKey, TableKey, TableId]
