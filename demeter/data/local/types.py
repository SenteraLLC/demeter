from typing import TypedDict, Optional, Union, Dict, Literal, Tuple, Generator
from datetime import date

from ... import db

from dataclasses import dataclass


@dataclass(frozen=True)
class LocalValue(db.Detailed):
  geom_id        : db.TableId
  field_id       : Optional[db.TableId]
  unit_type_id   : db.TableId
  quantity       : float
  local_group_id : Optional[db.TableId]
  acquired       : date

AnyDataTable = Union[LocalValue]


@dataclass(frozen=True)
class LocalType(db.TypeTable):
  type_name      : str
  type_category  : Optional[str]

@dataclass(frozen=True)
class UnitType(db.TypeTable):
  unit          : str
  local_type_id : db.TableId

@dataclass(frozen=True)
class LocalGroup(db.TypeTable):
  group_name     : str
  group_category : Optional[str]

AnyTypeTable = Union[LocalType, UnitType, LocalGroup]

AnyTable = Union[AnyDataTable, AnyTypeTable]

