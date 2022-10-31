from dataclasses import dataclass
from datetime import date
from typing import Dict, Generator, Literal, Optional, Tuple, TypedDict, Union

from ... import db


@dataclass(frozen=True)
class LocalValue(db.Detailed):
    geom_id: Optional[db.TableId]
    field_id: db.TableId
    unit_type_id: db.TableId
    quantity: float
    local_group_id: Optional[db.TableId]
    acquired: date


@dataclass(frozen=True)
class LocalType(db.TypeTable):
    type_name: str
    type_category: Optional[str]


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    unit: str
    local_type_id: db.TableId


@dataclass(frozen=True)
class LocalGroup(db.TypeTable):
    group_name: str
    group_category: Optional[str]
