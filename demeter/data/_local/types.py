from typing import Optional

from dataclasses import dataclass
from datetime import date

from ... import db


@dataclass(frozen=True)
class LocalValue(db.Detailed):
    field_id: db.TableId
    unit_type_id: db.TableId
    acquired: date
    quantity: float
    geom_id: Optional[db.TableId] = None


@dataclass(frozen=True)
class LocalType(db.TypeTable):
    type_name: str


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    unit: str
    local_type_id: db.TableId


@dataclass(frozen=True)
class Act(db.Detailed):
    field_id: db.TableId
    local_type_id: db.TableId
    performed: date
