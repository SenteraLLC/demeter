from typing import Optional

from dataclasses import dataclass
from datetime import date

from ... import db


@dataclass(frozen=True)
class ObservationValue(db.Detailed):
    field_id: db.TableId
    unit_type_id: db.TableId
    acquired: date
    quantity: float
    geom_id: Optional[db.TableId] = None


@dataclass(frozen=True)
class ObservationType(db.TypeTable):
    type_name: str


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    unit: str
    observation_type_id: db.TableId


@dataclass(frozen=True)
class Operation(db.Detailed):
    field_id: db.TableId
    observation_type_id: db.TableId
    performed: date
