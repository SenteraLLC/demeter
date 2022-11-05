from typing import Optional

from dataclasses import dataclass
from datetime import date

from ... import db


@dataclass(frozen=True)
class ObservationValue(db.Detailed):
    geom_id: Optional[db.TableId]
    parcel_id: db.TableId
    unit_type_id: db.TableId
    quantity: float
    acquired: date


@dataclass(frozen=True)
class ObservationType(db.TypeTable):
    type_name: str
    type_category: Optional[str]


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    unit: str
    observation_type_id: db.TableId


@dataclass(frozen=True)
class Operation(db.Detailed):
    parcel_id: db.TableId
    observation_type_id: db.TableId
    name: str
    performed: date
