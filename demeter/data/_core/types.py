from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ... import db


@dataclass(frozen=True)
class Field(db.Detailed):
    """Arbitrary spatiotemporal unit representing an agronomically-relevant
    area that is generally, but not always, managed as a single unit within
    the defined spatial and temporal constraints."""

    geom_id: db.TableId
    name: str
    field_group_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class CropType(db.TypeTable, db.Detailed):
    """Information related to the plant cultivated through a Planting activity."""

    crop: str
    product_name: Optional[str] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class PlantingKey(db.TableKey):
    """Unique key to group any planting and/or harvest activity for a given field's growing season."""

    crop_type_id: db.TableId
    field_id: db.TableId


@dataclass(frozen=True)
class Planting(PlantingKey, db.Detailed):
    date_performed: datetime
    geom_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class Harvest(PlantingKey, db.Detailed):
    date_performed: datetime
    geom_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


# @dataclass(frozen=True)
# class ReportType(db.TypeTable):
#     report: str


@dataclass(frozen=True)
class Act(db.Detailed):
    field_id: db.TableId
    date_performed: datetime
    geom_id: Optional[db.TableId] = None
    act_type: Optional[str] = None
    created: Optional[datetime] = None
