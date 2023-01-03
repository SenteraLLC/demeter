from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ... import db

from enum import Enum


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
class ActType(Enum):
    """ENUM to manage the types of `Acts` that can be inserted into Demeter."""

    PLANT = 1
    HARVEST = 2
    FERTILIZE = 3
    IRRIGATE = 4


@dataclass(frozen=True)
class Act(db.Detailed):
    """Spatiotemporal information for a management activity on a field."""

    field_id: db.TableId
    act_type: ActType
    date_performed: datetime
    geom_id: Optional[db.TableId] = None
    created: Optional[datetime] = None


# @dataclass(frozen=True)
# class PlantingKey(db.TableKey):
#     """Unique key to group any planting and/or harvest activity for a given field."""

#     crop_type_id: db.TableId
#     field_id: db.TableId


# @dataclass(frozen=True)
# class Planting(PlantingKey, db.Detailed):
#     """Spatiotemporal information for a planting activity on a field."""

#     act_id: db.TableId
#     date_performed: datetime
#     geom_id: Optional[db.TableId] = None
#     created: Optional[datetime] = None


# @dataclass(frozen=True)
# class Harvest(PlantingKey, db.Detailed):
#     """Spatiotemporal information for a harvest activity on a field."""

#     act_id: db.TableId
#     date_performed: datetime
#     geom_id: Optional[db.TableId] = None
#     created: Optional[datetime] = None


# @dataclass(frozen=True)
# class ReportType(db.TypeTable):
#     report: str
