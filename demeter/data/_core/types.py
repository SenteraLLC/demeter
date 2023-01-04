"""High-level API core Demeter data types"""

from dataclasses import dataclass, InitVar, field
from datetime import datetime
from typing import Optional

from enum import Enum

from ...db import TableId, Detailed, TypeTable


@dataclass(frozen=True)
class Field(Detailed):
    """Arbitrary spatiotemporal unit representing an agronomically-relevant
    area that is generally, but not always, managed as a single unit within
    the defined spatial and temporal constraints."""

    geom_id: TableId
    name: str
    field_group_id: Optional[TableId] = None
    created: Optional[datetime] = None


@dataclass(frozen=True)
class CropType(TypeTable, Detailed):
    """Information related to the plant cultivated through a Planting activity."""

    crop: str
    product_name: Optional[str] = None
    created: Optional[datetime] = None


class ActType(Enum):
    plant = "plant"
    harvest = "harvest"
    fertilize = "fertilize"
    irrigate = "irrigate"


@dataclass(frozen=True)
class Act(Detailed):
    """Spatiotemporal information for a management activity on a field.
    Types of management activities are limited to the types listed in `ActType`."""

    act_type: InitVar[ActType]

    name: str = field(init=False)
    field_id: TableId
    date_performed: datetime
    crop_type_id: Optional[TableId] = None
    geom_id: Optional[TableId] = None
    created: Optional[datetime] = None

    def __post_init__(self):
        """Be sure that `crop_type_id` is set for "plant" or "harvest" activity"""

        chk_act_type = object.__getattribute__(self, "act_type").value
        object.__setattr__(self, "name", chk_act_type)

        if chk_act_type in ["plant", "harvest"]:
            chk_crop_type_id = object.__getattribute__(self, "crop_type_id")
            if chk_crop_type_id is None:
                raise AttributeError(
                    f"Must pass `crop_type_id` with ActType {chk_act_type} "
                )


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
