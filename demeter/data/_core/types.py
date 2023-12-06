"""High-level API core Demeter data types"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from ...db import (
    Detailed,
    TableId,
    TypeTable,
)

"""Future behavior of finding duplicate fields in Demeter (1/9/2023):
1. Query the field table for a field_id that spatially intersects a spatiotemporal unit of interest. This requires that we have both a geometry, and a range of dates (remember, date_end defaults to infinity).
2. If no spatiotemporal intersection: Add in the new field, setting geom_id, date_start, and date_end (same as was performed on the query above.
3. If there is a spatial intersection, but not a temporal intersection: Still not a problem, we just would add the field using the attributes used for the query.
Note that if we tend to use infinity as date_end, this is probably a pretty rare scenario. It would mean that either the existing field that this intersects with or the potential field to add has a non-infinity value for date_end, which would have been explicitly set. There will probably be some logic here, that may be rather complicated. However, I think the level of complexity will probably be related to the level of complexity we decide to impose via setting date_start and date_end based on other data, like Acts.
4. If there is a spatial AND a temporal intersection: This is a problem that has to be fixed (as long as we assume two boundaries cannot CROSS, which I suggest we are strict about). First thing would be to inventory all the field_ids that this intersects with. Short term, I think we just employ the fix manually. Longer term, I'm hopeful that we can come up with a creative solution (and also hope it doesn't take too much effort) to automatically generate a new field(s) to cover the spatiotemporal extent of the intended upsert without intersecting any of the existing field_ids.
"""


@dataclass(frozen=True)
class Field(Detailed):
    """Arbitrary spatiotemporal unit representing an agronomically-relevant area that is generally, but not always,
    managed as a single unit within the defined spatial and temporal constraints."""

    name: str
    geom_id: TableId
    date_start: datetime
    date_end: datetime = field(default=datetime.max)
    grouper_id: Optional[TableId] = None


@dataclass(frozen=True)
class FieldTrial(Detailed):
    """A group of treatment plots geometrically organized according to an experimental design, whereby one and only one
    treatment is assigned to each plot. Two field trials shall not overlap one another spatially nor temporally, and a
    FieldTrial must fall fully within the spatial extent of the Field it is associated with.
    """

    name: str
    field_id: TableId
    date_start: datetime
    date_end: datetime = field(default=datetime.max)
    geom_id: Optional[TableId] = None
    grouper_id: Optional[TableId] = None


@dataclass(frozen=True)
class Plot(Detailed):
    """A spatiotemporal unit having a specified experimental treatment, usually defined by its management (planting,
    tillage, application, irrigation, and/or harvest). The management within a Plot shall be uniform across its
    spatiotemporal extent.
    """

    field_id: TableId
    field_trial_id: TableId
    name: str
    geom_id: Optional[TableId] = None
    treatment_id: Optional[int] = None
    block_id: Optional[int] = None
    replication_id: Optional[int] = None


@dataclass(frozen=True)
class CropType(TypeTable, Detailed):
    """Information related to the plant cultivated through a Planting activity."""

    crop: str
    product_name: str = None


list_act_types = ("plant", "harvest", "fertilize", "irrigate")


@dataclass(frozen=True)
class Act(Detailed):
    """Spatiotemporal information for a management activity on a field.
    Types of management activities are limited to the types listed in `ActType`."""

    act_type: str
    field_id: TableId
    date_performed: datetime
    crop_type_id: TableId = None
    geom_id: TableId = None

    def __post_init__(self):
        """Be sure that:
        - `act_type` is one of the correct possible values
        - `crop_type_id` is set for `act_type` = "plant" or "harvest"""

        chk_act_type = object.__getattribute__(self, "act_type")

        if chk_act_type not in list_act_types:
            raise AttributeError(
                f"`act_type` must be one of the following: {str(list_act_types)}"
            )

        if chk_act_type in ["plant", "harvest"]:
            if object.__getattribute__(self, "crop_type_id") is None:
                raise AttributeError(
                    f"Must pass `crop_type_id` with `act_type` = {chk_act_type} "
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


# @dataclass(frozen=True)
# class Harvest(PlantingKey, db.Detailed):
#     """Spatiotemporal information for a harvest activity on a field."""

#     act_id: db.TableId
#     date_performed: datetime
#     geom_id: Optional[db.TableId] = None


# @dataclass(frozen=True)
# class ReportType(db.TypeTable):
#     report: str
