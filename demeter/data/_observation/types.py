from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ... import db


@dataclass(frozen=True)
class Observation(db.Detailed):
    """An arbitrary observation/measurement of ObservationType that is spatiotemporal in nature
    and agronomically relevant. This observation can, but does not have to align with a specific
    agricultural field."""

    field_id: db.TableId
    unit_type_id: db.TableId
    observation_type_id: db.TableId
    value_observed: float
    date_observed: Optional[datetime] = None
    geom_id: Optional[db.TableId] = None
    act_id: Optional[db.TableId] = None
    created: Optional[datetime] = datetime.now()

    def __post_init__(self):

        this_act_id = object.__getattribute__(self, "act_id")

        # Ensure that at least one of `act_id` and `date_observed` is populated.
        if (
            this_act_id is None
            and object.__getattribute__(self, "date_observed") is None
        ):
            raise AttributeError("Must pass one of `act_id` or `date_observed`.")


# TODO: We need to impose constraints on which values can be passed to `type_name` to ensure that we aren't
# creating duplicates of types.
@dataclass(frozen=True)
class ObservationType(db.TypeTable):
    """Measurement type as it relates to collection methodology and/or agronomic interpretation."""

    type_name: str
    type_category: Optional[str] = None


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    """Reported units of an observation as should be interpreted in the scope of the observation type."""

    unit_name: str
    # aliases: list[str]
    observation_type_id: db.TableId
