from typing import Optional

from dataclasses import dataclass
from datetime import datetime

from ... import db


@dataclass(frozen=True)
class Observation(db.Detailed):
    """An arbitrary observation/measurement of ObservationType that is spatiotemporal in nature
    and agronomically relevant. This observation can, but does not have to align with a specific
    agricultural field."""

    field_id: db.TableId
    unit_type_id: db.TableId
    observation_type_id: db.TableId
    date_observed: datetime
    value_observed: float
    created: datetime
    geom_id: Optional[db.TableId] = None
    act_id: Optional[db.TableId] = None


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
