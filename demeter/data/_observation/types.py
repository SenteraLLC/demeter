from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from demeter import db

list_file_format_type = ("PARQUET", "TIF", "CSV", "GEOJSON", "JSON")
list_category_type = (
    "REMOTE_SENSING",
    "SOIL",
    "TISSUE",
    "GRAIN",
    "STOVER",
    "WEATHER",
    "SENSOR",
)


@dataclass(frozen=True)
class S3(db.Detailed):
    """A reference to a file stored in S3."""

    s3_url: Union[str, Path]
    organization_id: db.TableId
    file_format: str = None
    category: str = None

    def __post_init__(self):
        chk_file_format = object.__getattribute__(self, "file_format")
        chk_category = object.__getattribute__(self, "category")
        chk_s3_url = object.__getattribute__(self, "s3_url").lower()

        if chk_file_format not in list_file_format_type:
            raise AttributeError(
                f"`file_format` must be one of the following: {str(list_file_format_type)}"
            )
        if chk_category not in list_category_type:
            raise AttributeError(
                f"`category` must be one of the following: {str(list_category_type)}"
            )
        if chk_file_format.lower() != Path(chk_s3_url).suffix[1:]:
            raise AttributeError(
                f"`file_format` {chk_file_format.lower()} does not match file_format provided via `s3_url` {Path(chk_s3_url).suffix[1:]}"
            )


@dataclass(frozen=True)
class Observation(db.Detailed):
    """An arbitrary observation/measurement of ObservationType that is spatiotemporal in nature
    and agronomically relevant. This observation can, but does not have to align with a specific
    agricultural field."""

    field_id: db.TableId
    unit_type_id: db.TableId
    observation_type_id: db.TableId
    value_observed: float
    date_observed: datetime = None
    geom_id: db.TableId = None
    act_id: db.TableId = None

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
class ObservationType(db.Detailed):
    """Measurement type as it relates to collection methodology and/or agronomic interpretation."""

    observation_type_name: str
    category: str = None
    # TODO: Analytic and Sensor should be their own tables (can probably generate from the Product Catalog via GQL API)
    # analytic_name: str = None
    # sensor_name: str = None
    # statistic_type: str = None
    # masked: bool = None


@dataclass(frozen=True)
class UnitType(db.TypeTable):
    """Reported units of an observation as should be interpreted in the scope of the observation type."""

    unit_name: str
    # aliases: list[str]
    observation_type_id: db.TableId
