# %%
from datetime import datetime

from dotenv import load_dotenv
from shapely.geometry import Point, Polygon

# Core Types
from demeter.data import (
    Field,
    # Geom,
    FieldGroup,
    Planting,
    Harvest,
    CropType,
)
from demeter.data import (
    insertOrGetField,
    insertOrGetGeom,
    insertOrGetCropType,
    insertOrGetPlanting,
    insertOrGetHarvest,
)

# Observation Types
from demeter.data import UnitType, ObservationType, Observation

# , FieldGroup
from demeter.data import (
    insertOrGetUnitType,
    insertOrGetFieldGroup,
    insertOrGetObservationType,
    insertOrGetObservation,
)
from demeter.db import getConnection

# %%
# if __name__ == "__main__":
load_dotenv()

conn = getConnection(env_name="TEST_DEMETER")
# engine = getEngine(env_name="TEST_DEMETER")
# Session = getSession(env_name="TEST_DEMETER")

# cursor = Session().connection().connection.cursor()
cursor = conn.connection.cursor()
trans = conn.begin()

# from sqlalchemy import MetaData

# metadata_obj = MetaData(schema="test_demeter")
# metadata_obj.reflect(conn.engine)
# metadata_obj.tables["test_demeter.geom"]

# %% Add data

# %%
sa_group = FieldGroup(
    name="South America",
    parent_field_group_id=None,
)
sa_group_id = insertOrGetFieldGroup(cursor, sa_group)
print(
    f"Org name: {sa_group.name}\n  field_group_id: {sa_group_id}\n  parent_field_group_id: {sa_group.parent_field_group_id}"
)


argentina_group = FieldGroup(
    name="Argentina",
    parent_field_group_id=sa_group_id,
)
argentina_group_id = insertOrGetFieldGroup(cursor, argentina_group)
print(
    f"Org name: {argentina_group.name}\n  field_group_id: {argentina_group}\n  parent_field_group_id: {argentina_group.parent_field_group_id}"
)

test_field_group = FieldGroup(
    name="grupo de prueba",
)
field_group_id = insertOrGetFieldGroup(cursor, test_field_group)
print(
    f"Org name: {argentina_group.name}\n  field_group_id: {argentina_group}\n  parent_field_group_id: {argentina_group.parent_field_group_id}"
)

# %%

# NOTE: Be sure to use WGS-84 CRS (EPSG:4326) - `demeter` assumes geoms get entered in that format
# TODO: Add -180 to +180 and -90 to +90 coord constraint for Geom table?
field_geom = Polygon(
    [
        Point(-65.45453358, -36.19686410),
        Point(-65.70255811, -36.19366390),
        Point(-65.78401558, -35.94324941),
        Point(-65.55755160, -35.87451965),
        Point(-65.45449008, -36.18596793),
        Point(-65.45453358, -36.19686410),
    ]
)

# field_geom = Geom(geom=geometry)
field_geom_id = insertOrGetGeom(cursor, field_geom)
print(f"Field Geom id: {field_geom_id}")

field = Field(
    geom_id=field_geom_id,
    name="Test field",
    field_group_id=argentina_group_id,
)
field_id = insertOrGetField(cursor, field)

# %%
crop_type = CropType(species="barley")
crop_type_id = insertOrGetCropType(cursor, crop_type)

field_planting = Planting(
    crop_type_id=crop_type_id,
    field_id=field_id,
    planted=datetime(2022, 6, 1),
)

planting_key = insertOrGetPlanting(cursor, field_planting)

# %%
field_harvest = Harvest(
    crop_type_id=crop_type_id, field_id=field_id, planted=datetime(2022, 6, 1)
)

harvest_id = insertOrGetHarvest(cursor, field_harvest)

# %%
irrigation_type = ObservationType(
    type_name="my_irrigation_type",
)
irrigation_type_id = insertOrGetObservationType(cursor, irrigation_type)
print(f"Irrigation type id: {irrigation_type_id}")

gallons_unit = UnitType(unit_name="gallons", observation_type_id=irrigation_type_id)
gallons_unit_id = insertOrGetUnitType(cursor, gallons_unit)
print(f"Gallons type id: {gallons_unit_id}")

# %%
obs_geom = Point(-65.645145335822, -36.052968641022)
obs_geom_id = insertOrGetGeom(cursor, obs_geom)
print("Observation geom id: ", obs_geom_id)

o = Observation(
    geom_id=obs_geom_id,
    observation_type_id=irrigation_type_id,
    field_id=field_id,
    unit_type_id=gallons_unit_id,
    date_observed=datetime(2022, 1, 1),
    value_observed=1234.567,
    created=datetime.now(),
)

observation_value_id = insertOrGetObservation(cursor, o)
print(f"Observation value id: {observation_value_id}")


# %%
# NOTE SQL transaction intentionally left uncommitted
trans.commit()
# c.close()
# If you uncomment this, the script is no longer idempotent.
#  That is, any function 'insertFoo' will throw integrity errors when run a second time.
#  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts

# %%
