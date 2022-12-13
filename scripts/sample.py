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
from demeter.data import UnitType, LocalType, LocalValue

# , FieldGroup
from demeter.data import (
    insertOrGetUnitType,
    insertOrGetFieldGroup,
    insertOrGetLocalType,
    insertOrGetLocalValue,
)
from demeter.db import getConnection

# %%
# if __name__ == "__main__":
load_dotenv()

c = getConnection(
    host_key="DEMETER_HOST_WSL",
    port_key="DEMETER_PORT_WSL",
    pw_key="DEMETER_PASSWORD_WSL",
    user_key="DEMETER_USER_WSL",
    db_key="DEMETER_DATABASE_WSL",
    schema_search_path="test_demeter,public",
)
# conn = c.connect()# this line is redundant seeing as engine.connect() is returned by function
trans = c.begin()
cursor = c.connection.cursor()

# from sqlalchemy import MetaData

# metadata_obj = MetaData(schema="test_demeter")
# metadata_obj.reflect(conn.engine)
# metadata_obj.tables["test_demeter.geom"]


# %%
root_group = FieldGroup(
    name="ABC Group INC",
    parent_field_group_id=None,
)
root_group_id = insertOrGetFieldGroup(cursor, root_group)
print(f"Root group id: {root_group_id}")

argentina_group = FieldGroup(
    name="Argentina",
    parent_field_group_id=root_group_id,
)
argentina_group_id = insertOrGetFieldGroup(cursor, argentina_group)
print(f"Argentina group id: {argentina_group_id}")

# %%
test_field_group = FieldGroup(
    name="grupo de prueba",
)
field_group_id = insertOrGetFieldGroup(cursor, test_field_group)
print(f"Field group id: {field_group_id}")

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
irrigation_type = LocalType(
    type_name="my_irrigation_type",
)
irrigation_type_id = insertOrGetLocalType(cursor, irrigation_type)
print(f"Irrigation type id: {irrigation_type_id}")

gallons_unit = UnitType(unit="gallons", local_type_id=irrigation_type_id)
gallons_unit_id = insertOrGetUnitType(cursor, gallons_unit)
print(f"Gallons type id: {gallons_unit_id}")

# %%
obs_geom = Point(-65.645145335822, -36.052968641022)
obs_geom_id = insertOrGetGeom(cursor, obs_geom)
print("Observation geom id: ", obs_geom_id)

o = LocalValue(
    geom_id=obs_geom_id,
    field_id=field_id,
    unit_type_id=gallons_unit_id,
    quantity=1234.567,
    acquired=datetime.now(),
)

observation_value_id = insertOrGetLocalValue(cursor, o)
print(f"Observation value id: {observation_value_id}")


# %%
# NOTE SQL transaction intentionally left uncommitted
trans.commit()
c.close()
# If you uncomment this, the script is no longer idempotent.
#  That is, any function 'insertFoo' will throw integrity errors when run a second time.
#  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts

# %%
