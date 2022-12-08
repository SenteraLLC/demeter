# %%
from datetime import datetime

from dotenv import load_dotenv

# Core Types
from demeter.data import Field, Geom, MultiPolygon, Point, FieldGroup
from demeter.data import (
    insertOrGetField,
    insertOrGetGeom,
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

c = getConnection()
cursor = c.cursor()

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
# NOTE: Use MultiPolygon instead of Polygon
#       There isn't support for lone Polygons yet
bound: MultiPolygon = (
    (
        (-65.4545335800795, -35.19686410451283),
        (-62.702558113583365, -35.193663907793265),
        (-62.78401558669485, -36.94324941995089),
        (-65.55755160251499, -36.87451965566409),
        (-65.45449008889159, -35.18596793795566),
    ),
)

test_field_group = FieldGroup(
    name="grupo de prueba",
)
field_group_id = insertOrGetFieldGroup(cursor, test_field_group)
print(f"Field group id: {field_group_id}")

field_geom = Geom(
    type="Polygon",
    coordinates=bound,
    crs_name="urn:ogc:def:crs:EPSG::4326",
)
field_geom_id = insertOrGetGeom(cursor, field_geom)
print(f"Field Geom id: {field_geom_id}")

field = Field(
    geom_id=field_geom_id,
    name = 'Test field',
    field_group_id=argentina_group_id,
)
field_id = insertOrGetField(cursor, field)

irrigation_type = LocalType(
    type_name="my_irrigation_type",
)
irrigation_type_id = insertOrGetLocalType(cursor, irrigation_type)
print(f"Irrigation type id: {irrigation_type_id}")

gallons_unit = UnitType(unit="gallons", local_type_id=irrigation_type_id)
gallons_unit_id = insertOrGetUnitType(cursor, gallons_unit)
print(f"Gallons type id: {gallons_unit_id}")

obs: Point = (-63.4545335800795, -35.59686410451283)
obs_geom = Geom(
    type="Polygon",
    coordinates=obs,
    crs_name="urn:ogc:def:crs:EPSG::4326",
)
obs_geom_id = insertOrGetGeom(cursor, field_geom)
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

    # NOTE SQL transaction intentionally left uncommitted
c.commit()
    # If you uncomment this, the script is no longer idempotent.
    #  That is, any function 'insertFoo' will throw integrity errors when run a second time.
    #  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts

# %%
