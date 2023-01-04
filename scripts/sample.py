# %%
from datetime import datetime
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon
from demeter.data import (
    Act,
    ActType,
    CropType,
    Field,
    FieldGroup,
    Observation,
    ObservationType,
    UnitType,
    insertOrGetAct,
    insertOrGetCropType,
    insertOrGetField,
    insertOrGetFieldGroup,
    insertOrGetGeom,
    insertOrGetObservation,
    insertOrGetObservationType,
    insertOrGetUnitType,
)
from demeter.db import getConnection

# %% connect to database

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

# %% Add field groups

sa_group = FieldGroup(
    name="South America", parent_field_group_id=None, details={"external_id": 107}
)
sa_group_id = insertOrGetFieldGroup(cursor, sa_group)
print(
    f"Org name: {sa_group.name}\n  field_group_id: {sa_group_id}\n  parent_field_group_id: {sa_group.parent_field_group_id}"
)


argentina_group = FieldGroup(
    name="Argentina", parent_field_group_id=sa_group_id, details={"external_id": 1007}
)
argentina_group_id = insertOrGetFieldGroup(cursor, argentina_group)
print(
    f"Org name: {argentina_group.name}\n  field_group_id: {argentina_group}\n  parent_field_group_id: {argentina_group.parent_field_group_id}"
)

test_field_group = FieldGroup(
    name="Argonomist 1",
    parent_field_group_id=argentina_group_id,
    details={"external_id": 10007},
)
field_group_id = insertOrGetFieldGroup(cursor, test_field_group)
print(
    f"Org name: {argentina_group.name}\n  field_group_id: {argentina_group}\n  parent_field_group_id: {argentina_group.parent_field_group_id}"
)

# %% Add field data with geometry

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

field_geom_id = insertOrGetGeom(cursor, field_geom)
print(f"Field Geom id: {field_geom_id}")

field = Field(
    geom_id=field_geom_id,
    name="Test field",
    field_group_id=field_group_id,
    details={"external_id": 10736},
)
field_id = insertOrGetField(cursor, field)

# %% Add crop season information

crop_type = CropType(crop="barley", product_name="abi voyager")
crop_type_id = insertOrGetCropType(cursor, crop_type)

field_planting = Act(
    act_type=ActType.plant,
    field_id=field_id,
    date_performed=datetime(2022, 6, 1),
    crop_type_id=crop_type_id,
)
planting_id = insertOrGetAct(cursor, field_planting)

field_replanting = Act(
    act_type=ActType.plant,
    field_id=field_id,
    date_performed=datetime(2022, 6, 15),
    crop_type_id=crop_type_id,
)
replanting_id = insertOrGetAct(cursor, field_replanting)


field_harvest = Act(
    act_type=ActType.harvest,
    field_id=field_id,
    date_performed=datetime(2022, 10, 1),
    crop_type_id=crop_type_id,
)
harvest_id = insertOrGetAct(cursor, field_harvest)

# %%
obs_geom = Point(-65.645145335822, -36.052968641022)
obs_geom_id = insertOrGetGeom(cursor, obs_geom)
print("Observation geom id: ", obs_geom_id)

agronomic_yield_type = ObservationType(type_name="agronomic barley yield")
ag_yield_id = insertOrGetObservationType(cursor, agronomic_yield_type)

kg_ha_ag_yield_unit = UnitType(
    unit_name="kilograms per hectare", observation_type_id=ag_yield_id
)
unit_1_id = insertOrGetUnitType(cursor, kg_ha_ag_yield_unit)

malt_barley_yield_type = ObservationType(type_name="malt barley yield")
malt_yield_id = insertOrGetObservationType(cursor, malt_barley_yield_type)

kg_ha_malt_yield_unit = UnitType(
    unit_name="kilograms per hectare", observation_type_id=malt_yield_id
)
unit_2_id = insertOrGetUnitType(cursor, kg_ha_malt_yield_unit)

## FIXME: Do we want an observation to inherit date from `Act` if `act_id` is given?
## FIXME: We also should be sure that the `observation_type_id` is the same as that in
## the specified unit type.

obs_agronomic = Observation(
    field_id=field_id,
    unit_type_id=unit_1_id,
    observation_type_id=ag_yield_id,
    date_observed=datetime(2022, 10, 1),
    value_observed=5500,
    geom_id=obs_geom_id,
    act_id=harvest_id,
)

observation_value_id = insertOrGetObservation(cursor, obs_agronomic)


obs_malt = Observation(
    field_id=field_id,
    unit_type_id=unit_2_id,
    observation_type_id=malt_yield_id,
    date_observed=datetime(2022, 10, 1),
    value_observed=5000,
    geom_id=obs_geom_id,
    act_id=harvest_id,
)
observation_value_2_id = insertOrGetObservation(cursor, obs_malt)

# %%
field_irrigate = Act(
    act_type=ActType.irrigate,
    field_id=field_id,
    date_performed=datetime(2022, 6, 1),
)
irrigate_id = insertOrGetAct(cursor, field_irrigate)


# %%
# NOTE SQL transaction intentionally left uncommitted
trans.commit()
# c.close()
# If you uncomment this, the script is no longer idempotent.
#  That is, any function 'insertFoo' will throw integrity errors when run a second time.
#  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts

# %%
