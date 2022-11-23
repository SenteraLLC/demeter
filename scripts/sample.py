from datetime import datetime

from dotenv import load_dotenv

# Core Types
from demeter.data import Parcel, Field, ParcelGroup, Geom, MultiPolygon, Point
from demeter.data import (
    insertField,
    insertParcelGroup,
    insertOrGetGeom,
    insertOrGetParcel,
)

# Observation Types
from demeter.data import UnitType, ObservationType, ObservationValue, FieldGroup
from demeter.data import insertUnitType, insertObservationType, insertObservationValue, insertFieldGroup
from demeter.db import getConnection


if __name__ == "__main__":
    load_dotenv()

    c = getConnection()
    cursor = c.cursor()

    root_group = ParcelGroup(
        name="ABC Group INC",
        parent_parcel_group_id=None,
    )
    root_group_id = insertParcelGroup(cursor, root_group)
    print(f"Root group id: {root_group_id}")

    argentina_group = ParcelGroup(
        name="Argentina",
        parent_parcel_group_id=root_group_id,
    )
    argentina_group_id = insertParcelGroup(cursor, argentina_group)
    print(f"Argentina group id: {argentina_group_id}")

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

    test_field_group = ParcelGroup(
        name="grupo de prueba",
    )
    field_group_id = insertFieldGroup(cursor, test_field_group)
    print(f"Field group id: {field_group_id}")

    field_geom = Geom(
        type="Polygon",
        coordinates=bound,
        crs_name="urn:ogc:def:crs:EPSG::4326",
    )
    field_geom_id = insertOrGetGeom(cursor, field_geom)
    print(f"Field Geom id: {field_geom_id}")

    parcel = Parcel(
        geom_id=field_geom_id,
        parcel_group_id=argentina_group_id,
    )
    parcel_id = insertOrGetParcel(cursor, parcel)

    test_field = Field(
        name="campo de prueba",
        parcel_id=parcel_id,
    )
    field_id = insertField(cursor, test_field)
    print(f"Field id: {field_id}")

    irrigation_type = ObservationType(
        type_name="my_irrigation_type",
    )
    irrigation_type_id = insertObservationType(cursor, irrigation_type)
    print(f"Irrigation type id: {irrigation_type_id}")

    gallons_unit = UnitType(unit="gallons", observation_type_id=irrigation_type_id)
    gallons_unit_id = insertUnitType(cursor, gallons_unit)
    print(f"Gallons type id: {gallons_unit_id}")

    obs: Point = (-63.4545335800795, -35.59686410451283)
    obs_geom = Geom(
        type="Polygon",
        coordinates=obs,
        crs_name="urn:ogc:def:crs:EPSG::4326",
    )
    obs_geom_id = insertOrGetGeom(cursor, field_geom)
    print("Observation geom id: ", obs_geom_id)

    o = ObservationValue(
        geom_id=obs_geom_id,
        parcel_id=field_id,
        unit_type_id=gallons_unit_id,
        quantity=1234.567,
        acquired=datetime.now(),
    )

    observation_value_id = insertObservationValue(cursor, o)
    print(f"Observation value id: {observation_value_id}")

    # NOTE SQL transaction intentionally left uncommitted
    # cursor.commit()
    # If you uncomment this, the script is no longer idempotent.
    #  That is, any function 'insertFoo' will throw integrity errors when run a second time.
    #  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts
