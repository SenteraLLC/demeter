from datetime import datetime

from dotenv import load_dotenv

# Core Types
from demeter.data import Field, FieldGroup, Geom, MultiPolygon, Point
from demeter.data import insertField, insertFieldGroup, insertOrGetGeom

# Local Types
from demeter.data import UnitType, LocalType, LocalValue
from demeter.data import insertUnitType, insertLocalType, insertLocalValue

from demeter.db import getConnection


if __name__ == "__main__":
  load_dotenv()

  c = getConnection()
  cursor = c.cursor()

  root_group = FieldGroup(
                 name = "test field group",
                 parent_field_group_id = None,
                 external_id = "ABC Group Inc",
               )
  root_group_id = insertFieldGroup(cursor, root_group)
  print(f"Root group id: {root_group_id}")

  argentina_group = FieldGroup(
                      name = "Argentina",
                      parent_field_group_id = root_group_id,
                    )
  argentina_group_id = insertFieldGroup(cursor, argentina_group)
  print(f"Argentina group id: {argentina_group_id}")

  # NOTE: Use MultiPolygon instead of Polygon
  #       There isn't support for lone Polygons yet
  bound : MultiPolygon = (((
                       -65.4545335800795,
                       -35.19686410451283
                     ),
                     (
                       -62.702558113583365,
                       -35.193663907793265
                     ),
                     (
                       -62.78401558669485,
                       -36.94324941995089
                     ),
                     (
                       -65.55755160251499,
                       -36.87451965566409
                     ),
                     (
                       -65.45449008889159,
                       -35.18596793795566
                     )
                   ), )

  field_geom = Geom(
      type='Polygon',
      coordinates=bound,
      crs_name="urn:ogc:def:crs:EPSG::4326",
      container_geom_id=None,
  )
  field_geom_id = insertOrGetGeom(cursor, field_geom)
  print(f"Field Geom id: {field_geom_id}")

  test_field = Field(
                 name = "campo de prueba",
                 external_id = "123456789",
                 field_group_id = argentina_group_id,
                 geom_id = field_geom_id,
               )
  field_id = insertField(cursor, test_field)
  print(f"Field id: {field_id}")

  irrigation_type = LocalType(type_name="my_irrigation_type", type_category=None)
  irrigation_type_id = insertLocalType(cursor, irrigation_type)
  print(f"Irrigation type id: {irrigation_type_id}")

  gallons_unit = UnitType(unit="gallons", local_type_id=irrigation_type_id)
  gallons_unit_id = insertUnitType(cursor, gallons_unit)
  print(f"Gallons type id: {gallons_unit_id}")

  obs : Point = (-63.4545335800795, -35.59686410451283)
  obs_geom = Geom(
      type='Polygon',
      coordinates = obs,
      crs_name = "urn:ogc:def:crs:EPSG::4326",
      container_geom_id = None,
  )
  obs_geom_id = insertOrGetGeom(cursor, field_geom)
  print("Observation geom id: ",obs_geom_id)

  l = LocalValue(
        geom_id = obs_geom_id,
        field_id = field_id,
        unit_type_id = gallons_unit_id,
        quantity = 1234.567,
        local_group_id = None,
        acquired = datetime.now(),
  )

  local_value_id = insertLocalValue(cursor, l)
  print(f"Local value id: {local_value_id}")

  # NOTE SQL transaction intentionally left uncommitted
  # cursor.commit()
  # If you uncomment this, the script is no longer idempotent.
  #  That is, any function 'insertFoo' will throw integrity errors when run a second time.
  #  This can be remedied by swapping them with their 'insertOrGetFoo' counterparts
