from ..lib.datasource import DataSource
from ..lib.ingest import S3File
from ..lib.function import Function, Load
from ..lib.types import LocalType

import geopandas as gpd  # type: ignore
from typing import Dict
# TODO: Defer datasource loading
# TODO: Register function signature with database
#       This will require a preliminary run
#       Should always double-check in the future that this hasn't changed
# TODO: Upload "init" file somewhere
# TODO: Add functionality to reload input
# TODO: Add functionality to reload subset of input
# TODO: Add functionality to reload multiple subsets of input
# TODO: Add functionality to skip function execution altogether
# TODO: Add functionality to download output locally when cached in S3
# TODO: Add 'idempotent' boolean property? And/or 'mapping' property?
# TODO: ???? Add meta-functionality to toggle the above options?


@Load
def init(datasource : DataSource, some_constant : int):
  datasource.local(LocalType(type_name="nitrogen",
                             type_category="application",
                            ),
                   )

  parameters = lambda k : {"field_id"   : k["field_id"],
                           "start_date" : k["start_date"],
                           "end_date"   : k["end_date"],
                          }
  datasource.http("foo_type", param_fn=parameters)

  request_body = lambda k : {"field_id"   : k["field_id"],
                             "start_date" : k["start_date"],
                             "end_date"   : k["end_date"]
                            }
  datasource.http("bar_type", json_fn=request_body)

  datasource.s3("my_test_type")


FUNCTION_NAME = "my_function"
VERSION = 1
OUTPUTS = {"foo": "test_geojson_type"}

@Function(FUNCTION_NAME, VERSION, OUTPUTS)
def example_transformation(datasource : DataSource, some_constant : int):
  print("Some constant: ",some_constant)

  inputs = init(datasource, some_constant)
  print("Inputs: ",inputs)

  geoms = datasource.get_geometry()
  print("\nGeoms: ",geoms)

  #m = datasource.getMatrix()

  # TODO: Record output and related keys
  #return {"foo": "test_geojson_type"}
  return S3File("test_geojson_type", geoms)


def main():
  example_transformation(some_constant = 5)


if __name__ == "__main__":
  main()

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

