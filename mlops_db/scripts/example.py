from ..lib.datasource import DataSource, OneToManyResponseFunction
from ..lib.ingest import S3File
from ..lib.function import Function, Load
from ..lib.types import LocalType

import pandas as pd
import geopandas as gpd  # type: ignore
from typing import Dict, List, Union, Tuple
# TODO: Defer datasource loading
# TODO: How much to enforce dataframe indexing?
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

# TODO: Handle missing data that becomes available

# TODO: Allow init to override join behavior, manually return a gpd.GeoDataFrame
@Load
def init(datasource : DataSource, some_constant : int) -> None:
  datasource.local([LocalType(type_name="nitrogen",
                              type_category="application",
                             ),
                    LocalType(type_name="urea ammonium nitrate",
                              type_category="application",
                             ),
                  ])

  parameters = lambda k : {"field_id"   : k["field_id"],
                           "start_date" : k["start_date"],
                           "end_date"   : k["end_date"],
                          }
  datasource.http("foo_type", param_fn=parameters, response_fn=OneToManyResponseFunction)


  request_body = lambda k : {"field_id"   : k["field_id"],
                             "start_date" : k["start_date"],
                             "end_date"   : k["end_date"]
                            }
  bar_df = datasource.http("bar_type", json_fn=request_body)

  datasource.s3("my_test_geo_type"),

  datasource.s3("my_test_nongeo_type")

  datasource.join(datasource.GEOM, "my_test_geo_type", gpd.GeoDataFrame.sjoin, lsuffix = "primary", rsuffix = "my_test_geo_type")

  #datasource.join("foo_type", "bar_type", pd.DataFrame.merge, on=["geom_id", "date"], how="outer")



FUNCTION_NAME = "my_function"
VERSION = 1

# TODO: Pass init function as argument here
@Function(FUNCTION_NAME, VERSION, init)
#def example_transformation(gdf : gpd.GeoDataFrame, some_constant : int):
def example_transformation(datasource : DataSource, some_constant : int):
  init(datasource, 5)

  print("Some constant: ",some_constant)

  # TODO: Record output and related keys
  import sys
  sys.exit(1234)
  #return {"foo": S3File("test_geojson_type", gdf, key_prefix="testing_geojson")}


def main():
  example_transformation(some_constant = 5)


if __name__ == "__main__":
  main()

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

