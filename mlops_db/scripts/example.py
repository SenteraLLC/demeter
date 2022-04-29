from ..lib.datasource import DataSource, OneToManyResponseFunction
from ..lib.ingest import S3File
from ..lib.function import Function, ExecutionMode
from ..lib.types import LocalType

import pandas as pd
import geopandas as gpd  # type: ignore
from typing import Dict, List, Union, Tuple

# Long Term
# TODO: How to deal with dataframe indexes?
# TODO: Upload "init" file somewhere
# TODO: Allow init to override join behavior, manually return a gpd.GeoDataFrame

# ASAP
# TODO: Add functionality to reload subset of input
# TODO: Add functionality to reload multiple subsets of input
# TODO: Add functionality to skip function execution altogether
# TODO: Add functionality to download output locally when cached in S3
# TODO: Add 'idempotent' boolean property? And/or 'mapping' property?
#       There could be different types of 'Function' decorators
# TODO: ???? Add meta-functionality to toggle the above options?

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
  datasource.http("bar_type", json_fn=request_body)

  datasource.s3("my_test_geo_type"),

  datasource.s3("my_test_nongeo_type")

  datasource.join(datasource.GEOM, "my_test_geo_type", gpd.GeoDataFrame.sjoin, lsuffix = "primary", rsuffix = "my_test_geo_type")

  # How to join foo::start_date - foo::end_date with bar::date
  #datasource.join("foo_type", "bar_type", pd.DataFrame.merge, on=["geom_id", "date"], how="outer")



FUNCTION_NAME = "my_function"
VERSION = 2
OUTPUTS = {"foo": "test_geojson_type"}

@Function(FUNCTION_NAME, VERSION, OUTPUTS, init)
def example_transformation(gdf : gpd.GeoDataFrame, some_constant : int):
  return {"foo": S3File(gdf, "testing_geojson")}


def cli(fn):
  fn(mode = ExecutionMode.CLI)
  #fn(mode = ExecutionMode.DRY)



if __name__ == "__main__":
  cli(example_transformation)

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

