from ..lib.datasource.datasource import DataSource
from ..lib.datasource.types import OneToManyResponseFunction
from ..lib.datasource.s3_file import S3File
from ..functions.transformation import Transformation
from ..lib.local.types import LocalType

import geopandas as gpd  # type: ignore
from typing import Dict, List, Union, Tuple

# Long Term
# TODO: How to deal with dataframe indexes?
# TODO: Upload "init" file somewhere
# TODO: Allow init to override join behavior, manually return a gpd.GeoDataFrame
# TODO: Compare the headers of S3 outputs between function minor versions

# ASAP
# TODO: Should duplicate runs look a the major version of the function that outputs them? Should probably be considered
# TODO: Add functionality to reload subset of input
# TODO: Add functionality to reload multiple subsets of input
# TODO: Add functionality to skip function execution altogether
# TODO: Add functionality to download output locally when cached in S3
# TODO: Add 'idempotent' boolean property? And/or 'mapping' property?
#       There could be different types of 'Function' decorators
# TODO: ???? Add meta-functionality to toggle the above options?
# TODO: Show how to access other 'local' meta data like: owner, field
# TODO:   How to access quasi-local data like "harvest" and "planting"?

# TODO: How to deal with models that change from one spatial resolution to another
#       Post processing
#       E.G. Field level data -> Org level responses
#       E.G. How to get region level data when a field is searched
#       E.G. Multiple regions in field to scaler for a field

# TODO: Model metric functions?
# TODO: Model seletion function?
# TODO: Function for taking rasters and generating summary data
# TODO: How to do queries that use different geometries, for example, a transformation function that maps geometries to different geometries. E.G. field -> county -> county level data -> field

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
VERSION = 4
OUTPUTS = {"foo": "test_geojson_type"}

@Transformation(FUNCTION_NAME, VERSION, OUTPUTS, init)
def example_transformation(gdf : gpd.GeoDataFrame, some_constant : int):
  return {"foo": S3File(gdf, "testing_geojson")}


def cli(fn):
  from ..functions.util.mode import ExecutionMode
  fn(mode = ExecutionMode.CLI)
  #fn(mode = ExecutionMode.REGISTER)

if __name__ == "__main__":
  cli(example_transformation)

