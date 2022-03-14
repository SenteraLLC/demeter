from ..lib.datasource import DataSource, S3
from ..lib.function import Function
from ..lib.types import LocalType

import geopandas as gpd  # type: ignore

from typing import Callable, Any
# Mocked function names
centroid : Callable[[Any], Any] = lambda x : x
processData : Callable[[Any], gpd.GeoDataFrame] = lambda x : x


@Function("my_function", 1)
def example_transformation(datasource : DataSource, some_constant : int) -> S3[gpd.GeoDataFrame]:
  print("Some constant: ",some_constant)

  print("Keys: ")
  for k in datasource.keys():
    print(k)

  nitrogen = datasource.local(LocalType(type_name="nitrogen",
                                        type_category="application",
                                       ),
                             )
  for i, n in enumerate(nitrogen):
    print("Result #",i)
    print(n)
    print("\n")

  parameters = lambda k : {"field_id"   : k.field_id(),
                           "start_date" : k.start_date(),
                           "end_date"   : k.end_date(),
                          }
  datasource.http("foo_type", params=parameters)

  request_body = lambda k : {"lat": centroid(k.geometry()).lat,
                             "long": centroid(k.geometry()).long,
                             "start_date": k.start_date(),
                             "end_date": k.end_date()
                            }
  datasource.http("bar_type", json=request_body)

  # TODO: Auto-fetch results using 's3_object' table
  datasource.download(database.keys(), "sentera-sagemaker-dev", "my_s3_type")

  input_matrix = datasource.join()   # type: ignore

  feature_matrix : gpd.GeoDataFrame = processData(input_matrix)

  # TODO: Record output and relatd keys
  return S3("fizzbuzz_type", feature_matrix)



if __name__ == "__main__":
  example_transformation(some_constant = 5)

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

