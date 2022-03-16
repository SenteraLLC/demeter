from ..lib.datasource import DataSource
from ..lib.ingest import S3File
from ..lib.function import Function
from ..lib.types import LocalType

import geopandas as gpd  # type: ignore


@Function("my_function", 1)
def example_transformation(datasource : DataSource, some_constant : int) -> S3File[gpd.GeoDataFrame]:
  print("Some constant: ",some_constant)

  print("\nKeys: ")
  for k in datasource.keys:
    print(k)

  nitrogen = datasource.local(LocalType(type_name="nitrogen",
                                        type_category="application",
                                       ),
                             )
  print("\nLocal nitrogen data: ")
  for i, n in enumerate(nitrogen):
    print("Result #",i)
    print(n)

  parameters = lambda k : {"field_id"   : k["field_id"],
                           "start_date" : k["start_date"],
                           "end_date"   : k["end_date"],
                          }

  foo_responses = datasource.http("foo_type", param_fn=parameters)
  print("\nFoo responses: ",foo_responses)

  request_body = lambda k : {"field_id"   : k["field_id"],
                             "start_date" : k["start_date"],
                             "end_date"   : k["end_date"]
                            }

  bar_responses = datasource.http("bar_type", json_fn=request_body)
  print("\nBar responses: ",bar_responses)

  my_s3_results = datasource.s3("my_test_type")
  print("\nS3 Results: ",my_s3_results)

  geoms = datasource.get_geometry()
  print("\nGeoms: ",geoms)

  # TODO: Record output and related keys
  return S3File("test_geojson_type", geoms)


def main():
  example_transformation(some_constant = 5)


if __name__ == "__main__":
  main()

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

