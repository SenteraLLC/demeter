from ..lib.datasource import DataSource
from ..lib.ingest import S3File
from ..lib.function import Function
from ..lib.types import LocalType

import geopandas as gpd  # type: ignore


@Function("my_function", 1)
def example_transformation(datasource : DataSource, some_constant : int) -> S3File[gpd.GeoDataFrame]:
  print("Some constant: ",some_constant)

  print("Keys: ")
  for k in datasource.keys:
    print(k)

  nitrogen = datasource.local(LocalType(type_name="nitrogen",
                                        type_category="application",
                                       ),
                             )
  for i, n in enumerate(nitrogen):
    print("Result #",i)
    print(n)
    print("\n")

  parameters = {"field_id": 1, "start_date": "01/01/2020", "end_date": "01/06/2020"}
  #parameters = lambda k : {"field_id"   : k.field_id(),
  #                         "start_date" : k.start_date(),
  #                         "end_date"   : k.end_date(),
  #                        }

  foo_response = datasource.http("foo_type", params=parameters)
  print("Foo response: ",foo_response.text)

  request_body = {"field_id": 2, "start_date": "01/07/2020", "end_date": "01/12/2020"}
  #request_body = lambda k : {"field_id": k.field_id(),
  #                           "start_date": k.start_date(),
  #                           "end_date": k.end_date()
  #                          }

  bar_response = datasource.http("bar_type", json=request_body)
  print("Bar response: ",bar_response.text)

  #my_fizzbuzz = datasource.upload("fizzbuzz")

  # TODO: Auto-fetch results using 's3_object' table
  #for k in datasource.keys:
  #  my_s3_results = datasource.download("fizzbuzz_type", k)
  #  print("Key: ",k)
  #  print("  S3 results: ",my_s3_results.read())

  geoms = datasource.get_geometry()
  print("Geoms: ",geoms)

  # TODO: Record output and related keys
  return S3File("test_geojson_type", geoms)


def main():
  example_transformation(some_constant = 5)


if __name__ == "__main__":
  main()

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

