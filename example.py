from lib.datasource import DataSource, S3
from lib.function import Function
from lib.types import LocalType

# TODO: Stubs?

import geopandas as gpd  # type: ignore


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

  parameters = {"field_id": 1, "start_date": 2, "end_date": 3}
  my_stuff = datasource.http("foo_type", params=parameters)
  #my_fizzbuzz = datasource.s3.get("fizzbuzz")
  return S3(gpd.GeoDataFrame())

if __name__ == "__main__":
  example_transformation(some_constant = 5)


  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

