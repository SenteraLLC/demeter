from ..lib.datasource import DataSource, S3
from ..lib.function import Function
from ..lib.types import LocalType

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

  parameters = {"field_id": 1, "start_date": "01/01/2020", "end_date": "01/06/2020"}
  foo_response = datasource.http("foo_type", params=parameters)
  print("Foo response: ",foo_response.text)

  request_body = {"field_id": 2, "start_date": "01/07/2020", "end_date": "01/12/2020"}
  bar_response = datasource.http("bar_type", json=request_body)
  print("Bar response: ",bar_response.text)

  #my_fizzbuzz = datasource.upload("fizzbuzz")
  my_s3 = datasource.download("sentera-sagemaker-dev", "foo.csv")
  print("MY S3: ",my_s3.read())

  return S3(gpd.GeoDataFrame())

if __name__ == "__main__":
  example_transformation(some_constant = 5)

  #geom_binary = schema_api.getGeom(cursor, g["geom_id"])
  #geom = wkb.loads(geom_binary, hex=True)

