import psycopg2.extras

from demeter import connections

from demeter.task import FunctionType, S3Type, S3TypeDataFrame
from demeter.task import insertOrGetFunctionType, insertOrGetS3TypeDataFrame

def main() -> None:
  connection = connections.getPgConnection()

  cursor = connection.cursor()

  f = FunctionType(
        function_type_name='transformation',
        function_subtype_name=None
      )
  t_id = insertOrGetFunctionType(cursor, f)
  print("Transformation function id: ",t_id)

  s = S3Type(
    type_name = "input_geodataframe_type"
  )
  sdf = S3TypeDataFrame(
          driver = "GeoJSON",
          has_geometry = True,
        )
  s_id = insertOrGetS3TypeDataFrame(cursor, s, sdf)
  print("S3 Type id: ",s_id)

  connection.commit()

if __name__ == "__main__":
  main()
