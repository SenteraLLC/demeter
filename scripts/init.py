import psycopg2.extras

from demeter import connections

from demeter import FunctionType
from demeter import insertOrGetFunctionType

def main() -> None:
  connection = connections.getPgConnection()

  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  f = FunctionType(
        function_type_name='transformation',
        function_subtype_name=None
      )
  t_id = insertOrGetFunctionType(cursor, f)
  print("Transformation function id: ",t_id)

  connection.commit()

if __name__ == "__main__":
  main()
