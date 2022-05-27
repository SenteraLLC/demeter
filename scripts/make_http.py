from typing import Callable

import psycopg2
import psycopg2.extras

from ..demeter.types.inputs import HTTPType, HTTPVerb, RequestBodySchema
from ..demeter.inputs import insertHTTPType

http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

http_verb_to_postgres = lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"]))
psycopg2.extensions.register_adapter(HTTPVerb, http_verb_to_postgres)

json_schema_to_postgres = lambda d : psycopg2.extras.Json(d.schema)
psycopg2.extensions.register_adapter(RequestBodySchema, json_schema_to_postgres)


if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  http_get = HTTPType(
               type_name           = "http_uri_params_test_type",
               verb                = HTTPVerb.GET,
               uri                 = "http://localhost:8080",
               uri_parameters      = ["field_id", "start_date", "end_date"],
               request_body_schema = None,
             )
  http_get_id = insertHTTPType(cursor, http_get)
  print("HTTP GET ID: ",http_get_id)


  http_post = HTTPType(
                type_name           = "http_request_body_test_type",
                verb                = HTTPVerb.POST,
                uri                 = "http://localhost:8080",
                uri_parameters      = None,
                request_body_schema = RequestBodySchema({"type": "object",
                                                         "properties": {
                                                             "field_id":   {"type": "number"},
                                                             "start_date": {"type": "string",
                                                                            "format": "date"
                                                                           },
                                                             "end_date":   {"type": "string",
                                                                            "format": "date"
                                                                           },

                                                         }
                                                       })
              )
  http_post_id = insertHTTPType(cursor, http_post)
  print("HTTP POST ID: ",http_post_id)

  connection.commit()


