# Python 3 server example
from typing import Callable

import json
import urllib
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2

from demeter import getPgConnection

from demeter.task import HTTPType, HTTPVerb
from demeter.task import insertOrGetHTTPType


hostName = "localhost"
serverPort = 8080

dateFormat = "%Y-%m-%d"
parseDate : Callable[[str], datetime] = lambda s : datetime.strptime(s, dateFormat)
dateToString : Callable[[datetime], str] = lambda d : d.strftime(dateFormat)

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        query = urllib.parse.urlparse(self.path).query
        query = urllib.parse.unquote(query)
        query_components = dict(qc.split("=") for qc in query.split("&"))
        try:
            field_id = query_components["field_id"]
            start = query_components["start_date"]
            end = query_components["end_date"]
        except KeyError:
            self.send_response(400)
            return

        start_date = parseDate(start)
        end_date = parseDate(end)
        interval_half = (end_date - start_date) / 2

        middle_date = start_date + interval_half
        three_dates = [start_date, middle_date, end_date]

        result = []

        for d in three_dates:
          date = dateToString(d)
          result.append({"hash": hash(field_id + date), "date": date})

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        self.wfile.write(bytes(json.dumps(result), "utf-8"))


    def do_POST(self) -> None:
        '''Reads post request body'''
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        content_len = int(self.headers.get('content-length', 0))
        post_body = self.rfile.read(content_len)
        as_json = json.loads(post_body)

        field_id = str(as_json["field_id"])
        start = as_json["start_date"]
        end = as_json["end_date"]

        result = {"hash": hash(field_id + start + end)}

        self.wfile.write(bytes(json.dumps(result), "utf-8"))


def registerTypes() -> None:
  http_verb_to_string : Callable[[HTTPVerb], str] = lambda v : v.name.lower()

  http_verb_to_postgres = lambda v : psycopg2.extensions.AsIs("".join(["'", http_verb_to_string(v), "'"]))
  psycopg2.extensions.register_adapter(HTTPVerb, http_verb_to_postgres)

  connection = getPgConnection()
  cursor = connection.cursor()
  http_get = HTTPType(
               type_name           = "http_uri_params_test_type",
               verb                = HTTPVerb.GET,
               uri                 = "http://localhost:8080",
               uri_parameters      = ["field_id", "start_date", "end_date"],
               request_body_schema = None,
             )
  http_get_id = insertOrGetHTTPType(cursor, http_get)
  print("HTTP GET ID: ",http_get_id)


  http_post = HTTPType(
                type_name           = "http_request_body_test_type",
                verb                = HTTPVerb.POST,
                uri                 = "http://localhost:8080",
                uri_parameters      = None,
                request_body_schema = {"type": "object",
                                       "properties": {
                                         "field_id":   {"type": "number"},
                                         "start_date": {"type": "string",
                                         "format": "date"
                                       },
                                      "end_date":   {"type": "string",
                                                     "format": "date"
                                                    },
                                      }
                                     }
            )
  http_post_id = insertOrGetHTTPType(cursor, http_post)
  print("HTTP POST ID: ",http_post_id)

  connection.commit()



if __name__ == "__main__":
    registerTypes()

    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")



