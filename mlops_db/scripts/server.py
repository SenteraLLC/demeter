# Python 3 server example
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import datetime

import urllib

hostName = "localhost"
serverPort = 8080

dateFormat = "%Y-%m-%d"
parseDate = lambda s : datetime.datetime.strptime(s, dateFormat)
dateToString = lambda d : d.strftime(dateFormat)

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
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

        #self.wfile.write(bytes(result, "utf-8"))
        self.wfile.write(bytes(json.dumps(result), "utf-8"))


    def do_POST(self):
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


if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
