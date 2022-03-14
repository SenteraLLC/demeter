# Python 3 server example
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import json

import urllib

hostName = "localhost"
serverPort = 8080

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

        result = hash(field_id + start + end)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        #self.wfile.write(bytes(result, "utf-8"))
        self.wfile.write(bytes(str(result), "utf-8"))


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

        result = hash(field_id + start + end)

        self.wfile.write(bytes(str(result), "utf-8"))


if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
