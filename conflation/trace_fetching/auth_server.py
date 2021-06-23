#!/usr/bin/env python3
"""
update mapillary config items to take client_id and client_secret
generate auth url and print to screen, should be of the form: https://www.mapillary.com/connect?client_id={}
start the server above to wait for the auth callback to happen, note that the app has to be configured to call back localhost:8080
when the callback happens we authorize to get token in do_GET above, we should store the auth_token that is returned somewhere for later use in other requests
programatically kill the httpd
continue with the rest of the work


Usage::
    ./server.py mapillary_client_id mapillary_client_secret
"""
from sys import argv
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests

client_id = ''
client_secret = ''

class S(BaseHTTPRequestHandler):
    def _set_response(self, type='text/html'):
        self.send_response(200)
        self.send_header('Content-type', type)
        self.send_header('Origin', 'http://localhost:8080/')
        self.end_headers()

    def do_GET(self):
        auth_code = self.path[self.path.find('code=') + 5:]
        body = {'grant_type': 'authorization_code', 'code': auth_code}
        headers = {'Authorization': 'OAuth MLY|{}|{}'.format(client_id, client_secret)}
        resp = requests.post('https://graph.mapillary.com/token?client_id={}'.format(client_id), json = body, headers = headers).text
        self._set_response('application/json')
        self.wfile.write(resp.encode('utf-8'))

def run(server_class=HTTPServer, handler_class=S, port=8080):
    server_address = ('localhost', port)
    httpd = server_class(server_address, handler_class)
    print('Starting httpd...')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print('Stopping httpd...')

if __name__ == '__main__':
    client_id = sys.argv[1]
    client_secret = sys.argv[2]
    run()
