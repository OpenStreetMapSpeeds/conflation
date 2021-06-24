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
import requests
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer


AUTH_URL = "https://www.mapillary.com/connect?client_id={}"

client_id = ""
client_secret = ""
access_token = ""


class S(BaseHTTPRequestHandler):
    def _set_response(self, type="text/html"):
        self.send_response(200)
        self.send_header("Content-type", type)
        self.send_header("Origin", "http://localhost:8080/")
        self.end_headers()

    def do_GET(self):
        global client_id
        global client_secret
        global access_token
        auth_code = self.path[self.path.find("code=") + 5 :]
        body = {"grant_type": "authorization_code", "code": auth_code}
        headers = {"Authorization": "OAuth MLY|{}|{}".format(client_id, client_secret)}
        resp = requests.post(
            "https://graph.mapillary.com/token?client_id={}".format(client_id),
            json=body,
            headers=headers,
        ).json()
        access_token = resp["access_token"]
        raise KeyboardInterrupt


def run(
    client_id_: str, client_secret_: str, server_class=HTTPServer, handler_class=S, port=8080
) -> str:
    global client_id
    global client_secret
    global access_token

    client_id = client_id_
    client_secret = client_secret_

    server_address = ("localhost", port)
    httpd = server_class(server_address, handler_class)
    print("Starting httpd and opening Mapillary to authenticate...")
    try:
        webbrowser.open_new_tab(AUTH_URL.format(client_id))
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Stopping httpd...")
    return access_token
