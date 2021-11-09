#!/usr/bin/env python3
import logging
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
        """
        This endpoint is opened so that we can receive the access token from Mapillary. When the user authenticates
        through the OAuth link, Mapillary redirects to localhost:8080 (configured inside Mapillary dashboard) and this
        endpoint catches that and parses the auth code and makes a request to get the access token. After getting the
        token, it stores it in a global variable and closes the HTTP server by raising an interrupt.
        """
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

        # Raise a KeyboardInterrupt to close the HTTP server since we don't need it anymore
        raise KeyboardInterrupt


def run(
    client_id_: str, client_secret_: str, server_class=HTTPServer, handler_class=S, port=8080
) -> str:
    """
    Generates a Mapillary OAuth url and prints to screen as well as opens it automatically in a browser. Declares some
    global variables to pull data from the HTTP server through the GET endpoint.
    """
    # These global variables are defined so that we can pass data to / get data from the GET endpoint
    global client_id
    global client_secret
    global access_token

    client_id = client_id_
    client_secret = client_secret_

    server_address = ("localhost", port)
    httpd = server_class(server_address, handler_class)
    logging.info("Starting httpd and opening Mapillary to authenticate...")
    try:
        # Print the OAuth link to console and also tries to open it directly in the browser
        auth_url = AUTH_URL.format(client_id)
        logging.info(
            "Please authenticate (if browser didn't automatically open): {}".format(auth_url)
        )
        webbrowser.open_new_tab(auth_url)
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info("Stopping httpd...")
    return access_token
