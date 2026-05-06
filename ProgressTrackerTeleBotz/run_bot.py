from __future__ import annotations

import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from progress_tracker.bot import run_polling


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args) -> None:
        return


def _start_http_server() -> None:
    port = os.environ.get("PORT")
    if not port:
        return
    try:
        value = int(port)
    except ValueError:
        return

    server = HTTPServer(("0.0.0.0", value), _HealthHandler)
    # Render web services need a listening port; keep a tiny health endpoint.
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


if __name__ == "__main__":
    _start_http_server()
    run_polling()
