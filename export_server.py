"""
export_server.py — HTTP export server for CSV file downloads.

Serves log.csv, resolve_log.csv, and paper_trades.csv as file downloads.
Railway exposes the PORT env var; locally defaults to 8080.

Usage from browser or curl:
  curl http://localhost:8080/export -o log.csv
  curl http://localhost:8080/resolve -o resolve_log.csv
  curl http://localhost:8080/paper -o paper_trades.csv
"""

import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from config import LOG_PATH, RESOLVE_LOG, log
from paper_trading import PAPER_TRADE_LOG


# ============================================================
# SECTION 10 — HTTP EXPORT SERVER
# ============================================================

class ExportHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — serves log.csv and resolve_log.csv as downloads."""

    def do_GET(self):
        # Route the request to the correct file based on the path
        if self.path in ("/", "/export"):
            file_path = LOG_PATH
            filename = "log.csv"
        elif self.path == "/resolve":
            file_path = RESOLVE_LOG
            filename = "resolve_log.csv"
        elif self.path == "/paper":
            file_path = PAPER_TRADE_LOG
            filename = "paper_trades.csv"
        else:
            self.send_response(404)
            self.end_headers()
            return

        if not os.path.isfile(file_path):
            # No data yet — return an empty 200 so callers don't crash
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            return

        try:
            with open(file_path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            log.error(f"ExportHandler error: {exc}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, fmt, *args):
        # Silence the default per-request stdout noise; our logger handles it
        log.debug("HTTP %s", fmt % args)


def run_http_server():
    """Start the export HTTP server on PORT (default 8080). Blocks forever."""
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), ExportHandler)
    log.info(f"Export server listening on port {port}  (GET / or /export → log.csv)")
    server.serve_forever()
