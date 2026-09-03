import json, os
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(size)
        if os.getenv("FAILURE_MODE", "off") == "on":
            self.send_response(503); self.end_headers(); return
        try: payload = json.loads(body)
        except Exception:
            self.send_response(400); self.end_headers(); return
        self.send_response(200); self.send_header("content-type", "application/json"); self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "event_id": payload.get("event_id")}).encode())
    def log_message(self, fmt, *args): print(json.dumps({"message": fmt % args}), flush=True)

HTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
