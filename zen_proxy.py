#!/usr/bin/env python3
"""Thin reverse proxy: inject reasoning_effort=none into zen chat completions.
jcode points its base_url at http://172.17.45.173:8790/v1 and we forward to
https://opencode.ai/zen/v1, adding reasoning_effort:"none" so deepseek-v4-flash-free
stops emitting reasoning_content (which jcode cannot round-trip -> 400 on 2nd turn).
"""
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

UPSTREAM = "https://opencode.ai/zen"

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def _handle(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n) if n else b""
            req = json.loads(body) if body else {}
            # inject reasoning_effort=none for deepseek models unless already set
            if "reasoning_effort" not in req:
                req["reasoning_effort"] = "none"
            data = json.dumps(req).encode()
            # urllib drops empty-valued headers; zen needs the (empty) Bearer
            # header present to accept the request, so send a single space.
            headers = {"Content-Type": "application/json", "Authorization": "Bearer ", "User-Agent": "curl/8.0"}
            url = UPSTREAM + self.path
            r = urlopen(Request(url, data=data, headers=headers, method="POST"), timeout=300)
            resp = r.read()
            self.send_response(r.status)
            for k, v in r.headers.items():
                if k.lower() in ("content-type", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
        except Exception as e:
            msg = json.dumps({"error": {"message": str(e)}}).encode()
            try:
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            except Exception:
                pass
    do_POST = _handle
    do_GET = _handle

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8790
    print(f"zen-proxy on :{port} -> {UPSTREAM}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
