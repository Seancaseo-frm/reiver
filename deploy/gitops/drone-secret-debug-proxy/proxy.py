#!/usr/bin/env python3
"""Forward proxy: log secret requests/responses, forward to real extension. Builds keep working."""
import http.server
import socketserver
import urllib.request
import sys

TARGET = "http://drone-kubernetes-secrets:3000"
PORT = 3000


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        # Log request (name/path/repo — no secret value)
        print("=== SECRET REQUEST ===", flush=True)
        for k, v in self.headers.items():
            print(f"  {k}: {v}", flush=True)
        print(f"  Body: {body.decode(errors='replace')}", flush=True)
        # Forward to real extension
        req_headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")}
        req_headers["Host"] = "drone-kubernetes-secrets:3000"
        req = urllib.request.Request(TARGET, data=body, method="POST", headers=req_headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp_body = resp.read()
                # Log response metadata only (do not log secret value)
                print("=== SECRET RESPONSE ===", flush=True)
                print(f"  Status: {resp.status}", flush=True)
                try:
                    import json
                    j = json.loads(resp_body.decode())
                    data = j.get("data", "")
                    print(f"  data present: {bool(data)}, data length: {len(data) if isinstance(data, str) else 'N/A'}", flush=True)
                except Exception:
                    print(f"  body length: {len(resp_body)}", flush=True)
                self.send_response(resp.status)
                # Runner expects this Content-Type (matches Accept); extension may send application/json
                self.send_header("Content-Type", "application/vnd.drone.secret.v1+json")
                for k, v in resp.headers.items():
                    if k.lower() not in ("transfer-encoding", "content-type"):
                        self.send_header(k, v)
                self.end_headers()
                self.wfile.write(resp_body)
        except urllib.error.HTTPError as e:
            resp_body = e.read()
            print("=== SECRET RESPONSE (error) ===", flush=True)
            print(f"  Status: {e.code}", flush=True)
            print(f"  Body: {resp_body.decode(errors='replace')[:500]}", flush=True)
            self.send_response(e.code)
            self.end_headers()
            self.wfile.write(resp_body)
        except Exception as e:
            print("=== FORWARD ERROR ===", flush=True)
            print(f"  {e}", flush=True)
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(f"Proxy forward error: {e}".encode())

    def log_message(self, format, *args):
        pass


with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Drone secret debug proxy listening on {PORT}, forwarding to {TARGET}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    httpd.serve_forever()
