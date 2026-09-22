#!/usr/bin/env python3
"""origin server that reports what actually arrived.

runs behind whatever you want to measure. the runner sends probes through
the chain and compares what was sent with what came out the other end.
"""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

START = time.time()
READ = 8192


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "cdn-probe/1.0"

    def _param(self, name, default, cap):
        try:
            v = int(parse_qs(urlparse(self.path).query).get(name, [default])[0])
        except (TypeError, ValueError):
            return default
        return max(0, min(v, cap))

    def _read(self):
        declared = self.headers.get("Content-Length")
        chunked = "chunked" in (self.headers.get("Transfer-Encoding") or "").lower()
        started = time.time()
        first = None
        got = 0
        head = b""

        if declared:
            left = int(declared)
            while left > 0:
                part = self.rfile.read(min(READ, left))
                if not part:
                    break
                if first is None:
                    first = time.time() - started
                if len(head) < 120:
                    head += part[: 120 - len(head)]
                got += len(part)
                left -= len(part)
        elif chunked:
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    break
                try:
                    n = int(line.split(b";")[0], 16)
                except ValueError:
                    break
                if n == 0:
                    self.rfile.readline()
                    break
                part = self.rfile.read(n)
                if first is None:
                    first = time.time() - started
                if len(head) < 120:
                    head += part[: 120 - len(head)]
                got += len(part)
                self.rfile.readline()

        return {
            "declared_length": declared,
            "transfer_encoding": self.headers.get("Transfer-Encoding"),
            "received_bytes": got,
            "first_byte_delay_s": round(first, 3) if first else None,
            "full_read_s": round(time.time() - started, 3),
            "preview": head.decode("utf-8", "replace"),
        }

    def _nocache(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("CDN-Cache-Control", "no-store")
        self.send_header("Surrogate-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")

    def _echo(self):
        body = self._read()
        out = json.dumps({
            "server_uptime_s": round(time.time() - START, 3),
            "server_time": time.time(),
            "request": {
                "method": self.command,
                "path": self.path,
                "http_version": self.request_version,
                "client": self.client_address[0],
            },
            "headers": dict(self.headers),
            "body": body,
        }, ensure_ascii=False, indent=2).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(out)))
        self._nocache()
        self.end_headers()
        self.wfile.write(out)

    def _stream(self):
        self._read()
        n = self._param("seconds", 60, 3600)

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self._nocache()
        self.end_headers()

        try:
            for i in range(n):
                line = f"{i} {time.time():.3f}\n".encode()
                self.wfile.write(b"%X\r\n" % len(line) + line + b"\r\n")
                self.wfile.flush()
                time.sleep(1)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _bulk(self):
        self._read()
        n = self._param("bytes", 1024 * 1024, 256 * 1024 * 1024)

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(n))
        self._nocache()
        self.end_headers()

        block = b"\0" * READ
        try:
            left = n
            while left > 0:
                k = min(READ, left)
                self.wfile.write(block[:k])
                left -= k
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _route(self):
        path = urlparse(self.path).path.rstrip("/")
        if path.endswith("/stream"):
            self._stream()
        elif path.endswith("/bulk"):
            self._bulk()
        else:
            self._echo()

    do_GET = _route
    do_HEAD = _route
    do_POST = _route
    do_PUT = _route
    do_PATCH = _route
    do_DELETE = _route
    do_OPTIONS = _route

    def log_message(self, fmt, *args):
        print(f"{time.strftime('%H:%M:%S')} {self.command} {self.path}", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    a = p.parse_args()

    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"listening on {a.host}:{a.port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
