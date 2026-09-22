#!/usr/bin/env python3
"""runs a fixed set of probes against an http intermediary and prints a table.

--through is the url that reaches probe/server.py through the chain.
--direct is the same server without the chain, for side by side numbers.
"""

import argparse
import http.client
import json
import os
import random
import select
import socket
import ssl
import time
from urllib.parse import urlparse

TIMEOUT = 30


def open_conn(url, timeout=TIMEOUT):
    p = urlparse(url)
    host = p.hostname
    port = p.port or (443 if p.scheme == "https" else 80)
    if p.scheme == "https":
        return http.client.HTTPSConnection(host, port, timeout=timeout,
                                           context=ssl.create_default_context())
    return http.client.HTTPConnection(host, port, timeout=timeout)


def send(url, method="GET", suffix="", body=None, headers=None, timeout=TIMEOUT):
    p = urlparse(url)
    path = (p.path or "/") + suffix
    c = open_conn(url, timeout)
    t0 = time.time()
    try:
        c.request(method, path, body=body, headers=headers or {})
        r = c.getresponse()
        return r.status, dict(r.getheaders()), r.read(), time.time() - t0
    finally:
        c.close()


def report_of(payload):
    try:
        return json.loads(payload.decode("utf-8", "replace"))
    except (ValueError, AttributeError):
        return None


def probe_methods(through, **_):
    ok, no = [], []
    for m in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
        try:
            st, _, _, _ = send(through, method=m, suffix="/echo")
        except Exception:
            no.append(f"{m}(err)")
            continue
        (ok if st == 200 else no).append(m if st == 200 else f"{m}({st})")
    return {
        "result": ", ".join(ok) or "none",
        "detail": ("blocked: " + ", ".join(no)) if no else "all methods pass",
    }


def probe_get_body(through, direct=None, **_):
    payload = os.urandom(4096)
    h = {"Content-Length": str(len(payload)), "Content-Type": "application/octet-stream"}

    def measure(url):
        _, _, body, _ = send(url, method="GET", suffix="/echo", body=payload, headers=h)
        r = report_of(body)
        return r["body"]["received_bytes"] if r else None

    via = measure(through)
    dir_ = measure(direct) if direct else None

    if via == 4096:
        res, det = "preserved", "GET bodies pass intact"
    elif via == 0:
        res, det = "stripped", "intermediary discards GET bodies"
    else:
        res, det = f"partial ({via})", "unexpected truncation"
    if dir_ is not None:
        det += f" / direct={dir_}"
    return {"result": res, "detail": det}


def probe_request_buffering(through, **_):
    def slow():
        for _ in range(10):
            yield b"x" * 1024
            time.sleep(0.5)

    p = urlparse(through)
    c = open_conn(through)
    try:
        c.request("POST", (p.path or "/") + "/echo", body=slow(),
                  headers={"Content-Type": "application/octet-stream"})
        r = report_of(c.getresponse().read())
    except Exception as e:
        return {"result": "error", "detail": str(e)[:60]}
    finally:
        c.close()

    if not r:
        return {"result": "unknown", "detail": "no report returned"}
    d = r["body"]["first_byte_delay_s"]
    if d is None:
        return {"result": "no body", "detail": "body did not arrive"}
    if d < 1.0:
        return {"result": "streamed", "detail": f"first byte after {d}s, stream-up viable"}
    return {"result": "buffered", "detail": f"first byte after {d}s, use packet-up"}


def stream_arrivals(url, seconds, want, deadline):
    # http.client waits for the whole response, so every line would be
    # timestamped at once. talk to the socket directly instead.
    p = urlparse(url)
    host = p.hostname
    port = p.port or (443 if p.scheme == "https" else 80)
    nonce = random.randint(10**9, 10**10)
    path = (p.path or "/") + f"/stream?seconds={seconds}&nonce={nonce}"

    s = socket.create_connection((host, port), timeout=15)
    if p.scheme == "https":
        s = ssl.create_default_context().wrap_socket(s, server_hostname=host)

    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "User-Agent: cdn-probe\r\n"
        "Accept: */*\r\n"
        "Connection: close\r\n\r\n"
    ).encode()

    arrivals = []
    t0 = time.time()
    status = None
    try:
        s.sendall(req)
        buf = b""
        head_done = False
        while len(arrivals) < want and time.time() - t0 < deadline:
            ready, _, _ = select.select([s], [], [], 1.0)
            if not ready:
                continue
            try:
                blk = s.recv(4096)
            except (ssl.SSLWantReadError, BlockingIOError):
                continue
            if not blk:
                break
            now = time.time() - t0
            buf += blk
            if not head_done:
                if b"\r\n\r\n" not in buf:
                    continue
                head, buf = buf.split(b"\r\n\r\n", 1)
                status = head.split(b"\r\n")[0].decode("latin-1")
                head_done = True
            while b"\n" in buf and len(arrivals) < want:
                line, buf = buf.split(b"\n", 1)
                if b" " in line.strip():
                    arrivals.append(now)
    except Exception as e:
        return arrivals, str(e)[:60]
    finally:
        try:
            s.close()
        except OSError:
            pass

    if status and " 200" not in status:
        return None, status
    return arrivals, None


def probe_response_streaming(through, **_):
    arrivals, err = stream_arrivals(through, seconds=8, want=4, deadline=20)
    if arrivals is None:
        return {"result": "error", "detail": err}
    if len(arrivals) < 2:
        return {"result": "no stream", "detail": err or "fewer than two lines received"}
    spread = arrivals[-1] - arrivals[0]
    if spread > 0.5:
        return {"result": "streamed", "detail": f"{len(arrivals)} lines over {spread:.1f}s"}
    return {"result": "buffered", "detail": f"{len(arrivals)} lines within {spread:.2f}s"}


def probe_idle_timeout(through, **_):
    arrivals, err = stream_arrivals(through, seconds=300, want=10**6, deadline=300)
    if not arrivals:
        return {"result": "error", "detail": err or "no data received"}
    last = arrivals[-1]
    if last >= 295:
        return {"result": ">300s", "detail": "no cut within the test window"}
    return {"result": f"~{int(last)}s", "detail": f"stream ended after {len(arrivals)} lines"}


def probe_body_limit(through, **_):
    largest, rejected = 0, None
    for mb in (1, 8, 32, 64):
        payload = b"\0" * (mb * 1024 * 1024)
        try:
            st, _, _, _ = send(through, method="POST", suffix="/echo", body=payload,
                               headers={"Content-Length": str(len(payload))}, timeout=90)
        except Exception:
            rejected = mb
            break
        if st == 200:
            largest = mb
        else:
            rejected = f"{mb}MB -> {st}"
            break
    return {
        "result": f">={largest} MB",
        "detail": f"rejected at {rejected}" if rejected else "no limit found in tested range",
    }


def probe_path_handling(through, **_):
    cases = ["/echo/seg.ts", "/echo/seg.ts/", "/echo//seg.ts", "/echo/seg.ts/?a=1&b=2"]
    changed = []
    for sent in cases:
        try:
            _, _, body, _ = send(through, suffix=sent)
        except Exception:
            continue
        r = report_of(body)
        if not r:
            continue
        arrived = r["request"]["path"]
        base = urlparse(through).path or ""
        got = arrived[len(base):] if arrived.startswith(base) else arrived
        if got != sent:
            changed.append(f"{sent} -> {got}")
    if not changed:
        return {"result": "preserved", "detail": "no rewrite needed at the origin"}
    return {"result": "rewritten", "detail": "; ".join(changed[:2])}


def probe_caching(through, **_):
    suffix = f"/echo?nonce={random.randint(10**6, 10**7)}"
    try:
        _, h1, first, _ = send(through, suffix=suffix)
        time.sleep(2)
        _, h2, second, _ = send(through, suffix=suffix)
    except Exception as e:
        return {"result": "error", "detail": str(e)[:60]}

    a, b = report_of(first), report_of(second)
    if not a or not b:
        return {"result": "unknown", "detail": "no report returned"}

    def marker(hs):
        for key in ("age", "cache", "x-cache", "x-cached-since"):
            for name, val in hs.items():
                if name.lower() == key and val not in ("0", "MISS", "", None):
                    return f"{name}: {val}"
        return None

    age = marker(h2) or marker(h1)
    if a["server_time"] == b["server_time"]:
        return {
            "result": "CACHED",
            "detail": "same response served twice; one origin hit, " + (age or "no cache header"),
        }
    detail = "distinct responses"
    if age:
        detail += " but cache header present: " + age
    return {"result": "not cached", "detail": detail}


PROBES = [
    ("HTTP methods", probe_methods, False),
    ("GET request body", probe_get_body, False),
    ("Request buffering", probe_request_buffering, False),
    ("Response streaming", probe_response_streaming, False),
    ("Max request body", probe_body_limit, False),
    ("Path handling", probe_path_handling, False),
    ("Caching", probe_caching, False),
    ("Idle timeout", probe_idle_timeout, True),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--through", required=True)
    p.add_argument("--direct")
    p.add_argument("--skip-slow", action="store_true")
    a = p.parse_args()

    rows = []
    for name, fn, slow in PROBES:
        if slow and a.skip_slow:
            continue
        print(f"  running: {name} ...", flush=True)
        try:
            out = fn(through=a.through, direct=a.direct)
        except Exception as e:
            out = {"result": "error", "detail": str(e)[:60]}
        rows.append((name, out["result"], out["detail"]))

    w = max(len(r[0]) for r in rows)
    print(f"\n## intermediary characterisation, {a.through}\n")
    print(f"| {'Probe'.ljust(w)} | Result | Notes |")
    print(f"| {'-' * w} | --- | --- |")
    for name, res, det in rows:
        print(f"| {name.ljust(w)} | {res} | {det} |")
    print()


if __name__ == "__main__":
    main()
