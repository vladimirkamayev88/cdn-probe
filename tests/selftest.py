#!/usr/bin/env python3
"""runs every probe against a local server with no intermediary.

expected answers are known in advance here, so any deviation means the probe
is broken, not that some cdn misbehaved.
"""

import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "probe"))

PORT = 9399
BASE = f"http://127.0.0.1:{PORT}"

EXPECTED = {
    "HTTP methods": "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
    "GET request body": "preserved",
    "Request buffering": "streamed",
    "Response streaming": "streamed",
    "Path handling": "preserved",
    "Caching": "not cached",
}


def wait_for_server(timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            urllib.request.urlopen(f"{BASE}/echo", timeout=1).read()
            return True
        except Exception:
            time.sleep(0.2)
    return False


def main():
    srv = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "probe", "server.py"),
         "--host", "127.0.0.1", "--port", str(PORT)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    try:
        if not wait_for_server():
            print("FAIL: probe server did not start")
            return 1

        import runner

        fails = []
        for name, fn, slow in runner.PROBES:
            if slow or name not in EXPECTED:
                continue
            try:
                out = fn(through=BASE, direct=None)
            except Exception as e:
                fails.append(f"{name}: raised {e}")
                print(f"  FAIL  {name}: raised {e}")
                continue
            got, want = out["result"], EXPECTED[name]
            if got == want:
                print(f"  ok    {name}: {got}")
            else:
                fails.append(f"{name}: expected {want!r}, got {got!r}")
                print(f"  FAIL  {name}: expected {want!r}, got {got!r}")

        # timing probe must show real spread. if every line comes back at the
        # same instant, a working stream would look like buffering.
        arrivals, err = runner.stream_arrivals(BASE, seconds=4, want=4, deadline=15)
        if err or not arrivals or len(arrivals) < 3:
            fails.append(f"stream timing: got {arrivals}, err={err}")
            print(f"  FAIL  stream timing: got {arrivals}, err={err}")
        elif arrivals[-1] - arrivals[0] < 1.0:
            fails.append(f"stream timing: no spread ({arrivals})")
            print("  FAIL  stream timing: lines are not spread over time")
        else:
            spread = arrivals[-1] - arrivals[0]
            print(f"  ok    stream timing: {len(arrivals)} lines over {spread:.1f}s")

        print()
        if fails:
            print(f"{len(fails)} check(s) failed")
            return 1
        print("all checks passed")
        return 0

    finally:
        srv.terminate()
        srv.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
