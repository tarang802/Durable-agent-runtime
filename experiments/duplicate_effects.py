"""Duplicate provider calls per failpoint.

Runs the crash-matrix tests (a worker killed at each failpoint, a replacement finishes)
and collects the duplicate calls each one reports.
"""

import re
import subprocess
import sys

from experiments.common import table

LINE = re.compile(r"crash (\w+):(\w+): duplicate provider calls = (\d+)")


def main() -> str:
    out = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-s", "tests/test_crash.py", "-k", "crash_matrix"],
        capture_output=True, text=True,
    )
    rows = [[kind, f"`{point}`", n] for kind, point, n in LINE.findall(out.stdout)]
    if out.returncode != 0 or not rows:
        raise SystemExit(f"crash matrix failed:\n{out.stdout[-3000:]}")
    return table("Duplicate provider calls when a worker is killed", ["Worker", "Killed at", "Duplicate calls"], rows)


if __name__ == "__main__":
    print(main())
