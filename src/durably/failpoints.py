"""Failpoints: make a worker die or freeze at a named point, for crash tests.

`FAILPOINT=<name>` exits the process abruptly (no cleanup, no flush) the first time it
reaches `<name>`; `FAILPOINT=<name>@<n>` waits for the n-th time. Adding `:pause`
(`<name>:pause`, `<name>@<n>:pause`) stops it with SIGSTOP instead, so a test can let its
lease expire and resume it later with SIGCONT (POSIX only).

Points: `before_claim`, `after_claim`, `before_activity`, `after_effect_before_record`,
`before_append`, `after_record_before_ack`, `during_timer_fire`.
"""

import os
import signal
from collections import Counter

EXIT_CODE = 86

_hits: Counter[str] = Counter()


def parse(spec: str) -> tuple[str, int, str]:
    """`name[@n][:action]` -> (name, n, action)."""
    target, _, action = spec.partition(":")
    name, _, nth = target.partition("@")
    return name, int(nth or 1), action or "exit"


def failpoint(name: str) -> None:
    spec = os.environ.get("FAILPOINT")
    if not spec:
        return
    target, nth, action = parse(spec)
    if target != name:
        return
    _hits[name] += 1
    if _hits[name] != nth:  # fire once, so a resumed worker carries on normally
        return
    os.write(2, f"durably: failpoint {name}@{nth} ({action}) pid {os.getpid()}\n".encode())
    if action == "pause":
        os.kill(os.getpid(), signal.SIGSTOP)
    else:
        os._exit(EXIT_CODE)
