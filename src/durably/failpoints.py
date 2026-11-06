"""Failpoints: make a worker die or freeze at a named point, for crash tests.

`FAILPOINT=<name>` exits the process abruptly (no cleanup, no flush) the first time it
reaches `<name>`. `FAILPOINT=<name>:pause` stops it with SIGSTOP instead, so a test can
let its lease expire and resume it later with SIGCONT (POSIX only).

Points: `before_claim`, `after_claim`, `before_activity`, `after_effect_before_record`,
`before_append`, `after_record_before_ack`.
"""

import os
import signal

EXIT_CODE = 86

_fired: set[str] = set()


def failpoint(name: str) -> None:
    spec = os.environ.get("FAILPOINT")
    if not spec or name in _fired:
        return
    target, _, action = spec.partition(":")
    if target != name:
        return
    _fired.add(name)  # fire once per process, so a resumed worker carries on normally
    os.write(2, f"durably: failpoint {name} ({action or 'exit'}) pid {os.getpid()}\n".encode())
    if action == "pause":
        os.kill(os.getpid(), signal.SIGSTOP)
    else:
        os._exit(EXIT_CODE)
