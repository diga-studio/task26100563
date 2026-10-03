"""Deterministic crash-injection points for crash-recovery tests."""

from __future__ import annotations

import os

#: Known injection points, in execution order inside ``advance``:
#: ``after_effect_commit``   - judgment effects committed, progress not yet saved
#: ``after_progress_commit`` - progress committed for one workspace
#: ``before_done_commit``    - about to commit the run-completion marker
#: ``after_done_commit``     - run-completion marker committed
POINTS = (
    "after_effect_commit",
    "after_progress_commit",
    "before_done_commit",
    "after_done_commit",
)

_ENV_VAR = "GROUPPILOT_CRASH_AT"


def check(point: str) -> None:
    """Kill the process (os._exit) when ``point`` is listed in the env var.

    ``GROUPPILOT_CRASH_AT`` holds a comma-separated list of point names,
    optionally with ``:N`` to trigger only on the Nth hit of that point.
    Used by tests to simulate a hard crash (no exception, no cleanup).
    """
    spec = os.environ.get(_ENV_VAR)
    if not spec:
        return
    for token in spec.split(","):
        name, _, count = token.partition(":")
        if name != point:
            continue
        if count:
            seen = getattr(check, "_hits", {}).get(point, 0) + 1
            check._hits = {**getattr(check, "_hits", {}), point: seen}
            if seen != int(count):
                continue
        os._exit(1)
