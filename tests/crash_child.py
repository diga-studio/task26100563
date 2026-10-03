"""Child-process entry for crash-injection tests.

Usage: python tests/crash_child.py <db_path> <run_id> <budget>
The parent sets GROUPPILOT_CRASH_AT; the child dies via os._exit at the
injection point, simulating a hard kill (no exception, no cleanup).
"""

import sys

from grouppilot import GroupPilot


def main() -> None:
    db_path, run_id, budget = sys.argv[1], sys.argv[2], int(sys.argv[3])
    g = GroupPilot(db_path)
    g.start(run_id)
    g.advance(run_id, budget)
    g.close()


if __name__ == "__main__":
    main()
