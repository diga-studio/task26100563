import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from grouppilot import GroupPilot

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRASH_CHILD = os.path.join(REPO_ROOT, "tests", "crash_child.py")


class GroupPilotTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="gp_test_")
        self.db = os.path.join(self.dir, "test.db")
        self.g = GroupPilot(self.db)

    def tearDown(self):
        self.g.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def actions_of(self, info, workspace_id):
        return [a for a in info.actions if a["workspace_id"] == workspace_id]


class FreezeSemanticsTest(GroupPilotTestCase):
    def test_frozen_view_and_incremental_window(self):
        # seed: a violating, b compliant, c compliant (Confidential)
        self.g.upsert("a", 30.0, "Public")
        self.g.upsert("b", 10.0, "Public")
        self.g.upsert("c", 30.0, "Confidential")
        run1 = self.g.start("run1")

        # process only "a" so far
        info = self.g.advance("run1", 1)
        self.assertEqual(info.cursor_id, "a")
        self.assertEqual(info.judged_count, 1)

        # writes after start: processed id, unprocessed id, and a new id
        # that sorts before the current cursor
        self.g.upsert("a", 1.0, "Confidential")   # now compliant
        self.g.upsert("b", 99.0, "Public")        # now violating
        self.g.upsert("0", 99.0, "Public")        # sorts before cursor "a"

        # run1 must still use the frozen values
        info = self.g.advance("run1", 100)
        self.assertEqual(info.status, "done")
        self.assertEqual(info.judged_count, 3)
        a_acts = self.actions_of(info, "a")
        self.assertEqual(len(a_acts), 1)
        self.assertEqual(a_acts[0]["status"], "open")  # judged on frozen 30/Public
        self.assertEqual(self.actions_of(info, "b"), [])  # frozen 10/Public
        self.assertEqual(self.actions_of(info, "0"), [])  # not in run1's window

        # run2 covers exactly the three post-run1 writes, with latest values
        run2 = self.g.start("run2")
        self.assertEqual(run2.prev_start_seq, run1.start_seq)
        info = self.g.advance("run2", 100)
        self.assertEqual(info.status, "done")
        self.assertEqual(info.judged_count, 3)
        a_acts = self.actions_of(info, "a")
        self.assertEqual(len(a_acts), 1)
        self.assertEqual(a_acts[0]["status"], "closed")  # now compliant
        self.assertEqual(a_acts[0]["closed_run_id"], "run2")
        b_acts = self.actions_of(info, "b")
        self.assertEqual(len(b_acts), 1)
        self.assertEqual(b_acts[0]["status"], "open")    # now violating
        zero_acts = self.actions_of(info, "0")
        self.assertEqual(len(zero_acts), 1)
        self.assertEqual(zero_acts[0]["status"], "open")

    def test_multiple_writes_in_window_merge_to_latest(self):
        self.g.upsert("a", 30.0, "Public")
        self.g.start("run1")
        self.g.advance("run1", 100)
        # several writes to the same id inside run2's window
        self.g.upsert("a", 5.0, "Confidential")
        self.g.upsert("a", 60.0, "Public")
        self.g.upsert("a", 7.0, "Confidential")
        self.g.start("run2")
        info = self.g.advance("run2", 100)
        # merged: judged exactly once, on the latest value (compliant)
        self.assertEqual(info.judged_count, 1)
        a_acts = self.actions_of(info, "a")
        self.assertEqual(len(a_acts), 1)
        self.assertEqual(a_acts[0]["status"], "closed")
        self.assertEqual(a_acts[0]["closed_run_id"], "run2")

    def test_empty_window_judges_nothing(self):
        self.g.upsert("a", 30.0, "Public")
        self.g.start("run1")
        self.g.advance("run1", 100)
        self.g.start("run2")
        info = self.g.advance("run2", 100)
        self.assertEqual(info.status, "done")
        self.assertEqual(info.judged_count, 0)
        self.assertEqual(len(info.actions), 1)  # run1's action untouched

    def test_repeat_start_returns_same_run_even_when_done(self):
        self.g.upsert("a", 30.0, "Public")
        first = self.g.start("run1")
        self.g.upsert("b", 30.0, "Public")  # must not enter run1's snapshot
        again = self.g.start("run1")
        self.assertEqual(again.start_seq, first.start_seq)
        info = self.g.advance("run1", 100)
        self.assertEqual(info.judged_count, 1)
        done = self.g.start("run1")
        self.assertEqual(done.status, "done")
        self.assertEqual(done.judged_count, 1)

    def test_only_one_unfinished_run(self):
        self.g.upsert("a", 30.0, "Public")
        self.g.start("run1")
        with self.assertRaises(RuntimeError):
            self.g.start("run2")
        self.g.advance("run1", 100)
        self.g.start("run2")  # allowed once run1 is done

    def test_advance_after_done_is_noop(self):
        self.g.upsert("a", 30.0, "Public")
        self.g.start("run1")
        self.g.advance("run1", 100)
        before = self.g.inspect("run1")
        after = self.g.advance("run1", 100)
        self.assertEqual(before, after)

    def test_inspect_does_not_judge(self):
        self.g.upsert("a", 30.0, "Public")
        self.g.start("run1")
        info = self.g.inspect("run1")
        self.assertEqual(info.judged_count, 0)
        self.assertEqual(info.actions, [])
        info = self.g.inspect("run1")
        self.assertEqual(info.judged_count, 0)

    def test_unknown_run_raises(self):
        with self.assertRaises(KeyError):
            self.g.advance("nope", 1)
        with self.assertRaises(KeyError):
            self.g.inspect("nope")


def full_history(db_path):
    conn = sqlite3.connect(db_path)
    try:
        actions = conn.execute(
            "SELECT workspace_id, generation, status, opened_run_id, closed_run_id"
            " FROM actions ORDER BY action_id"
        ).fetchall()
        runs = conn.execute(
            "SELECT run_id, status, prev_start_seq, start_seq, cursor_id,"
            " judged_count FROM runs ORDER BY start_seq"
        ).fetchall()
        conclusions = conn.execute(
            "SELECT workspace_id, violating, run_id FROM conclusions"
            " ORDER BY workspace_id"
        ).fetchall()
        judged_total = conn.execute(
            "SELECT value FROM meta WHERE key = 'judged_total'"
        ).fetchone()[0]
    finally:
        conn.close()
    return {
        "actions": actions,
        "runs": runs,
        "conclusions": conclusions,
        "judged_total": judged_total,
    }


class CrashRecoveryTest(GroupPilotTestCase):
    def seed(self, g):
        # mixed data: violations, compliances, and a Confidential override
        g.upsert("ws01", 30.0, "Public")        # violating
        g.upsert("ws02", 10.0, "Public")        # compliant
        g.upsert("ws03", 40.0, "Internal")      # violating
        g.upsert("ws04", 50.0, "Confidential")  # compliant
        g.upsert("ws05", 60.0, "Public")        # violating

    def run_crashed_child(self, db_path, crash_at):
        env = dict(os.environ)
        env["GROUPPILOT_CRASH_AT"] = crash_at
        env["PYTHONPATH"] = REPO_ROOT
        proc = subprocess.run(
            [sys.executable, CRASH_CHILD, db_path, "run1", "100"],
            env=env,
            cwd=REPO_ROOT,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0, "child must die at the fault point")

    def check_crash_equiv(self, crash_at):
        # reference: same seed and run boundary, never interrupted
        ref_db = os.path.join(self.dir, "ref.db")
        ref = GroupPilot(ref_db)
        self.seed(ref)
        ref.start("run1")
        ref.advance("run1", 100)
        ref.close()

        # crashed: child process is killed at the injection point
        crash_db = os.path.join(self.dir, "crash.db")
        seeder = GroupPilot(crash_db)
        self.seed(seeder)
        seeder.close()
        self.run_crashed_child(crash_db, crash_at)

        # reopen the same database, repeat Start, continue the original run
        resumed = GroupPilot(crash_db)
        info = resumed.start("run1")
        # after_done_commit kills the child after completion was committed,
        # so the repeated Start may legitimately return a finished run
        self.assertIn(info.status, ("running", "done"))
        info = resumed.advance("run1", 100)
        self.assertEqual(info.status, "done")
        resumed.close()

        self.assertEqual(full_history(ref_db), full_history(crash_db))

    def test_crash_after_effect_commit_before_progress(self):
        # action/effects committed, progress not yet saved (2nd workspace)
        self.check_crash_equiv("after_effect_commit:2")

    def test_crash_after_progress_commit(self):
        self.check_crash_equiv("after_progress_commit:3")

    def test_crash_before_final_commit(self):
        self.check_crash_equiv("before_done_commit")

    def test_crash_after_final_commit(self):
        self.check_crash_equiv("after_done_commit")

    def test_crash_then_crash_again(self):
        # two consecutive hard kills must still converge to the reference
        crash_db = os.path.join(self.dir, "crash2.db")
        seeder = GroupPilot(crash_db)
        self.seed(seeder)
        seeder.close()
        self.run_crashed_child(crash_db, "after_effect_commit:1")
        self.run_crashed_child(crash_db, "after_effect_commit:2")
        resumed = GroupPilot(crash_db)
        resumed.start("run1")
        info = resumed.advance("run1", 100)
        self.assertEqual(info.status, "done")
        self.assertEqual(info.judged_count, 5)
        open_ws = sorted(a["workspace_id"] for a in info.actions if a["status"] == "open")
        self.assertEqual(open_ws, ["ws01", "ws03", "ws05"])
        resumed.close()


class ActionGenerationTest(GroupPilotTestCase):
    def test_violation_generations_across_runs(self):
        self.g.upsert("ws", 30.0, "Public")  # violating
        self.g.start("run1")
        info = self.g.advance("run1", 100)
        acts = self.actions_of(info, "ws")
        self.assertEqual(len(acts), 1)
        self.assertEqual((acts[0]["generation"], acts[0]["status"]), (1, "open"))

        # still violating: reuse the open action, no new generation
        self.g.upsert("ws", 40.0, "Public")
        self.g.start("run2")
        info = self.g.advance("run2", 100)
        acts = self.actions_of(info, "ws")
        self.assertEqual(len(acts), 1)
        self.assertEqual((acts[0]["generation"], acts[0]["status"]), (1, "open"))

        # compliant: the open action is closed
        self.g.upsert("ws", 10.0, "Public")
        self.g.start("run3")
        info = self.g.advance("run3", 100)
        acts = self.actions_of(info, "ws")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0]["status"], "closed")
        self.assertEqual(acts[0]["closed_run_id"], "run3")

        # violating again: a new generation opens, old history preserved
        self.g.upsert("ws", 50.0, "Internal")
        self.g.start("run4")
        info = self.g.advance("run4", 100)
        acts = self.actions_of(info, "ws")
        self.assertEqual(len(acts), 2)
        self.assertEqual(
            [(a["generation"], a["status"], a["opened_run_id"]) for a in acts],
            [(1, "closed", "run1"), (2, "open", "run4")],
        )

    def test_confidential_is_never_violating(self):
        self.g.upsert("ws", 1000.0, "Confidential")
        self.g.start("run1")
        info = self.g.advance("run1", 100)
        self.assertEqual(info.actions, [])


class IncrementalScaleTest(GroupPilotTestCase):
    def seed_100k_with_completed_run1(self):
        """Bulk-load 100k workspaces plus a completed run1 (test setup only)."""
        n = 100_000
        conn = sqlite3.connect(self.db)
        with conn:
            conn.executemany(
                "INSERT INTO registry_log(seq, workspace_id, storage_gb,"
                " classification) VALUES(?, ?, 1.0, 'Confidential')",
                ((i + 1, f"ws{i:06d}") for i in range(n)),
            )
            conn.executemany(
                "INSERT INTO registry(workspace_id, storage_gb, classification, seq)"
                " VALUES(?, 1.0, 'Confidential', ?)",
                ((f"ws{i:06d}", i + 1) for i in range(n)),
            )
            conn.executemany(
                "INSERT INTO conclusions(workspace_id, violating, run_id)"
                " VALUES(?, 0, 'run1')",
                ((f"ws{i:06d}",) for i in range(n)),
            )
            conn.execute(
                "INSERT INTO runs(run_id, prev_start_seq, start_seq, status,"
                " cursor_id, judged_count)"
                " VALUES('run1', 0, ?, 'done', ?, ?)",
                (n, f"ws{n - 1:06d}", n),
            )
            conn.execute("INSERT INTO meta(key, value) VALUES('seq', ?)", (n,))
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('judged_total', ?)", (n,)
            )
        conn.close()

    def test_incremental_run_judges_only_changed_ids(self):
        self.g.close()
        self.seed_100k_with_completed_run1()
        self.g = GroupPilot(self.db)

        before = self.g.inspect("run1").total_judgments
        self.assertEqual(before, 100_000)

        # 10 writes inside run2's window: 7 existing ids, 3 brand-new ids
        self.g.upsert("ws000003", 30.0, "Public")        # -> violating
        self.g.upsert("ws000007", 40.0, "Confidential")  # -> compliant
        self.g.upsert("ws010000", 50.0, "Internal")      # -> violating
        self.g.upsert("ws050000", 2.0, "Public")         # -> compliant
        self.g.upsert("ws099999", 60.0, "Public")        # -> violating
        self.g.upsert("ws000003", 5.0, "Confidential")   # merge: -> compliant
        self.g.upsert("new-a", 70.0, "Public")           # -> violating
        self.g.upsert("new-b", 1.0, "Public")            # -> compliant
        self.g.upsert("new-c", 80.0, "Confidential")     # -> compliant
        self.g.upsert("new-a", 90.0, "Public")           # merge: -> violating

        run2 = self.g.start("run2")
        self.assertEqual(run2.prev_start_seq, 100_000)
        info = self.g.advance("run2", 1000)
        self.assertEqual(info.status, "done")

        # verifiable judgment count: exactly the 8 distinct changed ids
        self.assertEqual(info.judged_count, 8)
        self.assertEqual(info.total_judgments - before, 8)

        # merged writes judged once, on the latest value
        self.assertEqual(self.actions_of(info, "ws000003"), [])  # merged compliant
        new_a = self.actions_of(info, "new-a")
        self.assertEqual(len(new_a), 1)
        self.assertEqual(new_a[0]["status"], "open")
        open_ws = sorted(a["workspace_id"] for a in info.actions)
        self.assertEqual(open_ws, ["new-a", "ws010000", "ws099999"])

        # untouched workspaces keep their old conclusion and produce no actions
        conn = sqlite3.connect(self.db)
        untouched = conn.execute(
            "SELECT violating, run_id FROM conclusions WHERE workspace_id = 'ws000000'"
        ).fetchone()
        changed = conn.execute(
            "SELECT COUNT(*) FROM conclusions WHERE run_id = 'run2'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(untouched, (0, "run1"))
        self.assertEqual(changed, 8)


if __name__ == "__main__":
    unittest.main()
