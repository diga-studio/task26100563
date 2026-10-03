"""强制结束进程后的崩溃一致性：与从未中断的参考执行完全对齐。"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grouppilot import GovernanceExecutor
from tests.crash_child import SEED, seed

CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_child.py")


def drain(ex, run_id, budget=1000):
    while not ex.advance(run_id, budget).done:
        pass


def run_reference(db_path):
    ex = GovernanceExecutor(str(db_path), batch_size=1)
    seed(ex)
    ex.start("run1")
    drain(ex, "run1")
    snap = ex.inspect("run1")
    ex.close()
    return snap


def run_crashed(db_path, crash_point, crash_n, budget):
    env = dict(os.environ)
    env["GROUPPILOT_CRASH_POINT"] = crash_point
    env["GROUPPILOT_CRASH_N"] = str(crash_n)
    proc = subprocess.run(
        [sys.executable, CHILD, str(db_path), str(budget)],
        env=env,
        capture_output=True,
    )
    assert proc.returncode == 3, (proc.returncode, proc.stderr.decode())
    # 重开同一数据库：重复 Start 必须返回原任务（不重新取快照）。
    ex = GovernanceExecutor(str(db_path), batch_size=1)
    ex.upsert("w99", 99, "Public")  # 崩溃恢复期间的新写入，只能进下一任务
    run = ex.start("run1")
    assert run.boundary == len(SEED)  # 仍是首次启动时的边界
    drain(ex, "run1")
    snap = ex.inspect("run1")
    ex.close()
    return snap


def assert_same_run(snap_a, snap_b):
    assert snap_a["status"] == snap_b["status"] == "done"
    assert snap_a["processed"] == snap_b["processed"]
    assert snap_a["results"] == snap_b["results"]
    assert snap_a["actions"] == snap_b["actions"]


def test_crash_after_effects_before_progress(tmp_path):
    ref = run_reference(tmp_path / "ref.db")
    got = run_crashed(tmp_path / "crash.db", "after_effects_commit", 2, budget=6)
    assert_same_run(ref, got)


def test_crash_before_final_batch_commit(tmp_path):
    ref = run_reference(tmp_path / "ref.db")
    got = run_crashed(tmp_path / "crash.db", "before_effects_commit", 6, budget=6)
    assert_same_run(ref, got)


def test_crash_after_final_progress_commit(tmp_path):
    ref = run_reference(tmp_path / "ref.db")
    got = run_crashed(tmp_path / "crash.db", "after_progress_commit", 6, budget=6)
    assert_same_run(ref, got)


def test_crash_mid_run_then_next_run_picks_up_late_write(tmp_path):
    db_path = tmp_path / "crash.db"
    run_crashed(db_path, "after_effects_commit", 3, budget=6)
    ex = GovernanceExecutor(str(db_path), batch_size=1)
    ex.eval_count = 0
    ex.start("run2")
    drain(ex, "run2")
    info = ex.inspect("run2")
    assert info["results"] == [{"workspace_id": "w99", "violating": True}]
    assert ex.eval_count == 1
    ex.close()


if __name__ == "__main__":
    import tempfile
    from pathlib import Path
    for fn in (
        test_crash_after_effects_before_progress,
        test_crash_before_final_batch_commit,
        test_crash_after_final_progress_commit,
        test_crash_mid_run_then_next_run_picks_up_late_write,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(fn.__name__, "ok")
