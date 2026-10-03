"""10 万工作区中仅 10 个增量写入：本轮只能对这 10 个 ID 执行策略判定。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grouppilot import GovernanceExecutor

TOTAL = 100_000
CHANGED = 10


def seed_registry(ex, total):
    """快速播种注册表（等价于 total 次 Upsert 的结果）。"""
    with ex._conn:
        ex._conn.executemany(
            "INSERT INTO versions (workspace_id, seq, storage_gb, classification)"
            " VALUES (?, ?, ?, ?)",
            (
                (f"ws-{i:06d}", i + 1, 10.0 if i % 2 else 30.0, "Public")
                for i in range(total)
            ),
        )
        ex._meta_set("seq", total)


def drain(ex, run_id, budget):
    while not ex.advance(run_id, budget).done:
        pass


def test_incremental_run_only_judges_changed_ids(tmp_path):
    ex = GovernanceExecutor(str(tmp_path / "g.db"), batch_size=1000)
    seed_registry(ex, TOTAL)

    ex.eval_count = 0
    ex.start("run1")
    drain(ex, "run1", budget=TOTAL)
    assert ex.eval_count == TOTAL  # 首轮判定启动时的全部工作区

    changed_ids = [f"ws-{i:06d}" for i in range(0, TOTAL, TOTAL // CHANGED)]
    for workspace_id in changed_ids:
        ex.upsert(workspace_id, 99, "Public")  # 全部改为违规

    ex.eval_count = 0
    run2 = ex.start("run2")
    assert run2.prev_boundary == TOTAL and run2.boundary == TOTAL + CHANGED
    drain(ex, "run2", budget=TOTAL)

    # 判定计数恰好为 10，且结果就是这 10 个 ID（不是只核对违规数量）。
    assert ex.eval_count == CHANGED
    info2 = ex.inspect("run2")
    assert info2["processed"] == CHANGED
    assert info2["results"] == [
        {"workspace_id": wid, "violating": True} for wid in sorted(changed_ids)
    ]
    ex.close()


if __name__ == "__main__":
    import tempfile
    import time
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        start = time.time()
        test_incremental_run_only_judges_changed_ids(Path(d))
    print(f"test_incremental_scale ok ({time.time() - start:.1f}s)")
