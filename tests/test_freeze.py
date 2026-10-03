"""启动后写入的冻结语义与下一任务的增量覆盖。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grouppilot import GovernanceExecutor


def drain(ex, run_id, budget=1000):
    while True:
        res = ex.advance(run_id, budget)
        if res.done:
            return


def test_frozen_view_and_incremental_coverage(tmp_path):
    ex = GovernanceExecutor(str(tmp_path / "g.db"), batch_size=1)
    ex.upsert("ws_a", 30, "Public")   # 违规
    ex.upsert("ws_b", 10, "Public")   # 合规
    ex.upsert("ws_c", 30, "Public")   # 违规
    run1 = ex.start("run1")
    assert run1.boundary == 3

    first = ex.advance("run1", 1)     # 只处理排序最前的 ws_a
    assert first.judged == 1 and not first.done

    # 启动后写入：改已处理的 ws_a、改未处理的 ws_c、
    # 新增排序位置早于当前进度游标的 ws_0。
    ex.upsert("ws_a", 10, "Public")   # 变为合规
    ex.upsert("ws_c", 10, "Public")   # 变为合规
    ex.upsert("ws_0", 99, "Public")   # 新 ID，违规，排序早于游标

    drain(ex, "run1")
    info1 = ex.inspect("run1")
    assert info1["status"] == "done"
    # 当前任务始终使用冻结值：ws_a/ws_c 仍按启动时的 30GB 判违规；
    # ws_0 在边界之后写入，本轮完全不出现。
    assert info1["results"] == [
        {"workspace_id": "ws_a", "violating": True},
        {"workspace_id": "ws_b", "violating": False},
        {"workspace_id": "ws_c", "violating": True},
    ]

    ex.eval_count = 0
    run2 = ex.start("run2")
    assert run2.prev_boundary == 3 and run2.boundary == 6
    drain(ex, "run2")
    info2 = ex.inspect("run2")
    # 下一任务恰好覆盖这三处写入，且取合并后的最新值。
    assert info2["results"] == [
        {"workspace_id": "ws_0", "violating": True},
        {"workspace_id": "ws_a", "violating": False},
        {"workspace_id": "ws_c", "violating": False},
    ]
    assert ex.eval_count == 3

    actions = {a["workspace_id"]: a for a in info2["actions"]}
    assert actions["ws_a"]["open"] is False and actions["ws_a"]["closed_run"] == "run2"
    assert actions["ws_c"]["open"] is False and actions["ws_c"]["closed_run"] == "run2"
    assert actions["ws_0"]["open"] is True and actions["ws_0"]["opened_run"] == "run2"
    assert "ws_b" not in actions
    ex.close()


if __name__ == "__main__":
    from pathlib import Path
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        test_frozen_view_and_incremental_coverage(Path(d))
    print("test_freeze ok")
