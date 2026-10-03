"""跨任务的违规世代：仅形成两个动作世代，旧历史保留。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grouppilot import GovernanceExecutor


def drain(ex, run_id, budget=1000):
    while not ex.advance(run_id, budget).done:
        pass


def test_two_generations_across_runs(tmp_path):
    ex = GovernanceExecutor(str(tmp_path / "g.db"), batch_size=1)

    ex.upsert("ws_x", 30, "Public")          # 违规
    ex.start("run1")
    drain(ex, "run1")

    ex.upsert("ws_x", 40, "Internal")        # 持续违规
    ex.start("run2")
    drain(ex, "run2")

    ex.upsert("ws_x", 40, "Confidential")    # 转为合规
    ex.start("run3")
    drain(ex, "run3")

    ex.upsert("ws_x", 60, "Public")          # 再度违规
    ex.start("run4")
    drain(ex, "run4")

    info = ex.inspect("run4")
    actions = info["actions"]
    assert len(actions) == 2, actions

    gen1, gen2 = actions
    assert gen1["workspace_id"] == "ws_x" and gen1["generation"] == 1
    assert gen1["opened_run"] == "run1" and gen1["closed_run"] == "run3"
    assert gen1["open"] is False
    assert gen2["workspace_id"] == "ws_x" and gen2["generation"] == 2
    assert gen2["opened_run"] == "run4" and gen2["closed_run"] is None
    assert gen2["open"] is True

    # run2 持续违规时沿用了 gen1，没有新建动作。
    assert [a["opened_run"] for a in actions] == ["run1", "run4"]
    ex.close()


if __name__ == "__main__":
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        test_two_generations_across_runs(Path(d))
    print("test_generations ok")
