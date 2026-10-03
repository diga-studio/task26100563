"""子进程：建库、播种、启动任务并推进，在指定提交点 os._exit 硬退出。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grouppilot import GovernanceExecutor
from grouppilot.fault import crash_hook_from_env

SEED = [
    ("w00", 30, "Public"),        # 违规
    ("w01", 10, "Public"),        # 合规
    ("w02", 40, "Confidential"),  # 合规（Confidential 豁免）
    ("w03", 50, "Internal"),      # 违规
    ("w04", 25, "Public"),        # 合规（边界值）
    ("w05", 26, "Public"),        # 违规
]


def seed(ex):
    for workspace_id, storage_gb, classification in SEED:
        ex.upsert(workspace_id, storage_gb, classification)


def main():
    db_path = sys.argv[1]
    budget = int(sys.argv[2])
    ex = GovernanceExecutor(db_path, batch_size=1, fault_hook=crash_hook_from_env())
    seed(ex)
    ex.start("run1")
    ex.advance("run1", budget)
    ex.close()


if __name__ == "__main__":
    main()
