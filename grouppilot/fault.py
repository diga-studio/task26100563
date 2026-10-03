"""可控故障注入：供子进程在指定提交点直接 os._exit 退出。

用法（子进程内）：
    import os
    from grouppilot import GovernanceExecutor
    from grouppilot.fault import crash_hook_from_env
    ex = GovernanceExecutor(db_path, batch_size=1, fault_hook=crash_hook_from_env())

环境变量：
    GROUPPILOT_CRASH_POINT  触发点：before_effects_commit /
                            after_effects_commit / after_progress_commit
    GROUPPILOT_CRASH_N      第几次命中该触发点时退出（从 1 开始，默认 1）
"""
from __future__ import annotations

import os
from typing import Callable, Optional


def crash_hook_from_env() -> Optional[Callable[[str], None]]:
    point = os.environ.get("GROUPPILOT_CRASH_POINT")
    if not point:
        return None
    target = int(os.environ.get("GROUPPILOT_CRASH_N", "1"))
    counter = {"n": 0}

    def hook(hit: str) -> None:
        if hit != point:
            return
        counter["n"] += 1
        if counter["n"] >= target:
            os._exit(3)  # 模拟强制结束：不刷新、不抛异常

    return hook
