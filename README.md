# task26100563

# GroupPilot 增量治理执行器

纯 Python + SQLite 实现的增量治理执行器，无外部服务依赖。

## 策略

唯一保留的分类升级策略：`storage_gb > 25` 且 `classification != "Confidential"` 即违规
（见 `grouppilot/policy.py`）。

## API

```python
from grouppilot import GovernanceExecutor

ex = GovernanceExecutor("governance.db")   # 可选 batch_size、fault_hook
ex.upsert("ws-1", 30.0, "Public")          # 新增或替换工作区数据
run = ex.start("run-0001")                 # 冻结启动边界；重复 run_id 返回原任务
res = ex.advance("run-0001", budget=500)   # 最多判定 500 个；res.done 表示完成
info = ex.inspect("run-0001")              # 状态 / 已提交结果 / 动作历史（不判定）
ex.close()
```

- 首个任务判定启动时的全部工作区；后续任务只判定上一任务启动后、本次启动前
  被 Upsert 的 ID（同一 ID 多次写入只取启动时最新值）。
- 任务启动后的写入对当前任务不可见（按 seq 边界冻结），由下一任务处理。
- 治理动作按违规世代记录：首次违规开动作，持续违规沿用，合规关闭，再违规开新世代。
- 崩溃恢复：每批分两个事务（先幂等提交结果与动作，再提交进度游标）。
  进程被强制结束后重开数据库，`start(原 run_id)` 返回原任务，`advance` 继续推进；
  已提交效果凭 `(run_id, workspace_id)` 结果行去重，不会重复创建动作。

## 故障注入

子进程内用环境变量控制硬退出（`os._exit`，非异常）：

```bash
GROUPPILOT_CRASH_POINT=after_effects_commit GROUPPILOT_CRASH_N=2 \
    python3 tests/crash_child.py /tmp/g.db 6
```

触发点：`before_effects_commit` / `after_effects_commit` / `after_progress_commit`。

## 运行测试

```bash
python3 -m pytest tests -q
```

覆盖：冻结视图与增量覆盖（`test_freeze.py`）、子进程硬退出崩溃一致性
（`test_crash.py`）、动作世代（`test_generations.py`）、
10 万工作区中 10 个增量写入的判定计数（`test_incremental_scale.py`）。
