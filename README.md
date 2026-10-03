# GroupPilot 增量治理执行器

纯 Python + SQLite 实现的增量治理执行器，可直接作为库调用，无外部服务依赖。

## 治理策略

`storage_gb > 25` 且 `classification != "Confidential"` 即判定为违规。

## API

```python
from grouppilot import GroupPilot

g = GroupPilot("governance.db")
g.upsert("ws-1", 30.0, "Public")     # 新增或替换工作区
info = g.start("run-2026-10")        # 启动任务，冻结启动时的注册表视角
info = g.advance("run-2026-10", 500) # 本批最多判定 500 个工作区
info = g.inspect("run-2026-10")      # 只读状态/进度/动作历史，不做判定
g.close()
```

- `start(run_id)`：重复 `run_id` 返回原任务（含已完成任务），不重新取快照；
  同一时刻最多一个未完成任务，否则抛 `RuntimeError`。
- `advance(run_id, budget)`：每个工作区的判定效果与进度分别小事务提交；
  完成后调用无效果。返回 `RunInfo`（状态、游标、已提交判定数、动作历史）。
- 首个任务判定启动时的全部工作区；后续任务只判定上一任务启动后到本次
  启动前被 upsert 的 ID（同一 ID 合并为启动时的最新值）。
- 违规世代：首次违规开一个动作，持续违规沿用，合规关闭，再次违规开新世代。

## 崩溃恢复

判定效果按工作区逐个幂等提交（`conclusions.run_id` 去重），进度单独提交；
进程在任意点被强杀后，重开同一数据库、以相同 `run_id` 继续 `advance`，
最终结果与动作历史与从未中断的执行一致。`synchronous=FULL` 保证已提交
事务 durable。

## 故障注入（测试用）

环境变量 `GROUPPILOT_CRASH_AT` 触发 `os._exit` 硬杀，取值为逗号分隔的
注入点，可带 `:N` 表示第 N 次命中时触发：

- `after_effect_commit`：判定效果已提交、进度未保存
- `after_progress_commit`：单个工作区进度已提交
- `before_done_commit` / `after_done_commit`：最终批次（完成标记）提交前后

例：`GROUPPILOT_CRASH_AT=after_effect_commit:2 python tests/crash_child.py db run1 100`

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：冻结视角与增量窗口、窗口内多次写入合并、重复 Start、并发任务拒绝、
子进程硬杀后的崩溃恢复（与参考执行核对全部动作历史）、违规世代生命周期、
10 万工作区中仅 10 次写入时判定计数精确等于变更 ID 数。
