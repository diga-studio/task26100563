"""GroupPilot 增量治理执行器（SQLite 持久化）。

设计要点：
- 注册表采用追加式版本表 `versions`，每次 Upsert 分配单调递增的 seq。
  任务启动时只记录当前 seq 作为边界 boundary，不复制注册表；
  冻结视图 = 每个 ID 在 seq <= boundary 下的最新版本。
- 第 N 个任务的候选集 = 在 (prev_boundary, boundary] 窗口内出现过的 ID，
  通过 versions 上的 seq 索引按需 LIMIT 查询，不预载全集。
- 每个批次分两个事务：先提交判定结果与治理动作（幂等），再提交进度游标。
  崩溃后重做未提交判定时，凭 (run_id, workspace_id) 结果行去重，
  不会重复提交动作效果。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Optional

from .policy import is_violation

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    workspace_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    storage_gb REAL NOT NULL,
    classification TEXT NOT NULL,
    PRIMARY KEY (workspace_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_versions_seq ON versions (seq);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    prev_boundary INTEGER NOT NULL,
    boundary INTEGER NOT NULL,
    status TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    cursor TEXT
);
CREATE TABLE IF NOT EXISTS results (
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    violating INTEGER NOT NULL,
    PRIMARY KEY (run_id, workspace_id)
);
CREATE TABLE IF NOT EXISTS workspace_state (
    workspace_id TEXT PRIMARY KEY,
    violating INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 0,
    open_action_id INTEGER
);
CREATE TABLE IF NOT EXISTS actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    opened_run TEXT NOT NULL,
    closed_run TEXT,
    open INTEGER NOT NULL DEFAULT 1
);
"""

STATUS_RUNNING = "running"
STATUS_DONE = "done"


@dataclass
class RunInfo:
    run_id: str
    status: str
    prev_boundary: int
    boundary: int
    processed: int
    cursor: Optional[str]


@dataclass
class AdvanceResult:
    run_id: str
    judged: int
    done: bool


class GovernanceExecutor:
    """对单个 SQLite 数据库文件提供 Upsert/Start/Advance/Inspect。"""

    def __init__(
        self,
        db_path: str,
        batch_size: int = 100,
        fault_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.fault_hook = fault_hook
        self.eval_count = 0
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # -- 内部工具 -----------------------------------------------------------

    def _meta_get(self, key: str, default: int = 0) -> int:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return default if row is None else int(row[0])

    def _meta_set(self, key: str, value: int) -> None:
        self._conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _fire_hook(self, point: str) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point)

    # -- 四个操作 -----------------------------------------------------------

    def upsert(self, workspace_id: str, storage_gb: float, classification: str) -> None:
        """新增或替换工作区数据（追加一个版本）。"""
        seq = self._meta_get("seq") + 1
        with self._conn:
            self._meta_set("seq", seq)
            self._conn.execute(
                "INSERT INTO versions (workspace_id, seq, storage_gb, classification)"
                " VALUES (?, ?, ?, ?)",
                (workspace_id, seq, float(storage_gb), classification),
            )

    def _run_info(self, run_id: str) -> Optional[RunInfo]:
        row = self._conn.execute(
            "SELECT run_id, status, prev_boundary, boundary, processed, cursor"
            " FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return RunInfo(
            run_id=row[0],
            status=row[1],
            prev_boundary=int(row[2]),
            boundary=int(row[3]),
            processed=int(row[4]),
            cursor=row[5],
        )

    def start(self, run_id: str) -> RunInfo:
        """启动任务并冻结边界；重复 run_id 返回原任务，不重新取快照。"""
        existing = self._run_info(run_id)
        if existing is not None:
            return existing
        running = self._conn.execute(
            "SELECT run_id FROM runs WHERE status = ?", (STATUS_RUNNING,)
        ).fetchone()
        if running is not None:
            raise RuntimeError(
                f"run {running[0]!r} is still unfinished; "
                "at most one unfinished run is allowed"
            )
        boundary = self._meta_get("seq")
        prev_boundary = self._meta_get("last_boundary")
        with self._conn:
            self._conn.execute(
                "INSERT INTO runs (run_id, prev_boundary, boundary, status,"
                " processed, cursor) VALUES (?, ?, ?, ?, 0, NULL)",
                (run_id, prev_boundary, boundary, STATUS_RUNNING),
            )
            self._meta_set("last_boundary", boundary)
        return RunInfo(
            run_id=run_id,
            status=STATUS_RUNNING,
            prev_boundary=prev_boundary,
            boundary=boundary,
            processed=0,
            cursor=None,
        )

    def _fetch_candidates(self, run: RunInfo, limit: int) -> list[str]:
        """按 workspace_id 顺序取下一批候选 ID（不预载全集）。"""
        if run.cursor is None:
            rows = self._conn.execute(
                "SELECT DISTINCT workspace_id FROM versions"
                " WHERE seq > ? AND seq <= ?"
                " ORDER BY workspace_id LIMIT ?",
                (run.prev_boundary, run.boundary, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT DISTINCT workspace_id FROM versions"
                " WHERE seq > ? AND seq <= ? AND workspace_id > ?"
                " ORDER BY workspace_id LIMIT ?",
                (run.prev_boundary, run.boundary, run.cursor, limit),
            ).fetchall()
        return [row[0] for row in rows]

    def _frozen_value(self, workspace_id: str, boundary: int) -> tuple[float, str]:
        """启动边界处的冻结值：seq <= boundary 的最新版本。"""
        row = self._conn.execute(
            "SELECT storage_gb, classification FROM versions"
            " WHERE workspace_id = ? AND seq <= ?"
            " ORDER BY seq DESC LIMIT 1",
            (workspace_id, boundary),
        ).fetchone()
        return float(row[0]), row[1]

    def _apply_effects(self, run: RunInfo, batch: list[str]) -> None:
        """事务一：幂等提交本批判定结果与治理动作。"""
        with self._conn:
            for workspace_id in batch:
                seen = self._conn.execute(
                    "SELECT 1 FROM results WHERE run_id = ? AND workspace_id = ?",
                    (run.run_id, workspace_id),
                ).fetchone()
                if seen is not None:
                    continue  # 崩溃前已提交过效果，跳过重做
                storage_gb, classification = self._frozen_value(
                    workspace_id, run.boundary
                )
                self.eval_count += 1
                violating = is_violation(storage_gb, classification)
                state = self._conn.execute(
                    "SELECT violating, generation, open_action_id"
                    " FROM workspace_state WHERE workspace_id = ?",
                    (workspace_id,),
                ).fetchone()
                if state is None:
                    cur_violating, generation, open_action_id = 0, 0, None
                else:
                    cur_violating, generation, open_action_id = (
                        int(state[0]),
                        int(state[1]),
                        state[2],
                    )
                if violating:
                    if open_action_id is None:
                        generation += 1
                        cur = self._conn.execute(
                            "INSERT INTO actions (workspace_id, generation,"
                            " opened_run, open) VALUES (?, ?, ?, 1)",
                            (workspace_id, generation, run.run_id),
                        )
                        open_action_id = int(cur.lastrowid)
                else:
                    if open_action_id is not None:
                        self._conn.execute(
                            "UPDATE actions SET open = 0, closed_run = ?"
                            " WHERE id = ?",
                            (run.run_id, open_action_id),
                        )
                        open_action_id = None
                self._conn.execute(
                    "INSERT INTO workspace_state (workspace_id, violating,"
                    " generation, open_action_id) VALUES (?, ?, ?, ?)"
                    " ON CONFLICT(workspace_id) DO UPDATE SET"
                    " violating = excluded.violating,"
                    " generation = excluded.generation,"
                    " open_action_id = excluded.open_action_id",
                    (workspace_id, int(violating), generation, open_action_id),
                )
                self._conn.execute(
                    "INSERT INTO results (run_id, workspace_id, violating)"
                    " VALUES (?, ?, ?)",
                    (run.run_id, workspace_id, int(violating)),
                )

    def _commit_progress(self, run: RunInfo, cursor: str, processed: int) -> None:
        """事务二：提交进度游标。"""
        with self._conn:
            self._conn.execute(
                "UPDATE runs SET cursor = ?, processed = ? WHERE run_id = ?",
                (cursor, processed, run.run_id),
            )

    def advance(self, run_id: str, budget: int) -> AdvanceResult:
        """最多判定 budget 个工作区；完成后调用不产生新效果。"""
        run = self._run_info(run_id)
        if run is None:
            raise KeyError(f"unknown run_id: {run_id!r}")
        if run.status == STATUS_DONE:
            return AdvanceResult(run_id=run_id, judged=0, done=True)
        judged = 0
        cursor = run.cursor
        processed = run.processed
        remaining = max(0, budget)
        exhausted = False
        while remaining > 0:
            batch = self._fetch_candidates(run, min(self.batch_size, remaining))
            if not batch:
                exhausted = True
                break
            self._fire_hook("before_effects_commit")
            self._apply_effects(run, batch)
            self._fire_hook("after_effects_commit")
            cursor = batch[-1]
            processed += len(batch)
            judged += len(batch)
            remaining -= len(batch)
            self._commit_progress(run, cursor, processed)
            self._fire_hook("after_progress_commit")
            run = RunInfo(
                run_id=run.run_id,
                status=run.status,
                prev_boundary=run.prev_boundary,
                boundary=run.boundary,
                processed=processed,
                cursor=cursor,
            )
        if exhausted:
            with self._conn:
                self._conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    (STATUS_DONE, run_id),
                )
        return AdvanceResult(run_id=run_id, judged=judged, done=exhausted)

    def inspect(self, run_id: str) -> dict:
        """读取任务状态、已提交进度与治理动作历史；不执行策略判定。"""
        run = self._run_info(run_id)
        if run is None:
            raise KeyError(f"unknown run_id: {run_id!r}")
        results = self._conn.execute(
            "SELECT workspace_id, violating FROM results"
            " WHERE run_id = ? ORDER BY workspace_id",
            (run_id,),
        ).fetchall()
        actions = self._conn.execute(
            "SELECT id, workspace_id, generation, opened_run, closed_run, open"
            " FROM actions ORDER BY id"
        ).fetchall()
        return {
            "run_id": run.run_id,
            "status": run.status,
            "prev_boundary": run.prev_boundary,
            "boundary": run.boundary,
            "processed": run.processed,
            "cursor": run.cursor,
            "results": [
                {"workspace_id": row[0], "violating": bool(row[1])}
                for row in results
            ],
            "actions": [
                {
                    "id": int(row[0]),
                    "workspace_id": row[1],
                    "generation": int(row[2]),
                    "opened_run": row[3],
                    "closed_run": row[4],
                    "open": bool(row[5]),
                }
                for row in actions
            ],
        }

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "GovernanceExecutor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
