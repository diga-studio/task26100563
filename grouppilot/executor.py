"""Incremental governance executor backed by SQLite."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from . import faults


def _is_violating(storage_gb: float, classification: str) -> bool:
    return storage_gb > 25 and classification != "Confidential"


@dataclass
class RunInfo:
    """Snapshot of a run's persisted state."""

    run_id: str
    status: str
    prev_start_seq: int
    start_seq: int
    cursor_id: Optional[str]
    judged_count: int
    actions: list = field(default_factory=list)
    total_judgments: int = 0


class GroupPilot:
    """Library entry point: upsert / start / advance / inspect."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS registry (
                workspace_id TEXT PRIMARY KEY,
                storage_gb REAL NOT NULL,
                classification TEXT NOT NULL,
                seq INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS registry_log (
                seq INTEGER PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                storage_gb REAL NOT NULL,
                classification TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_registry_log_ws
                ON registry_log(workspace_id, seq);
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                prev_start_seq INTEGER NOT NULL,
                start_seq INTEGER NOT NULL,
                status TEXT NOT NULL,
                cursor_id TEXT,
                judged_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS conclusions (
                workspace_id TEXT PRIMARY KEY,
                violating INTEGER NOT NULL,
                run_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL,
                opened_run_id TEXT NOT NULL,
                closed_run_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_actions_ws
                ON actions(workspace_id, status);
            """
        )
        self._conn.commit()

    def _meta_get(self, key: str, default: int = 0) -> int:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return default if row is None else int(row[0])

    def _meta_set(self, key: str, value: int) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def close(self) -> None:
        self._conn.close()

    def upsert(self, workspace_id: str, storage_gb: float, classification: str) -> None:
        seq = self._meta_get("seq") + 1
        with self._conn:
            self._meta_set("seq", seq)
            self._conn.execute(
                "INSERT INTO registry_log(seq, workspace_id, storage_gb, classification)"
                " VALUES(?, ?, ?, ?)",
                (seq, workspace_id, storage_gb, classification),
            )
            self._conn.execute(
                "INSERT INTO registry(workspace_id, storage_gb, classification, seq)"
                " VALUES(?, ?, ?, ?) "
                "ON CONFLICT(workspace_id) DO UPDATE SET"
                " storage_gb = excluded.storage_gb,"
                " classification = excluded.classification,"
                " seq = excluded.seq",
                (workspace_id, storage_gb, classification, seq),
            )

    def _run_row(self, run_id: str):
        return self._conn.execute(
            "SELECT run_id, status, prev_start_seq, start_seq, cursor_id, judged_count"
            " FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

    def _to_info(self, row) -> RunInfo:
        actions = [
            {
                "action_id": a[0],
                "workspace_id": a[1],
                "generation": a[2],
                "status": a[3],
                "opened_run_id": a[4],
                "closed_run_id": a[5],
            }
            for a in self._conn.execute(
                "SELECT action_id, workspace_id, generation, status,"
                " opened_run_id, closed_run_id FROM actions ORDER BY action_id"
            )
        ]
        return RunInfo(
            run_id=row[0],
            status=row[1],
            prev_start_seq=row[2],
            start_seq=row[3],
            cursor_id=row[4],
            judged_count=row[5],
            actions=actions,
            total_judgments=self._meta_get("judged_total"),
        )

    def start(self, run_id: str) -> RunInfo:
        row = self._run_row(run_id)
        if row is not None:
            return self._to_info(row)
        running = self._conn.execute(
            "SELECT run_id FROM runs WHERE status = 'running'"
        ).fetchone()
        if running is not None:
            raise RuntimeError(
                f"run {running[0]!r} is still unfinished; cannot start {run_id!r}"
            )
        prev = self._conn.execute("SELECT MAX(start_seq) FROM runs").fetchone()[0]
        prev_start_seq = int(prev) if prev is not None else 0
        start_seq = self._meta_get("seq")
        with self._conn:
            self._conn.execute(
                "INSERT INTO runs(run_id, prev_start_seq, start_seq, status,"
                " cursor_id, judged_count) VALUES(?, ?, ?, 'running', NULL, 0)",
                (run_id, prev_start_seq, start_seq),
            )
        return self._to_info(self._run_row(run_id))

    def _next_candidate(self, prev_start_seq: int, start_seq: int, cursor_id):
        return self._conn.execute(
            "SELECT workspace_id FROM registry_log"
            " WHERE seq > ? AND seq <= ? AND workspace_id > COALESCE(?, '')"
            " GROUP BY workspace_id ORDER BY workspace_id LIMIT 1",
            (prev_start_seq, start_seq, cursor_id),
        ).fetchone()

    def _frozen_value(self, workspace_id: str, start_seq: int):
        return self._conn.execute(
            "SELECT storage_gb, classification FROM registry_log"
            " WHERE workspace_id = ? AND seq <= ? ORDER BY seq DESC LIMIT 1",
            (workspace_id, start_seq),
        ).fetchone()

    def _apply_judgment(self, run_id: str, workspace_id: str, violating: bool) -> None:
        """Commit judgment effects for one workspace (idempotent per run)."""
        with self._conn:
            row = self._conn.execute(
                "SELECT run_id FROM conclusions WHERE workspace_id = ?",
                (workspace_id,),
            ).fetchone()
            if row is not None and row[0] == run_id:
                return  # already applied for this run (crash recovery redo)
            open_action = self._conn.execute(
                "SELECT action_id FROM actions"
                " WHERE workspace_id = ? AND status = 'open'",
                (workspace_id,),
            ).fetchone()
            if violating:
                if open_action is None:
                    gen_row = self._conn.execute(
                        "SELECT MAX(generation) FROM actions WHERE workspace_id = ?",
                        (workspace_id,),
                    ).fetchone()
                    generation = (gen_row[0] or 0) + 1
                    self._conn.execute(
                        "INSERT INTO actions(workspace_id, generation, status,"
                        " opened_run_id, closed_run_id) VALUES(?, ?, 'open', ?, NULL)",
                        (workspace_id, generation, run_id),
                    )
            elif open_action is not None:
                self._conn.execute(
                    "UPDATE actions SET status = 'closed', closed_run_id = ?"
                    " WHERE action_id = ?",
                    (run_id, open_action[0]),
                )
            self._conn.execute(
                "INSERT INTO conclusions(workspace_id, violating, run_id)"
                " VALUES(?, ?, ?) "
                "ON CONFLICT(workspace_id) DO UPDATE SET"
                " violating = excluded.violating, run_id = excluded.run_id",
                (workspace_id, 1 if violating else 0, run_id),
            )
            self._meta_set("judged_total", self._meta_get("judged_total") + 1)

    def _commit_progress(self, run_id: str, cursor_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE runs SET cursor_id = ?, judged_count = judged_count + 1"
                " WHERE run_id = ?",
                (cursor_id, run_id),
            )

    def advance(self, run_id: str, budget: int) -> RunInfo:
        row = self._run_row(run_id)
        if row is None:
            raise KeyError(f"unknown run_id: {run_id!r}")
        if row[1] == "done":
            return self._to_info(row)
        _, _, prev_start_seq, start_seq, cursor_id, _ = row
        remaining = max(0, budget)
        while remaining > 0:
            cand = self._next_candidate(prev_start_seq, start_seq, cursor_id)
            if cand is None:
                faults.check("before_done_commit")
                with self._conn:
                    self._conn.execute(
                        "UPDATE runs SET status = 'done' WHERE run_id = ?", (run_id,)
                    )
                faults.check("after_done_commit")
                break
            workspace_id = cand[0]
            storage_gb, classification = self._frozen_value(workspace_id, start_seq)
            self._apply_judgment(
                run_id, workspace_id, _is_violating(storage_gb, classification)
            )
            faults.check("after_effect_commit")
            self._commit_progress(run_id, workspace_id)
            faults.check("after_progress_commit")
            cursor_id = workspace_id
            remaining -= 1
        return self._to_info(self._run_row(run_id))

    def inspect(self, run_id: str) -> RunInfo:
        row = self._run_row(run_id)
        if row is None:
            raise KeyError(f"unknown run_id: {run_id!r}")
        return self._to_info(row)
