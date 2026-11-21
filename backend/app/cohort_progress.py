"""同期群分析的持久窗口进度与交付台账。

长时间运行的同期群分析按「群组 × 周期窗口」逐格推进，每一格在同一个事务里
同时写入进度表和不可变的交付台账。因此：

* 进程中断后可以核对哪些窗口已经完成、哪些还没开始；
* 一个窗口只会以「完整」或「不存在」两种形态出现，不会消费半个窗口；
* 每个窗口都绑定 ``data_version``（输入事件批次的指纹），交付台账保留历史
  版本，迟到事件导致批次变化时旧版本被标记 superseded 而不是被覆盖；
* 查询调度可以直接读取这里的数据，判断失败恢复还需要处理哪些窗口。

存储使用标准库 sqlite3，默认落在 ``backend/.data/cohort_progress.sqlite3``，
可用环境变量 ``COHORT_PROGRESS_DB`` 覆盖。
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

RUN_RUNNING = "running"
RUN_INTERRUPTED = "interrupted"
RUN_COMPLETED = "completed"
RUN_SUPERSEDED = "superseded"

WINDOW_DELIVERED = "delivered"
WINDOW_SUPERSEDED = "superseded"


class CohortProgressError(RuntimeError):
    """进度台账被以不一致的方式使用。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _default_db_path() -> Path:
    override = os.environ.get("COHORT_PROGRESS_DB")
    if override:
        return Path(override)
    return Path(__file__).resolve().parent.parent / ".data" / "cohort_progress.sqlite3"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS cohort_runs (
    run_id          TEXT PRIMARY KEY,
    config_id       INTEGER NOT NULL,
    config_name     TEXT NOT NULL DEFAULT '',
    init_event      TEXT NOT NULL,
    return_event    TEXT NOT NULL,
    period_days     INTEGER NOT NULL,
    max_periods     INTEGER NOT NULL,
    data_version    TEXT NOT NULL,
    watermark       REAL NOT NULL,
    event_count     INTEGER NOT NULL,
    status          TEXT NOT NULL,
    stop_reason     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    completed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_cohort_runs_config
    ON cohort_runs(config_id, created_at);

CREATE TABLE IF NOT EXISTS cohort_run_cohorts (
    run_id          TEXT NOT NULL,
    cohort_index    INTEGER NOT NULL,
    cohort_id       TEXT NOT NULL,
    denominator     INTEGER NOT NULL,
    PRIMARY KEY (run_id, cohort_id)
);

CREATE TABLE IF NOT EXISTS cohort_run_events (
    run_id          TEXT PRIMARY KEY,
    payload         TEXT NOT NULL,
    stored_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cohort_windows (
    run_id          TEXT NOT NULL,
    cohort_id       TEXT NOT NULL,
    period_index    INTEGER NOT NULL,
    denominator     INTEGER NOT NULL,
    numerator       INTEGER NOT NULL,
    rate            REAL NOT NULL,
    status          TEXT NOT NULL,
    data_version    TEXT NOT NULL,
    delivered_at    TEXT NOT NULL,
    PRIMARY KEY (run_id, cohort_id, period_index)
);

CREATE TABLE IF NOT EXISTS cohort_deliveries (
    delivery_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    config_id       INTEGER NOT NULL,
    cohort_id       TEXT NOT NULL,
    period_index    INTEGER NOT NULL,
    denominator     INTEGER NOT NULL,
    numerator       INTEGER NOT NULL,
    rate            REAL NOT NULL,
    data_version    TEXT NOT NULL,
    status          TEXT NOT NULL,
    delivered_at    TEXT NOT NULL,
    superseded_by   TEXT
);
CREATE INDEX IF NOT EXISTS idx_cohort_deliveries_config
    ON cohort_deliveries(config_id, data_version);
CREATE INDEX IF NOT EXISTS idx_cohort_deliveries_run
    ON cohort_deliveries(run_id);

CREATE TABLE IF NOT EXISTS cohort_attempts (
    attempt_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    attempt_no      INTEGER NOT NULL,
    mode            TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    status          TEXT NOT NULL,
    stop_after_windows INTEGER,
    windows_written INTEGER NOT NULL DEFAULT 0,
    detail          TEXT,
    owner_pid       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_cohort_attempts_run
    ON cohort_attempts(run_id, attempt_id);
"""


class CohortProgressStore:
    """SQLite 进度台账；每次操作使用独立连接，事务内完成窗口落盘。

    写路径统一走 ``_write()``：进程内用一把锁串行化，跨进程靠 SQLite 的
    BEGIN IMMEDIATE + busy_timeout。read-modify-write 都在同一个写事务里，
    并发续跑不会因为「读时不存在、写时已存在」而互相踩踏。
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else _default_db_path()
        self._write_lock = threading.RLock()

    # ---- 连接与生命周期 ----

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None：由 _write() 显式控制 BEGIN IMMEDIATE / COMMIT，
        # 避免 sqlite3 模块隐式开事务与 PRAGMA 相互干扰。
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_SCHEMA)
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """独占写事务：进程内串行、进程间 BEGIN IMMEDIATE 排队。"""
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def reset(self) -> None:
        """清空台账（仅用于本地重置/测试）。"""
        with self._write() as conn:
            for table in (
                "cohort_attempts",
                "cohort_deliveries",
                "cohort_windows",
                "cohort_run_cohorts",
                "cohort_run_events",
                "cohort_runs",
            ):
                conn.execute(f"DELETE FROM {table}")

    # ---- 运行与批次 ----

    def create_run(
        self,
        *,
        run_id: str,
        config_id: int,
        config_name: str,
        init_event: str,
        return_event: str,
        period_days: int,
        max_periods: int,
        data_version: str,
        watermark: float,
        event_count: int,
        cohorts: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """登记一次分析运行及该批次的群组分母；并发下同一批次只会有一次运行。

        cohorts: [{"cohort_index": 0, "cohort_id": "W44-2023", "denominator": 12}]

        返回 ``(run, created)``。整个「查重 + 插入」在 BEGIN IMMEDIATE 写事务内
        完成，多个进程/线程同时提交同一 config_id + data_version 时，后到者拿到
        先到者的运行，不会分裂成多个批次各自写一半窗口。
        """
        now = _now()
        with self._write() as conn:
            existing = conn.execute(
                """
                SELECT * FROM cohort_runs
                WHERE config_id = ? AND data_version = ? AND status != ?
                ORDER BY rowid DESC LIMIT 1
                """,
                (config_id, data_version, RUN_SUPERSEDED),
            ).fetchone()
            if existing is not None:
                return dict(existing), False
            conn.execute(
                """
                INSERT INTO cohort_runs (
                    run_id, config_id, config_name, init_event, return_event,
                    period_days, max_periods, data_version, watermark, event_count,
                    status, stop_reason, created_at, updated_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)
                """,
                (
                    run_id, config_id, config_name, init_event, return_event,
                    period_days, max_periods, data_version, float(watermark),
                    event_count, RUN_RUNNING, now, now,
                ),
            )
            conn.executemany(
                """
                INSERT INTO cohort_run_cohorts
                    (run_id, cohort_index, cohort_id, denominator)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (run_id, int(c["cohort_index"]), str(c["cohort_id"]), int(c["denominator"]))
                    for c in cohorts
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO cohort_run_events (run_id, payload, stored_at)
                VALUES (?, ?, ?)
                """,
                (
                    run_id,
                    json.dumps(
                        [
                            {
                                "user_id": str(e.get("user_id", "")),
                                "event_name": str(e.get("event_name", "")),
                                "event_time": float(e.get("event_time", 0)),
                            }
                            for e in events
                        ],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    now,
                ),
            )
        run = self.get_run(run_id)
        assert run is not None
        return run, True

    def recall_events(self, run_id: str) -> list[dict[str, Any]] | None:
        """取回运行当初锁定的那批事件；进程重启后也能按同一批数据续跑。"""
        with self._read() as conn:
            row = conn.execute(
                "SELECT payload FROM cohort_run_events WHERE run_id = ?", (run_id,),
            ).fetchone()
        if row is None:
            return None
        return list(json.loads(row["payload"]))

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM cohort_runs WHERE run_id = ?", (run_id,),
            ).fetchone()
        return dict(row) if row else None

    def find_run(self, config_id: int, data_version: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                """
                SELECT * FROM cohort_runs
                WHERE config_id = ? AND data_version = ?
                ORDER BY rowid DESC LIMIT 1
                """,
                (config_id, data_version),
            ).fetchone()
        return dict(row) if row else None

    def list_runs(
        self,
        *,
        config_id: int | None = None,
        statuses: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if config_id is not None:
            clauses.append("config_id = ?")
            params.append(config_id)
        status_list = list(statuses) if statuses is not None else []
        if status_list:
            clauses.append(f"status IN ({', '.join('?' for _ in status_list)})")
            params.extend(status_list)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._read() as conn:
            rows = conn.execute(
                f"SELECT * FROM cohort_runs {where} ORDER BY created_at DESC, run_id",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def set_run_status(
        self, run_id: str, status: str, *, stop_reason: str | None = None,
    ) -> dict[str, Any]:
        now = _now()
        completed_at = now if status == RUN_COMPLETED else None
        with self._write() as conn:
            conn.execute(
                """
                UPDATE cohort_runs
                SET status = ?, stop_reason = ?, updated_at = ?,
                    completed_at = COALESCE(?, completed_at)
                WHERE run_id = ?
                """,
                (status, stop_reason, now, completed_at, run_id),
            )
        run = self.get_run(run_id)
        if run is None:
            raise CohortProgressError(f"运行 {run_id} 不存在")
        return run

    # ---- 群组分母 ----

    def get_run_cohorts(self, run_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                """
                SELECT * FROM cohort_run_cohorts
                WHERE run_id = ? ORDER BY cohort_index
                """,
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ---- 窗口进度 ----

    def get_windows(self, run_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                """
                SELECT * FROM cohort_windows
                WHERE run_id = ? ORDER BY cohort_id, period_index
                """,
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_window(
        self, run_id: str, cohort_id: str, period_index: int,
    ) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                """
                SELECT * FROM cohort_windows
                WHERE run_id = ? AND cohort_id = ? AND period_index = ?
                """,
                (run_id, cohort_id, period_index),
            ).fetchone()
        return dict(row) if row else None

    def deliver_window(
        self,
        *,
        run_id: str,
        cohort_id: str,
        period_index: int,
        denominator: int,
        numerator: int,
        rate: float,
    ) -> bool:
        """原子写入一个完整窗口；同时写入不可变交付台账。

        返回 True 表示本次新写入了窗口；False 表示该窗口此前已交付（幂等）。
        任何与运行批次不一致的写入都会抛错，避免新旧批次混拼。
        """
        run = self.get_run(run_id)
        if run is None:
            raise CohortProgressError(f"运行 {run_id} 不存在")
        if run["status"] == RUN_SUPERSEDED:
            raise CohortProgressError(f"运行 {run_id} 已被新批次取代，不能继续写入")
        if int(denominator) < 0 or not (0 <= int(numerator) <= int(denominator)):
            raise CohortProgressError(
                f"窗口 {cohort_id}/{period_index} 分子分母不合法："
                f"{numerator}/{denominator}"
            )
        expected_rate = round(int(numerator) / int(denominator) * 100, 2) if denominator else 0.0
        if abs(float(rate) - expected_rate) > 0.011:
            raise CohortProgressError(
                f"窗口 {cohort_id}/{period_index} 保留率与分子分母不一致"
            )

        now = _now()
        with self._write() as conn:
            # 独占写事务：并发的续跑者要么看到已存在的窗口，要么排队后写入，
            # 不会同时判断「不存在」再各写一次。
            current_run = conn.execute(
                "SELECT status, data_version FROM cohort_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if current_run is None:
                raise CohortProgressError(f"运行 {run_id} 不存在")
            if current_run["status"] == RUN_SUPERSEDED:
                raise CohortProgressError(f"运行 {run_id} 已被新批次取代，不能继续写入")
            data_version = str(current_run["data_version"])

            cohort_row = conn.execute(
                """
                SELECT denominator FROM cohort_run_cohorts
                WHERE run_id = ? AND cohort_id = ?
                """,
                (run_id, cohort_id),
            ).fetchone()
            if cohort_row is None:
                raise CohortProgressError(f"群组 {cohort_id} 不属于运行 {run_id}")
            if int(cohort_row["denominator"]) != int(denominator):
                raise CohortProgressError(
                    f"群组 {cohort_id} 分母与批次不一致："
                    f"{denominator} != {cohort_row['denominator']}"
                )

            existing = conn.execute(
                """
                SELECT * FROM cohort_windows
                WHERE run_id = ? AND cohort_id = ? AND period_index = ?
                """,
                (run_id, cohort_id, period_index),
            ).fetchone()
            if existing is not None:
                if (
                    int(existing["denominator"]) != int(denominator)
                    or int(existing["numerator"]) != int(numerator)
                    or abs(float(existing["rate"]) - float(rate)) > 0.011
                ):
                    raise CohortProgressError(
                        f"窗口 {cohort_id}/{period_index} 已交付且结果不同，拒绝覆盖"
                    )
                return False

            conn.execute(
                """
                INSERT INTO cohort_windows (
                    run_id, cohort_id, period_index, denominator, numerator,
                    rate, status, data_version, delivered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, cohort_id, period_index) DO NOTHING
                """,
                (
                    run_id, cohort_id, period_index, int(denominator),
                    int(numerator), float(rate), WINDOW_DELIVERED, data_version, now,
                ),
            )
            already_delivered = conn.execute(
                """
                SELECT 1 FROM cohort_deliveries
                WHERE run_id = ? AND cohort_id = ? AND period_index = ?
                  AND data_version = ? AND status = ?
                """,
                (run_id, cohort_id, period_index, data_version, WINDOW_DELIVERED),
            ).fetchone()
            if already_delivered is None:
                conn.execute(
                    """
                    INSERT INTO cohort_deliveries (
                        run_id, config_id, cohort_id, period_index, denominator,
                        numerator, rate, data_version, status, delivered_at,
                        superseded_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        run_id, int(run["config_id"]), cohort_id, period_index,
                        int(denominator), int(numerator), float(rate),
                        data_version, WINDOW_DELIVERED, now,
                    ),
                )
            conn.execute(
                "UPDATE cohort_runs SET updated_at = ? WHERE run_id = ?",
                (now, run_id),
            )
        return True

    def pending_windows(self, run_id: str) -> list[dict[str, Any]]:
        """按「群组 × 周期」列出尚未交付的窗口。"""
        run = self.get_run(run_id)
        if run is None:
            return []
        cohorts = self.get_run_cohorts(run_id)
        delivered = {
            (str(w["cohort_id"]), int(w["period_index"]))
            for w in self.get_windows(run_id)
            if w["status"] == WINDOW_DELIVERED
        }
        pending: list[dict[str, Any]] = []
        for cohort in cohorts:
            for period_index in range(int(run["max_periods"])):
                key = (str(cohort["cohort_id"]), period_index)
                if key not in delivered:
                    pending.append({
                        "run_id": run_id,
                        "cohort_id": str(cohort["cohort_id"]),
                        "cohort_index": int(cohort["cohort_index"]),
                        "period_index": period_index,
                        "denominator": int(cohort["denominator"]),
                    })
        return pending

    def window_counts(self, run_id: str) -> dict[str, int]:
        run = self.get_run(run_id)
        if run is None:
            return {"total": 0, "delivered": 0, "pending": 0}
        total = len(self.get_run_cohorts(run_id)) * int(run["max_periods"])
        delivered = sum(
            1 for w in self.get_windows(run_id) if w["status"] == WINDOW_DELIVERED
        )
        return {"total": total, "delivered": delivered, "pending": total - delivered}

    # ---- 交付台账 ----

    def list_deliveries(
        self,
        *,
        config_id: int | None = None,
        run_id: str | None = None,
        include_superseded: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if config_id is not None:
            clauses.append("config_id = ?")
            params.append(config_id)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if not include_superseded:
            clauses.append("status = ?")
            params.append(WINDOW_DELIVERED)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._read() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM cohort_deliveries {where}
                ORDER BY delivery_id
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def supersede_run(self, run_id: str, *, superseded_by: str) -> int:
        """把一个旧批次标记为 superseded，交付证据保留可查。"""
        now = _now()
        with self._write() as conn:
            conn.execute(
                """
                UPDATE cohort_runs
                SET status = ?, stop_reason = ?, updated_at = ?
                WHERE run_id = ? AND status != ?
                """,
                (RUN_SUPERSEDED, "superseded_by_new_batch", now, run_id, RUN_SUPERSEDED),
            )
            conn.execute(
                """
                UPDATE cohort_windows SET status = ?
                WHERE run_id = ? AND status = ?
                """,
                (WINDOW_SUPERSEDED, run_id, WINDOW_DELIVERED),
            )
            cursor = conn.execute(
                """
                UPDATE cohort_deliveries
                SET status = ?, superseded_by = ?
                WHERE run_id = ? AND status = ?
                """,
                (WINDOW_SUPERSEDED, superseded_by, run_id, WINDOW_DELIVERED),
            )
            superseded = cursor.rowcount
        return int(superseded)

    # ---- 尝试记录 ----

    def begin_attempt(
        self, run_id: str, *, mode: str, stop_after_windows: int | None,
    ) -> int:
        with self._write() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) AS n FROM cohort_attempts WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            attempt_no = int(row["n"]) + 1
            cursor = conn.execute(
                """
                INSERT INTO cohort_attempts (
                    run_id, attempt_no, mode, started_at, ended_at, status,
                    stop_after_windows, windows_written, detail, owner_pid
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, 0, NULL, ?)
                """,
                (
                    run_id, attempt_no, mode, _now(), RUN_RUNNING,
                    stop_after_windows, os.getpid(),
                ),
            )
            conn.execute(
                "UPDATE cohort_runs SET status = ?, updated_at = ? WHERE run_id = ?",
                (RUN_RUNNING, _now(), run_id),
            )
            return int(cursor.lastrowid)

    def reap_abandoned_attempts(self) -> int:
        """把属于已消失进程的 running 尝试改记为 interrupted。

        进程被杀/中断时无法执行 finally，这里用 PID 活性做兜底，保证调度器
        看到的是「中断但窗口进度完好」，而不是永远 running 的假象。
        当前进程内的活跃尝试不受影响。
        """
        reaped = 0
        with self._write() as conn:
            rows = conn.execute(
                "SELECT attempt_id, run_id, owner_pid FROM cohort_attempts WHERE status = ?",
                (RUN_RUNNING,),
            ).fetchall()
            for row in rows:
                owner = int(row["owner_pid"] or 0)
                if owner == os.getpid() or _pid_alive(owner):
                    continue
                now = _now()
                conn.execute(
                    """
                    UPDATE cohort_attempts
                    SET status = ?, ended_at = ?, detail = COALESCE(detail, 'abandoned')
                    WHERE attempt_id = ? AND status = ?
                    """,
                    (RUN_INTERRUPTED, now, int(row["attempt_id"]), RUN_RUNNING),
                )
                conn.execute(
                    """
                    UPDATE cohort_runs
                    SET status = ?, stop_reason = 'abandoned_process', updated_at = ?
                    WHERE run_id = ? AND status = ?
                    """,
                    (RUN_INTERRUPTED, now, row["run_id"], RUN_RUNNING),
                )
                reaped += 1
        return reaped

    def finish_attempt(
        self,
        attempt_id: int,
        *,
        status: str,
        windows_written: int,
        detail: str | None = None,
    ) -> None:
        with self._write() as conn:
            conn.execute(
                """
                UPDATE cohort_attempts
                SET status = ?, ended_at = ?, windows_written = ?, detail = ?
                WHERE attempt_id = ?
                """,
                (status, _now(), int(windows_written), detail, attempt_id),
            )

    def list_attempts(self, run_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                """
                SELECT * FROM cohort_attempts
                WHERE run_id = ? ORDER BY attempt_id
                """,
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def cohort_data_version(
    events: Iterable[dict[str, Any]],
    *,
    init_event: str,
    return_event: str,
    period_days: int,
    max_periods: int,
    watermark: float,
) -> str:
    """计算事件批次的稳定指纹。

    只纳入同期群口径真正依赖的字段（user_id / event_name / event_time），
    去重后排序。迟到一笔更早的注册事件会改变指纹，从而触发整批重算，
    而不是把新分子拼到旧分母上。
    """
    normalized: set[tuple[str, str, float]] = set()
    for event in events:
        try:
            event_time = float(event.get("event_time", 0))
        except (TypeError, ValueError):
            continue
        if event_time > watermark:
            continue
        normalized.add((
            str(event.get("user_id", "")),
            str(event.get("event_name", "")),
            round(event_time, 6),
        ))
    payload = {
        "init_event": init_event,
        "return_event": return_event,
        "period_days": int(period_days),
        "max_periods": int(max_periods),
        "watermark": round(float(watermark), 6),
        "events": sorted(normalized),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


cohort_progress_store = CohortProgressStore()
