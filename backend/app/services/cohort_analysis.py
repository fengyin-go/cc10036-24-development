"""同期群分析领域服务：保留率计算、群组追踪、跨模块数据消费。

这里是「持久窗口进度」的编排层：真正落盘、按窗口原子提交、保留历史交付
证据的部分在 :mod:`app.cohort_progress`。服务层负责：

* 计算整批事件的保留率明细（分子/分母同源）；
* 以批次指纹（data_version）判定是「续跑同一批」还是「换批重算」；
* 按窗口推进，支持 ``stop_after_windows`` 模拟/复现中断；
* 给查询调度提供失败恢复计划；
* 给用户提供已交付留存的核对视图。
"""
from __future__ import annotations

from typing import Any

from app.analytics_engine import compute_cohort_retention, compute_cohort_retention_detail
from app.cohort_progress import (
    RUN_COMPLETED,
    RUN_INTERRUPTED,
    RUN_RUNNING,
    RUN_SUPERSEDED,
    WINDOW_DELIVERED,
    CohortProgressError,
    cohort_data_version,
    cohort_progress_store,
)
from app.services.base import BaseModuleService
from app.store import store


class CohortAnalysisService(BaseModuleService):
    MODULE = "cohort_analysis"
    ENTITY = "群组配置"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["群组名称", "初始事件", "回访事件", "分析周期", "群组大小", "保留计算", "显示格式"]
    REQUIRED_FIELDS = ["群组名称", "初始事件", "回访事件"]
    STATUS_ORDER = ["草稿", "分析中", "已完成", "已归档"]
    ACTION_RULES = {"开始分析": "分析中", "完成分析": "已完成", "归档群组": "已归档"}
    NEGATIVE_ACTIONS = []

    # ---- 配置解析 ----

    def _resolve_config(self, config_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, config_id)

    @staticmethod
    def _period_settings(config: dict[str, Any]) -> tuple[str, str, int, int]:
        init_event = str(config.get("初始事件", ""))
        return_event = str(config.get("回访事件", ""))
        try:
            period_days = int(str(config.get("分析周期", "7")))
        except ValueError:
            period_days = 7
        max_periods = 12 if str(config.get("保留计算", "standard")).lower() == "bounded" else 8
        return init_event, return_event, period_days, max_periods

    # ---- 主流程 ----

    def run_cohort_analysis(
        self,
        config_id: int,
        events: list[dict[str, Any]],
        *,
        resume: bool = True,
        stop_after_windows: int | None = None,
        watermark: float | None = None,
    ) -> dict[str, Any]:
        """执行（或续跑）同期群分析：按窗口持久化进度并产出交付台账。

        - 传入的事件批次与已有运行一致：续跑未完成窗口，不重算已交付窗口。
        - 批次指纹发生变化（例如迟到一笔更早的注册事件）：旧运行整体标记为
          superseded，新运行从该批次重新计算全部窗口，避免新旧分子分母混拼。
        - ``stop_after_windows`` 在第 N 个完整窗口之后停止并标记 interrupted，
          用于复现「按周计算到一半」的中断场景；窗口本身不会写一半。
        """
        config = self._resolve_config(config_id)
        if config is None:
            return {"ok": False, "message": f"同期群配置 {config_id} 不存在"}

        init_event, return_event, period_days, max_periods = self._period_settings(config)
        events = list(events or [])
        if watermark is None:
            watermark = _max_event_time(events)
        # 水位线以内的批次才是本次计算的快照；指纹和分子分母都只取自这一批。
        events = _events_at_or_before(events, watermark)

        detail = compute_cohort_retention_detail(
            events, init_event, return_event, period_days, max_periods,
        )
        data_version = cohort_data_version(
            events,
            init_event=init_event,
            return_event=return_event,
            period_days=period_days,
            max_periods=max_periods,
            watermark=watermark,
        )

        progress = cohort_progress_store
        progress.reap_abandoned_attempts()

        run = progress.find_run(config_id, data_version)
        if run is not None and run["status"] == RUN_SUPERSEDED:
            # 旧批次被新批次取代后又整批重跑：视为一次新的运行，而不是复活旧批次。
            run = None
        created_new_run = run is None
        resumed = False
        superseded_runs: list[str] = []

        if run is not None and run["status"] == RUN_COMPLETED:
            return self._build_result(
                config, run, detail,
                message="该事件批次已完整交付，返回已发布的留存依据",
                resumed=False,
                written=0,
            )

        if created_new_run:
            # 新批次：旧批次的交付证据保留为 superseded，不覆盖、不混拼。
            run, created = progress.create_run(
                run_id=_new_run_id(config_id, data_version),
                config_id=config_id,
                config_name=str(config.get("群组名称", "")),
                init_event=init_event,
                return_event=return_event,
                period_days=period_days,
                max_periods=max_periods,
                data_version=data_version,
                watermark=float(watermark),
                event_count=len(events),
                cohorts=[
                    {
                        "cohort_index": index,
                        "cohort_id": cohort_id,
                        "denominator": detail.cohort_denominators[index],
                    }
                    for index, cohort_id in enumerate(detail.cohort_ids)
                ],
                events=events,
            )
            if not created:
                # 另一个并发调用已经建好同一批次：转为续跑它，避免重复开批。
                resumed = True
                self._assert_snapshot_matches(run, detail)
            else:
                for other in progress.list_runs(config_id=config_id):
                    if other["run_id"] == run["run_id"]:
                        continue
                    if other["status"] == RUN_SUPERSEDED:
                        continue
                    progress.supersede_run(other["run_id"], superseded_by=run["run_id"])
                    superseded_runs.append(str(other["run_id"]))
        else:
            resumed = True
            if not resume:
                return {
                    "ok": False,
                    "message": "该批次存在未完成的运行；如需续跑请允许恢复",
                    "run_id": run["run_id"],
                    "data_version": data_version,
                    "pending_windows": len(progress.pending_windows(run["run_id"])),
                }
            self._assert_snapshot_matches(run, detail)

        stored_cohorts = progress.get_run_cohorts(run["run_id"])
        if [row["cohort_id"] for row in stored_cohorts] != detail.cohort_ids:
            return {
                "ok": False,
                "message": "批次指纹相同但群组结构不一致，拒绝续跑",
                "run_id": run["run_id"],
                "data_version": data_version,
            }

        pending = progress.pending_windows(run["run_id"])
        mode = "resume" if resumed else "initial"
        attempt_id = progress.begin_attempt(
            run["run_id"], mode=mode, stop_after_windows=stop_after_windows,
        )

        cohort_index_map = {row["cohort_id"]: int(row["cohort_index"]) for row in stored_cohorts}
        written = 0
        interrupted = False
        try:
            for cell in pending:
                if stop_after_windows is not None and written >= stop_after_windows:
                    interrupted = True
                    break
                cohort_index = cohort_index_map[cell["cohort_id"]]
                numerator = detail.numerators[cohort_index][cell["period_index"]]
                rate = detail.retention_matrix[cohort_index][cell["period_index"]]
                progress.deliver_window(
                    run_id=run["run_id"],
                    cohort_id=cell["cohort_id"],
                    period_index=cell["period_index"],
                    denominator=detail.cohort_denominators[cohort_index],
                    numerator=numerator,
                    rate=rate,
                )
                written += 1
        except CohortProgressError as exc:
            progress.finish_attempt(
                attempt_id, status=RUN_INTERRUPTED, windows_written=written,
                detail=str(exc),
            )
            progress.set_run_status(run["run_id"], RUN_INTERRUPTED, stop_reason="write_conflict")
            return {
                "ok": False,
                "message": str(exc),
                "run_id": run["run_id"],
                "data_version": data_version,
                "windows_written": written,
            }

        remaining = progress.pending_windows(run["run_id"])
        if interrupted or remaining:
            progress.finish_attempt(
                attempt_id, status=RUN_INTERRUPTED, windows_written=written,
                detail=f"stopped_after_windows={stop_after_windows}",
            )
            progress.set_run_status(
                run["run_id"], RUN_INTERRUPTED,
                stop_reason=f"interrupted_after_{written}_windows",
            )
            self.run_action(config_id, "开始分析")
        else:
            progress.finish_attempt(
                attempt_id, status=RUN_COMPLETED, windows_written=written,
            )
            progress.set_run_status(run["run_id"], RUN_COMPLETED)
            self.run_action(config_id, "完成分析")

        refreshed = progress.get_run(run["run_id"])
        assert refreshed is not None
        result = self._build_result(
            config, refreshed, detail,
            message=(
                "分析已中断，已交付窗口可核对"
                if interrupted or remaining else "同期群分析已完成"
            ),
            resumed=resumed,
            written=written,
        )
        if superseded_runs:
            result["superseded_runs"] = superseded_runs
        return result

    # ---- 核对与交付视图 ----

    def inspect_progress(
        self, config_id: int, run_id: str | None = None,
    ) -> dict[str, Any]:
        """重新进入分析时核对窗口进度：哪些已交付、哪些待处理。"""
        config = self._resolve_config(config_id)
        if config is None:
            return {"ok": False, "message": f"同期群配置 {config_id} 不存在"}
        progress = cohort_progress_store
        progress.reap_abandoned_attempts()

        if run_id is not None:
            run = progress.get_run(run_id)
        else:
            runs = progress.list_runs(config_id=config_id)
            run = runs[0] if runs else None
        if run is None:
            return {
                "ok": True,
                "config_id": config_id,
                "config_name": str(config.get("群组名称", "")),
                "run": None,
                "runs": [],
                "windows": [],
                "pending_windows": [],
                "message": "暂无已持久化的分析运行",
            }

        windows = progress.get_windows(run["run_id"])
        pending = progress.pending_windows(run["run_id"])
        counts = progress.window_counts(run["run_id"])
        return {
            "ok": True,
            "config_id": config_id,
            "config_name": str(config.get("群组名称", "")),
            "run": run,
            "counts": counts,
            "windows": windows,
            "pending_windows": pending,
            "attempts": progress.list_attempts(run["run_id"]),
            "deliveries": progress.list_deliveries(run_id=run["run_id"]),
            "runs": progress.list_runs(config_id=config_id),
        }

    def get_delivered_retention(self, config_id: int) -> dict[str, Any]:
        """返回最近一批已交付的留存依据；未交付窗口显式为 null。"""
        config = self._resolve_config(config_id)
        if config is None:
            return {"ok": False, "message": f"同期群配置 {config_id} 不存在"}
        progress = cohort_progress_store
        progress.reap_abandoned_attempts()
        runs = [
            run for run in progress.list_runs(config_id=config_id)
            if run["status"] != RUN_SUPERSEDED
        ]
        if not runs:
            return {
                "ok": True,
                "config_id": config_id,
                "config_name": str(config.get("群组名称", "")),
                "published": None,
                "complete": False,
                "cohort_ids": [],
                "period_labels": [],
                "retention_matrix": [],
                "message": "尚无已交付窗口",
            }
        run = runs[0]
        stored_cohorts = progress.get_run_cohorts(run["run_id"])
        windows = progress.get_windows(run["run_id"])
        delivered = {
            (str(w["cohort_id"]), int(w["period_index"])): w
            for w in windows if w["status"] == WINDOW_DELIVERED
        }
        period_labels = [f"周{i + 1}" for i in range(int(run["max_periods"]))]
        matrix: list[list[float | None]] = []
        numerators: list[list[int | None]] = []
        denominators: list[int] = []
        for cohort in stored_cohorts:
            cohort_row: list[float | None] = []
            numerator_row: list[int | None] = []
            for period_index in range(int(run["max_periods"])):
                window = delivered.get((str(cohort["cohort_id"]), period_index))
                if window is None:
                    cohort_row.append(None)
                    numerator_row.append(None)
                else:
                    cohort_row.append(float(window["rate"]))
                    numerator_row.append(int(window["numerator"]))
            matrix.append(cohort_row)
            numerators.append(numerator_row)
            denominators.append(int(cohort["denominator"]))
        return {
            "ok": True,
            "config_id": config_id,
            "config_name": str(config.get("群组名称", "")),
            "run_id": run["run_id"],
            "data_version": run["data_version"],
            "watermark": run["watermark"],
            "event_count": run["event_count"],
            "run_status": run["status"],
            "published": True,
            "complete": run["status"] == RUN_COMPLETED,
            "cohort_ids": [str(c["cohort_id"]) for c in stored_cohorts],
            "period_labels": period_labels,
            "retention_matrix": matrix,
            "numerators": numerators,
            "denominators": denominators,
            "counts": progress.window_counts(run["run_id"]),
            "superseded_runs": [
                other["run_id"]
                for other in progress.list_runs(config_id=config_id)
                if other["status"] == RUN_SUPERSEDED
            ],
        }

    # ---- 查询调度消费的恢复计划 ----

    def build_recovery_plan(self, config_id: int | None = None) -> dict[str, Any]:
        """给查询调度判断失败恢复需要处理哪些窗口。"""
        progress = cohort_progress_store
        progress.reap_abandoned_attempts()
        configs = (
            [config_id] if config_id is not None
            else [int(row["id"]) for row in store.rows(self.MODULE)]
        )
        plans: list[dict[str, Any]] = []
        for cid in configs:
            config = self._resolve_config(cid)
            if config is None:
                continue
            active_runs = progress.list_runs(
                config_id=cid, statuses=[RUN_RUNNING, RUN_INTERRUPTED],
            )
            for run in active_runs:
                pending = progress.pending_windows(run["run_id"])
                counts = progress.window_counts(run["run_id"])
                plans.append({
                    "config_id": cid,
                    "config_name": str(config.get("群组名称", "")),
                    "run_id": run["run_id"],
                    "run_status": run["status"],
                    "data_version": run["data_version"],
                    "watermark": run["watermark"],
                    "stop_reason": run["stop_reason"],
                    "counts": counts,
                    "pending_windows": pending,
                    "retry_mode": "resume_same_batch",
                    "action": "恢复同一批数据，只补未交付窗口",
                })
        return {
            "ok": True,
            "plan_count": len(plans),
            "pending_window_total": sum(int(p["counts"]["pending"]) for p in plans),
            "plans": plans,
        }

    # ---- 兼容原有调用方 ----

    def compare_cohort_retention(
        self, config_id_1: int, config_id_2: int,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """比较两个群组的保留率。"""
        result_1 = self.run_cohort_analysis(config_id_1, events)
        result_2 = self.run_cohort_analysis(config_id_2, events)
        if not result_1.get("ok") or not result_2.get("ok"):
            return {"ok": False, "message": "分析运行失败"}
        r1_avg = result_1.get("avg_retention_week1", 0)
        r2_avg = result_2.get("avg_retention_week1", 0)
        return {
            "ok": True,
            "config_1": result_1.get("config_name"),
            "config_2": result_2.get("config_name"),
            "retention_1_week1": r1_avg,
            "retention_2_week1": r2_avg,
            "difference": round(r2_avg - r1_avg, 2),
        }

    def get_retention_for_segment(
        self, segment_id: str, events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """按分群筛选事件后运行同期群分析（分群分析→同期群分析的跨模块消费）。"""
        from app.services import service_for
        segment_svc = service_for("segment_rule")
        if segment_svc is None:
            return {"ok": False, "message": "分群服务不可用"}
        segment = store.find("segment_rule", int(segment_id))
        if segment is None:
            return {"ok": False, "message": f"分群 {segment_id} 不存在"}
        seg_results = segment_svc.compute_segment(int(segment_id), events)
        matched_data = seg_results.get("sample", events)
        return self.run_cohort_analysis(1, matched_data)

    # ---- 内部工具 ----

    def _assert_snapshot_matches(self, run: dict[str, Any], detail: Any) -> None:
        stored = cohort_progress_store.get_run_cohorts(run["run_id"])
        expected = {
            cohort_id: int(detail.cohort_denominators[index])
            for index, cohort_id in enumerate(detail.cohort_ids)
        }
        actual = {str(row["cohort_id"]): int(row["denominator"]) for row in stored}
        if actual != expected:
            raise CohortProgressError(
                "批次指纹相同但分母快照不一致，拒绝续跑以免混用分子分母"
            )

    def _build_result(
        self,
        config: dict[str, Any],
        run: dict[str, Any],
        detail: Any,
        *,
        message: str,
        resumed: bool,
        written: int,
    ) -> dict[str, Any]:
        progress = cohort_progress_store
        counts = progress.window_counts(run["run_id"])
        delivered = self.get_delivered_retention(int(run["config_id"]))
        avg_week1 = round(
            sum(row[0] for row in detail.retention_matrix if row) / max(len(detail.retention_matrix), 1),
            2,
        )
        return {
            "ok": True,
            "message": message,
            "run_id": run["run_id"],
            "config_name": str(config.get("群组名称", "")),
            "init_event": str(config.get("初始事件", "")),
            "return_event": str(config.get("回访事件", "")),
            "period_days": int(run["period_days"]),
            "max_periods": int(run["max_periods"]),
            "cohort_count": len(detail.cohort_ids),
            "cohort_ids": detail.cohort_ids,
            "period_labels": detail.period_labels,
            "retention_matrix": detail.retention_matrix,
            "cohort_denominators": detail.cohort_denominators,
            "numerators": detail.numerators,
            "avg_retention_week1": avg_week1,
            "data_version": run["data_version"],
            "watermark": run["watermark"],
            "event_count": run["event_count"],
            "run_status": run["status"],
            "resumed": resumed,
            "windows_written": written,
            "progress": counts,
            "published_retention": delivered,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "同期群分析数据集"
        rows = store.rows(self.MODULE)
        base["completed_analyses"] = sum(1 for row in rows if row.get("status") == "已完成")
        progress = cohort_progress_store
        runs = progress.list_runs()
        base["persisted_runs"] = len(runs)
        base["interrupted_runs"] = sum(
            1 for run in runs if run["status"] in (RUN_INTERRUPTED, RUN_RUNNING)
        )
        return base


def _new_run_id(config_id: int, data_version: str) -> str:
    import time
    import uuid
    return f"coh-{config_id}-{data_version[:10]}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"


def _max_event_time(events: list[dict[str, Any]]) -> float:
    latest = 0.0
    for event in events:
        try:
            latest = max(latest, float(event.get("event_time", 0)))
        except (TypeError, ValueError):
            continue
    return latest


def _events_at_or_before(events: list[dict[str, Any]], watermark: float) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for event in events:
        try:
            event_time = float(event.get("event_time", 0))
        except (TypeError, ValueError):
            continue
        if event_time <= watermark:
            snapshot.append(event)
    return snapshot
