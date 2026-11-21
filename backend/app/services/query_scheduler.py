"""查询调度领域服务：任务编排、依赖链、超时控制。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.cohort_progress import cohort_progress_store
from app.store import store


class QuerySchedulerService(BaseModuleService):
    MODULE = "query_scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["计划名称", "查询语句", "执行周期", "依赖任务", "超时分钟", "失败重试", "优先级"]
    REQUIRED_FIELDS = ["计划名称", "查询语句", "执行周期"]
    STATUS_ORDER = ["待调度", "执行中", "已完成", "调度失败", "已暂停"]
    ACTION_RULES = {"开始执行": "执行中", "完成调度": "已完成", "标记失败": "调度失败", "暂停计划": "已暂停"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # ---- 业务方法 ----

    def build_dependency_graph(self) -> dict[str, Any]:
        """构建调度计划的依赖图（数据血缘→查询调度消费）。"""
        rows = store.rows(self.MODULE)
        graph: dict[str, list[str]] = {}  # plan_name -> [dependent_plan_names]
        for row in rows:
            name = str(row.get("计划名称", ""))
            deps_raw = str(row.get("依赖任务", ""))
            deps = [d.strip() for d in deps_raw.split(",") if d.strip()]
            graph[name] = deps
        return {"ok": True, "nodes": len(rows), "graph": graph}

    def topological_order(self) -> dict[str, Any]:
        """按依赖拓扑排序调度计划（血缘关系→调度顺序）。"""
        deps = self.build_dependency_graph().get("graph", {})
        in_degree: dict[str, int] = {name: 0 for name in deps}
        for name, dep_list in deps.items():
            for dep in dep_list:
                if dep in in_degree:
                    in_degree[dep] = in_degree.get(dep, 0) + 1

        from collections import deque
        queue: deque[str] = deque(name for name, deg in in_degree.items() if deg == 0)
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            if node in deps:
                for dep in deps[node]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        queue.append(dep)

        has_cycle = len(order) != len(deps)
        return {
            "ok": True,
            "plan_count": len(deps),
            "execution_order": order,
            "has_cycle": has_cycle,
            "cycle_nodes": [n for n, d in in_degree.items() if d > 0],
        }

    def get_ready_plans(self) -> list[dict[str, Any]]:
        """获取待调度且无依赖的计划。"""
        rows = store.rows(self.MODULE)
        ready: list[dict[str, Any]] = []
        for row in rows:
            if row.get("status") != "待调度":
                continue
            deps_raw = str(row.get("依赖任务", ""))
            if not deps_raw.strip():
                ready.append(row)
                continue
            dep_names = [d.strip() for d in deps_raw.split(",")]
            all_completed = all(
                _is_completed(r) for r in rows
                if str(r.get("计划名称", "")) in dep_names
            )
            if all_completed:
                ready.append(row)
        return ready

    def retry_failed(self) -> dict[str, Any]:
        """重试失败的调度计划。"""
        rows = store.rows(self.MODULE)
        retried = 0
        for row in rows:
            if row.get("status") == "调度失败":
                max_retries = _parse_int(row.get("失败重试"), 3)
                retry_count = row.get("_retry_count", 0)
                if retry_count < max_retries:
                    row["_retry_count"] = retry_count + 1
                    self.run_action(int(row["id"]), "开始执行")
                    retried += 1
        return {"ok": True, "retried_count": retried}

    # ---- 跨模块：读取同期群持久窗口进度 ----

    def get_cohort_recovery_plan(self, config_id: int | None = None) -> dict[str, Any]:
        """读取同期群进度数据，判断失败恢复需要处理的窗口。"""
        cohort_svc = _cohort_service()
        if cohort_svc is None:
            return {"ok": False, "message": "同期群服务不可用"}
        plan = cohort_svc.build_recovery_plan(config_id)
        plan["source_module"] = "cohort_analysis"
        plan["scheduler"] = "query_scheduler"
        return plan

    def retry_cohort_recovery(
        self,
        config_id: int | None = None,
        events: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按同期群进度恢复失败窗口：同一批次续跑，从未交付窗口继续。"""
        cohort_svc = _cohort_service()
        if cohort_svc is None:
            return {"ok": False, "message": "同期群服务不可用"}

        plan = cohort_svc.build_recovery_plan(config_id)
        if not plan.get("plans"):
            return {
                "ok": True,
                "message": "没有需要恢复的同期群窗口",
                "retried": 0,
                "plans": [],
            }

        retried: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for item in plan["plans"]:
            cid = int(item["config_id"])
            batch_events = (
                events if events is not None else cohort_progress_store.recall_events(item["run_id"])
            )
            if batch_events is None:
                failures.append({
                    "config_id": cid,
                    "run_id": item["run_id"],
                    "data_version": item["data_version"],
                    "watermark": item["watermark"],
                    "message": "缺少该批次事件数据，无法恢复；请按 data_version 重新载入同一批事件",
                })
                continue
            result = cohort_svc.run_cohort_analysis(
                cid,
                batch_events,
                resume=True,
                watermark=float(item["watermark"]),
            )
            retried.append({
                "config_id": cid,
                "run_id": item["run_id"],
                "ok": bool(result.get("ok")),
                "run_status": result.get("run_status"),
                "windows_written": result.get("windows_written"),
                "progress": result.get("progress"),
            })

        return {
            "ok": not failures,
            "message": (
                "同期群窗口恢复完成"
                if not failures else "部分同期群批次缺少事件数据，未能恢复"
            ),
            "retried": len(retried),
            "results": retried,
            "failures": failures,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["artifact"] = "查询调度数据集"
        base["ready_count"] = len(self.get_ready_plans())
        cohort_plan = self.get_cohort_recovery_plan()
        base["cohort_recovery_plans"] = cohort_plan.get("plan_count", 0)
        base["cohort_pending_windows"] = cohort_plan.get("pending_window_total", 0)
        return base


def _cohort_service() -> Any:
    from app.services import service_for
    return service_for("cohort_analysis")


def _is_completed(row: dict[str, Any]) -> bool:
    return row.get("status") == "已完成"


def _parse_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default