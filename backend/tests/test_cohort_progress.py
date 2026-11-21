"""同期群持久窗口进度的回归测试。

直接使用标准库 unittest，无需额外依赖：
    cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import importlib
import os
import tempfile
import threading
import unittest
from pathlib import Path

from app.analytics_engine import compute_cohort_retention_detail
from app.cohort_progress import CohortProgressStore
from app.services import service_for

WEEK = 7 * 86400
BASE = 1698768000  # 一周的起点


def _ts(week: int, day: int, hour: int = 0) -> float:
    return BASE + week * WEEK + day * 86400 + hour * 3600


def _batch() -> list[dict[str, float | str]]:
    events: list[dict[str, float | str]] = []
    for u in range(10):
        events.append({"user_id": f"a{u}", "event_name": "注册", "event_time": _ts(0, u % 5) + 1})
        for ret_week in range(1, 6):
            if (u + ret_week) % 3 != 0:
                events.append(
                    {"user_id": f"a{u}", "event_name": "登录", "event_time": _ts(ret_week, 1)}
                )
    for u in range(6):
        events.append({"user_id": f"b{u}", "event_name": "注册", "event_time": _ts(1, u % 4) + 2})
        for ret_week in range(2, 7):
            if (u + ret_week) % 2 == 0:
                events.append(
                    {"user_id": f"b{u}", "event_name": "登录", "event_time": _ts(ret_week, 2)}
                )
    return events


class CohortProgressTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        db_path = Path(self.tmp.name) / "progress.sqlite3"
        self._old = os.environ.get("COHORT_PROGRESS_DB")
        os.environ["COHORT_PROGRESS_DB"] = str(db_path)
        self.progress = CohortProgressStore(db_path)
        self._patched = [
            ("app.cohort_progress", "cohort_progress_store"),
            ("app.services.cohort_analysis", "cohort_progress_store"),
            ("app.services.query_scheduler", "cohort_progress_store"),
        ]
        self._saved: list[tuple[str, str, object]] = []
        for module_name, attr in self._patched:
            module = importlib.import_module(module_name)
            self._saved.append((module_name, attr, getattr(module, attr, None)))
            setattr(module, attr, self.progress)
        self.svc = service_for("cohort_analysis")
        self.sched = service_for("query_scheduler")
        self.events = _batch()

    def tearDown(self) -> None:
        for module_name, attr, original in self._saved:
            importlib.import_module(module_name).__dict__[attr] = original
        if self._old is None:
            os.environ.pop("COHORT_PROGRESS_DB", None)
        else:
            os.environ["COHORT_PROGRESS_DB"] = self._old
        self.tmp.cleanup()

    def test_interrupted_run_is_resumable_and_consistent(self) -> None:
        first = self.svc.run_cohort_analysis(1, self.events, stop_after_windows=5)
        self.assertTrue(first["ok"])
        self.assertEqual(first["run_status"], "interrupted")
        self.assertEqual(first["progress"]["delivered"], 5)

        published = self.svc.get_delivered_retention(1)
        self.assertFalse(published["complete"])
        self.assertEqual(
            sum(1 for row in published["retention_matrix"] for cell in row if cell is not None),
            5,
        )

        second = self.svc.run_cohort_analysis(1, self.events)
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual(second["run_status"], "completed")
        self.assertEqual(second["progress"]["pending"], 0)

        detail = compute_cohort_retention_detail(self.events, "注册", "登录", 7, 8)
        final = self.svc.get_delivered_retention(1)
        self.assertEqual(final["denominators"], detail.cohort_denominators)
        for i in range(len(detail.cohort_ids)):
            for p in range(8):
                self.assertEqual(final["retention_matrix"][i][p], detail.retention_matrix[i][p])
                self.assertEqual(final["numerators"][i][p], detail.numerators[i][p])

    def test_late_earlier_registration_supersedes_old_batch(self) -> None:
        first = self.svc.run_cohort_analysis(1, self.events, stop_after_windows=4)
        late = list(self.events) + [
            {"user_id": "zz", "event_name": "注册", "event_time": _ts(0, 0, hour=0) - 60},
            {"user_id": "zz", "event_name": "登录", "event_time": _ts(1, 0)},
        ]
        second = self.svc.run_cohort_analysis(1, late)
        self.assertNotEqual(second["run_id"], first["run_id"])
        self.assertNotEqual(second["data_version"], first["data_version"])

        old = self.progress.get_run(first["run_id"])
        self.assertEqual(old["status"], "superseded")
        old_windows = self.progress.get_windows(first["run_id"])
        self.assertTrue(old_windows and all(w["status"] == "superseded" for w in old_windows))

        # 旧批次的分母仍在台账里可核对；新批次首周分母包含迟到的注册。
        old_cohorts = {c["cohort_id"]: c["denominator"] for c in
                       self.progress.get_run_cohorts(first["run_id"])}
        new_cohorts = {c["cohort_id"]: c["denominator"] for c in
                       self.progress.get_run_cohorts(second["run_id"])}
        self.assertEqual(new_cohorts[second["cohort_ids"][0]],
                         old_cohorts[first["cohort_ids"][0]] + 1)

    def test_half_window_is_rejected(self) -> None:
        started = self.svc.run_cohort_analysis(1, self.events, stop_after_windows=0)
        run_id = started["run_id"]
        cohort = self.progress.get_run_cohorts(run_id)[0]
        with self.assertRaises(Exception):
            self.progress.deliver_window(
                run_id=run_id, cohort_id=cohort["cohort_id"], period_index=0,
                denominator=cohort["denominator"],
                numerator=cohort["denominator"] + 1, rate=100.0,
            )
        with self.assertRaises(Exception):
            self.progress.deliver_window(
                run_id=run_id, cohort_id=cohort["cohort_id"], period_index=0,
                denominator=cohort["denominator"], numerator=1, rate=99.0,
            )

    def test_concurrent_same_batch_yields_one_run(self) -> None:
        results: list[dict] = []
        errors: list[str] = []

        def worker() -> None:
            try:
                results.append(self.svc.run_cohort_analysis(1, self.events))
            except Exception as exc:  # pragma: no cover - 失败时给出可读信息
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len({r["run_id"] for r in results}), 1)
        run_id = results[0]["run_id"]
        deliveries = self.progress.list_deliveries(run_id=run_id)
        keys = [(d["cohort_id"], d["period_index"]) for d in deliveries]
        self.assertEqual(len(keys), len(set(keys)))

    def test_scheduler_reads_progress_and_recovers(self) -> None:
        started = self.svc.run_cohort_analysis(1, self.events, stop_after_windows=3)
        plan = self.sched.get_cohort_recovery_plan(1)
        self.assertEqual(plan["plan_count"], 1)
        self.assertEqual(plan["plans"][0]["run_id"], started["run_id"])
        self.assertEqual(
            plan["pending_window_total"], started["progress"]["total"] - 3,
        )
        recovered = self.sched.retry_cohort_recovery(1)
        self.assertTrue(recovered["ok"])
        self.assertEqual(recovered["results"][0]["run_status"], "completed")
        self.assertEqual(self.svc.get_delivered_retention(1)["counts"]["pending"], 0)

    def test_superseded_batch_rerun_does_not_revive_old_run(self) -> None:
        first = self.svc.run_cohort_analysis(1, self.events, stop_after_windows=2)
        late = list(self.events) + [
            {"user_id": "zz", "event_name": "注册", "event_time": _ts(0, 0) - 60},
        ]
        self.svc.run_cohort_analysis(1, late)
        again = self.svc.run_cohort_analysis(1, self.events)
        self.assertNotEqual(again["run_id"], first["run_id"])
        self.assertEqual(self.progress.get_run(first["run_id"])["status"], "superseded")


if __name__ == "__main__":
    unittest.main()
