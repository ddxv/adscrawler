import datetime
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import DEFAULT, patch

import pytest

from adscrawler.process import pipeline_manager as manager


class FakeResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class FakeEngine:
    def __init__(self):
        self.rows = {}
        self.next_id = 1

    @contextmanager
    def begin(self):
        yield self

    def execute(self, statement, params):
        sql = str(statement)
        if "INSERT INTO" in sql:
            row_id = self.next_id
            self.next_id += 1
            self.rows[row_id] = {**params, "context": json.loads(params["context"])}
            return FakeResult(row_id)
        if "SET status" in sql:
            self.rows[params["id"]].update(
                status=params["status"], error=params["error"]
            )
        elif "SET heartbeat_at" in sql:
            for row_id, row in self.rows.items():
                if (
                    row_id == params["run_id"]
                    or row["parent_run_id"] == params["run_id"]
                ) and row["status"] == "running":
                    row["heartbeat"] = True
        else:
            raise AssertionError(sql)


@pytest.fixture
def pgdb():
    return SimpleNamespace(engine=FakeEngine())


def test_daily_plan_preserves_order_and_dates(pgdb):
    today = datetime.date(2026, 7, 10)
    steps = manager._daily_s3_steps(pgdb, "week", today)
    assert len(steps) == 37
    assert [step.name for step in steps[:5]] == [
        "import_ranks_from_s3",
        "import_keywords_from_s3",
        "compact_incoming_version_details",
        "compact_incoming_app_details",
        "import_app_details_from_s3_into_db",
    ]
    assert steps[0].context == {
        "period": "week",
        "start_date": "2026-07-01",
        "end_date": "2026-07-10",
    }
    assert steps[1].context == {"start_date": "2026-07-05", "end_date": "2026-07-09"}
    assert steps[2].unit_key == "2026-07-05"
    assert steps[4].depends_on == steps[3].name
    assert steps[-1].name == "map_version_details"
    day_steps = manager._daily_s3_steps(pgdb, "day", today)
    assert day_steps[0].context["start_date"] == "2026-07-07"
    with pytest.raises(ValueError):
        manager._daily_s3_steps(pgdb, "month", today)


def test_daily_plan_calls_all_functions_with_expected_arguments(pgdb):
    names = (
        "import_ranks_from_s3",
        "import_keywords_from_s3",
        "compact_incoming_version_details",
        "compact_incoming_app_details",
        "import_app_details_from_s3_into_db",
        "delete_and_aggregate_s3_agg",
        "clean_history_tables",
        "process_company_history",
        "map_version_details",
    )
    with patch.multiple(manager, **dict.fromkeys(names, DEFAULT)) as mocks:
        steps = manager._daily_s3_steps(pgdb, "day", datetime.date(2026, 7, 10))
        for step in steps:
            step.run()
    mocks["import_ranks_from_s3"].assert_called_once_with(
        pgdb=pgdb,
        start_date=datetime.date(2026, 7, 7),
        end_date=datetime.date(2026, 7, 10),
        period="day",
    )
    mocks["import_keywords_from_s3"].assert_called_once_with(
        pgdb=pgdb,
        start_date=datetime.date(2026, 7, 5),
        end_date=datetime.date(2026, 7, 9),
    )
    assert mocks["compact_incoming_version_details"].call_count == 6
    assert mocks["compact_incoming_app_details"].call_count == 12
    assert mocks["import_app_details_from_s3_into_db"].call_count == 12
    assert mocks["delete_and_aggregate_s3_agg"].call_count == 2
    for name in (
        "clean_history_tables",
        "process_company_history",
        "map_version_details",
    ):
        mocks[name].assert_called_once_with(pgdb=pgdb)


def test_pipeline_completes_and_records_all_steps(pgdb):
    calls = []
    steps = [
        manager.PipelineStep("first", lambda: calls.append("first")),
        manager.PipelineStep("second", lambda: calls.append("second")),
    ]
    with patch.object(manager, "_daily_s3_steps", return_value=steps):
        manager.daily_s3_imports(pgdb, "week")
    rows = list(pgdb.engine.rows.values())
    assert calls == ["first", "second"]
    assert [row["status"] for row in rows] == ["completed"] * 3
    assert rows[1]["parent_run_id"] == 1
    assert rows[0]["context"]["period"] == "week"


def test_pipeline_continues_after_failure_and_skips_dependent_step(pgdb):
    calls = []

    def fail():
        raise ValueError("bad input")

    steps = [
        manager.PipelineStep("compact", fail, unit_key="store=1"),
        manager.PipelineStep(
            "import",
            lambda: calls.append("import"),
            unit_key="store=1",
            depends_on="compact",
        ),
        manager.PipelineStep("later", lambda: calls.append("later")),
    ]
    with patch.object(manager, "_daily_s3_steps", return_value=steps):
        manager.daily_s3_imports(pgdb, "day")
    rows = list(pgdb.engine.rows.values())
    assert [row["status"] for row in rows] == [
        "partial_failure",
        "failed",
        "skipped",
        "completed",
    ]
    assert rows[1]["error"] == "bad input"
    assert calls == ["later"]


def test_fatal_error_keeps_later_steps_pending(pgdb):
    def fail():
        raise RuntimeError("fatal")

    steps = [
        manager.PipelineStep("fatal", fail, fatal=True),
        manager.PipelineStep("later", lambda: None),
    ]
    with (
        patch.object(manager, "_daily_s3_steps", return_value=steps),
        pytest.raises(RuntimeError, match="fatal"),
    ):
        manager.daily_s3_imports(pgdb, "week")
    assert [row["status"] for row in pgdb.engine.rows.values()] == [
        "failed",
        "failed",
        "pending",
    ]


def test_interrupt_marks_step_and_run_failed(pgdb):
    def interrupt():
        raise KeyboardInterrupt()

    steps = [
        manager.PipelineStep("interrupt", interrupt),
        manager.PipelineStep("later", lambda: None),
    ]
    with (
        patch.object(manager, "_daily_s3_steps", return_value=steps),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.daily_s3_imports(pgdb, "week")
    assert [row["status"] for row in pgdb.engine.rows.values()] == [
        "failed",
        "failed",
        "pending",
    ]


def test_heartbeat_only_updates_running_rows(pgdb):
    tracker = manager.PipelineTracker(pgdb, "test")
    tracker.start(
        [
            manager.PipelineStep("a", lambda: None),
            manager.PipelineStep("b", lambda: None),
        ],
        {},
    )
    tracker.update(tracker.step_ids[("a", "")], "running")
    tracker.heartbeat()
    rows = list(pgdb.engine.rows.values())
    assert [row.get("heartbeat", False) for row in rows] == [True, True, False]
