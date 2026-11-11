"""Orchestrate and track the daily S3 import pipeline."""

import datetime
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    create_pipeline_run,
    heartbeat_pipeline_run,
    update_pipeline_run,
)
from adscrawler.process.app_details import (
    compact_incoming_app_details,
    import_app_details_from_s3_into_db,
)
from adscrawler.process.app_domain_history import process_company_history
from adscrawler.process.app_metrics_history import (
    clean_history_tables,
    run_app_metrics_pipeline,
)
from adscrawler.process.app_rankings import import_ranks_from_s3
from adscrawler.process.keywords import import_keywords_from_s3
from adscrawler.process.version_details import (
    compact_incoming_version_details,
    map_version_details,
)

logger = get_logger(__name__)
PIPELINE_NAME = "daily_s3_imports"
HEARTBEAT_INTERVAL_SECONDS = 60


@dataclass
class PipelineStep:
    name: str
    run: Callable[[], None]
    unit_key: str = ""
    context: dict[str, Any] = field(default_factory=dict)
    fatal: bool = False
    depends_on: str | None = None


class PipelineTracker:
    """Persist one pipeline run and its planned step attempts."""

    def __init__(self, pgdb: PostgresEngine, pipeline_name: str) -> None:
        self.pgdb = pgdb
        self.pipeline_name = pipeline_name
        self.run_id: int | None = None
        self.step_ids: dict[tuple[str, str], int] = {}

    def start(self, steps: list[PipelineStep], context: dict[str, Any]) -> None:
        self.run_id, self.step_ids = create_pipeline_run(
            self.pgdb,
            self.pipeline_name,
            context,
            [(step.name, step.unit_key, step.context) for step in steps],
        )

    def update(self, row_id: int, status: str, error: str | None = None) -> None:
        update_pipeline_run(self.pgdb, row_id, status, error)

    def heartbeat(self) -> None:
        heartbeat_pipeline_run(self.pgdb, self.run_id)


def _heartbeat_loop(tracker: PipelineTracker, stop: threading.Event) -> None:
    while not stop.wait(HEARTBEAT_INTERVAL_SECONDS):
        try:
            tracker.heartbeat()
        except Exception:
            logger.exception("Could not update pipeline heartbeat")


def _daily_s3_steps(
    pgdb: PostgresEngine, period: str, today: datetime.date
) -> list[PipelineStep]:
    if period not in {"week", "day"}:
        raise ValueError(f"Invalid period {period}")
    rank_start = today - datetime.timedelta(days=9 if period == "week" else 3)
    keyword_start = today - datetime.timedelta(days=5)
    keyword_end = today - datetime.timedelta(days=1)
    steps = [
        PipelineStep(
            "import_ranks_from_s3",
            partial(
                import_ranks_from_s3,
                pgdb=pgdb,
                start_date=rank_start,
                end_date=today,
                period=period,
            ),
            context={
                "period": period,
                "start_date": rank_start.isoformat(),
                "end_date": today.isoformat(),
            },
        ),
        PipelineStep(
            "import_keywords_from_s3",
            partial(
                import_keywords_from_s3,
                pgdb=pgdb,
                start_date=keyword_start,
                end_date=keyword_end,
            ),
            context={
                "start_date": keyword_start.isoformat(),
                "end_date": keyword_end.isoformat(),
            },
        ),
    ]
    for offset in range(5, -1, -1):
        crawled_date = (today - datetime.timedelta(days=offset)).isoformat()
        steps.append(
            PipelineStep(
                "compact_incoming_version_details",
                partial(compact_incoming_version_details, date_str=crawled_date),
                unit_key=crawled_date,
                context={"crawled_date": crawled_date},
                fatal=True,
            )
        )
        for store in (1, 2):
            unit_key = f"{crawled_date}/store={store}"
            context = {"crawled_date": crawled_date, "store": store}
            steps.extend(
                [
                    PipelineStep(
                        "compact_incoming_app_details",
                        partial(
                            compact_incoming_app_details,
                            store=store,
                            crawled_date=crawled_date,
                        ),
                        unit_key=unit_key,
                        context=context,
                    ),
                    PipelineStep(
                        "import_app_details_from_s3_into_db",
                        partial(
                            import_app_details_from_s3_into_db,
                            store=store,
                            crawled_date=crawled_date,
                            pgdb=pgdb,
                        ),
                        unit_key=unit_key,
                        context=context,
                        depends_on="compact_incoming_app_details",
                    ),
                ]
            )
    steps.extend(
        [
            PipelineStep(
                "run_app_metrics_pipeline", partial(run_app_metrics_pipeline, pgdb=pgdb)
            ),
            PipelineStep(
                "clean_history_tables",
                partial(clean_history_tables, pgdb=pgdb),
                depends_on="run_app_metrics_pipeline",
            ),
            PipelineStep(
                "process_company_history", partial(process_company_history, pgdb=pgdb)
            ),
            PipelineStep(
                "map_version_details", partial(map_version_details, pgdb=pgdb)
            ),
        ]
    )
    return steps


def daily_s3_imports(pgdb: PostgresEngine, period: str) -> None:
    """Run the existing import sequence, recording each planned step in PostgreSQL.

    A completed step means its function returned normally; functions that handle
    errors internally may still have recorded individual failures elsewhere.
    """
    today = datetime.date.today()
    steps = _daily_s3_steps(pgdb, period, today)
    tracker = PipelineTracker(pgdb, PIPELINE_NAME)
    tracker.start(steps, {"period": period, "run_date": today.isoformat()})
    assert tracker.run_id is not None
    stop = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat_loop, args=(tracker, stop), daemon=True
    )
    heartbeat.start()
    failed_steps: set[tuple[str, str]] = set()
    try:
        for step in steps:
            step_id = tracker.step_ids[(step.name, step.unit_key)]
            if step.depends_on and (step.depends_on, step.unit_key) in failed_steps:
                tracker.update(
                    step_id, "skipped", f"Dependency {step.depends_on} failed"
                )
                continue
            tracker.update(step_id, "running")
            try:
                step.run()
            except BaseException as exc:
                logger.exception(
                    "Pipeline step failed: %s %s", step.name, step.unit_key
                )
                tracker.update(step_id, "failed", str(exc)[:1000])
                failed_steps.add((step.name, step.unit_key))
                if step.fatal or not isinstance(exc, Exception):
                    raise
            else:
                tracker.update(step_id, "completed")
        tracker.update(
            tracker.run_id, "partial_failure" if failed_steps else "completed"
        )
    except BaseException as exc:
        try:
            tracker.update(tracker.run_id, "failed", str(exc)[:1000])
        except Exception:
            logger.exception("Could not record pipeline failure")
        raise
    finally:
        stop.set()
        heartbeat.join(timeout=2)
