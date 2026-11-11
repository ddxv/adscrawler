import datetime
import io
import json
import os
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import psycopg
import pytest
from psycopg import sql

from adscrawler.process import app_metrics_history as metrics


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        yield {
            "Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]
        }

    def delete_objects(self, Bucket, Delete):
        for obj in Delete["Objects"]:
            del self.objects[obj["Key"]]
        return {}


class FakeDB:
    def __init__(self, weeks=()):
        self.cursor = Mock()
        self.cursor.fetchall.return_value = [(week,) for week in weeks]

    @contextmanager
    def get_driver_connection(self, autocommit=False):
        yield SimpleNamespace(), self.cursor


def country_frame():
    return pd.DataFrame(
        [[datetime.date(2026, 7, 6), 840, 4, 4.0, 20, 3, 1, 1, 1, 1, 1]],
        columns=metrics.COUNTRY_HISTORY_COLS,
    )


def test_complete_run_streams_both_tables_and_empty_existing_weeks():
    s3 = MemoryS3()
    db = FakeDB([datetime.date(2026, 6, 15)])
    run_id = "a" * 32
    cutoff = datetime.date(2026, 6, 10)
    end = datetime.date(2026, 7, 10)
    outputs = []

    def swap(**kwargs):
        outputs.append(
            (kwargs["table"], kwargs["partition_date"], "".join(kwargs["stream"]))
        )
        assert kwargs["date_column"] == "week_start"

    other_run = "b" * 32
    with (
        patch.object(metrics, "get_s3_client", return_value=s3),
        patch("adscrawler.process.storage.get_s3_client", return_value=s3),
        patch.object(metrics, "METRICS_BUCKETS", ("00",)),
    ):
        metrics.stage_app_metrics(
            country_frame(), pd.DataFrame(), 1, "00", run_id, cutoff, end
        )
        metrics.stage_app_metrics(
            pd.DataFrame(), pd.DataFrame(), 2, "00", run_id, cutoff, end
        )
        other_key = f"{metrics._metrics_run_prefix(other_run)}/manifest.json"
        s3.objects[other_key] = b"other run"
        with patch.object(
            metrics, "atomic_swap_date_partition_stream", side_effect=swap
        ):
            metrics.publish_app_metrics_history(db, run_id, end)
    assert s3.objects == {other_key: b"other run"}
    assert metrics._metrics_run_prefix(run_id).startswith(
        "tmp-data/app-hash-weekly-metrics/"
    )
    assert [(name, week) for name, week, _ in outputs] == [
        (name, week)
        for week in (datetime.date(2026, 6, 15), datetime.date(2026, 7, 6))
        for name in ("app_country_metrics_history", "app_global_metrics_history")
    ]
    assert outputs[0][2] == outputs[1][2] == outputs[3][2] == ""
    assert outputs[2][2].startswith("2026-07-06\t840\t4\t4.0\t")
    assert all(
        params == (cutoff, end)
        for _, params in (c.args for c in db.cursor.execute.call_args_list)
    )


def test_stream_formats_timestamp_and_null_for_postgres_copy():
    s3 = MemoryS3()
    frame = country_frame()
    frame["week_start"] = pd.to_datetime(frame["week_start"])
    frame.loc[0, "rating"] = None
    cutoff, end = datetime.date(2026, 6, 10), datetime.date(2026, 7, 10)
    with patch.object(metrics, "get_s3_client", return_value=s3):
        metrics.stage_app_metrics(frame, pd.DataFrame(), 1, "00", "a" * 32, cutoff, end)
        manifest = json.loads(
            s3.objects[next(k for k in s3.objects if k.endswith("manifest.json"))]
        )
        entry = manifest["tables"]["country"]["2026-07-06"]
        output = "".join(
            metrics._stream_metrics([entry], "country", datetime.date(2026, 7, 6))
        )
    assert output.startswith("2026-07-06\t840\t4\t\\N\t")


def test_missing_bucket_manifest_prevents_publish():
    s3 = MemoryS3()
    with (
        patch.object(metrics, "get_s3_client", return_value=s3),
        patch.object(metrics, "METRICS_BUCKETS", ("00",)),
    ):
        metrics.stage_app_metrics(
            country_frame(),
            pd.DataFrame(),
            1,
            "00",
            "a" * 32,
            datetime.date(2026, 6, 10),
            datetime.date(2026, 7, 10),
        )
        with (
            patch.object(metrics, "atomic_swap_date_partition_stream") as swap,
            pytest.raises(KeyError),
        ):
            metrics.publish_app_metrics_history(
                FakeDB(), "a" * 32, datetime.date(2026, 7, 10)
            )
        swap.assert_not_called()


def test_corrupt_data_prevents_all_swaps():
    s3 = MemoryS3()
    run_id = "a" * 32
    cutoff, end = datetime.date(2026, 6, 10), datetime.date(2026, 7, 10)
    with (
        patch.object(metrics, "get_s3_client", return_value=s3),
        patch.object(metrics, "METRICS_BUCKETS", ("00",)),
    ):
        metrics.stage_app_metrics(
            country_frame(), pd.DataFrame(), 1, "00", run_id, cutoff, end
        )
        metrics.stage_app_metrics(
            pd.DataFrame(), pd.DataFrame(), 2, "00", run_id, cutoff, end
        )
        key = next(key for key in s3.objects if key.endswith(".parquet"))
        s3.objects[key] = b"corrupt"
        with (
            patch.object(metrics, "atomic_swap_date_partition_stream") as swap,
            pytest.raises(Exception),
        ):
            metrics.publish_app_metrics_history(FakeDB(), run_id, end)
        swap.assert_not_called()


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"), reason="PostgreSQL test DSN not set"
)
def test_parquet_to_postgres_stream_replaces_complete_week():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    schema = f"test_metrics_stream_{uuid.uuid4().hex}"
    week = datetime.date(2026, 7, 6)
    run_id = "a" * 32
    s3 = MemoryS3()
    frame = country_frame()
    frame["week_start"] = pd.to_datetime(frame["week_start"])
    frame.loc[0, "rating"] = None

    class RealDB:
        @contextmanager
        def get_driver_connection(self, autocommit=False):
            with (
                psycopg.connect(dsn, autocommit=autocommit) as conn,
                conn.cursor() as cur,
            ):
                yield conn, cur

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            parent = sql.Identifier(schema, "metrics")
            partition = sql.Identifier(schema, "metrics_2026_07_06")
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {} (week_start date NOT NULL, country_id smallint NOT NULL, store_app int NOT NULL, rating real, rating_count integer, installs bigint, one_star integer, two_star integer, three_star integer, four_star integer, five_star integer) PARTITION BY RANGE (week_start)"
                ).format(parent)
            )
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})"
                ).format(
                    partition,
                    parent,
                    sql.Literal(week),
                    sql.Literal(week + datetime.timedelta(days=1)),
                )
            )
            conn.execute(
                sql.SQL(
                    "INSERT INTO {} (week_start, country_id, store_app) VALUES (%s, 1, 99)"
                ).format(parent),
                (week,),
            )
            with patch.object(metrics, "get_s3_client", return_value=s3):
                metrics.stage_app_metrics(
                    frame,
                    pd.DataFrame(),
                    1,
                    "00",
                    run_id,
                    datetime.date(2026, 6, 10),
                    datetime.date(2026, 7, 10),
                )
                manifest = json.loads(
                    s3.objects[
                        next(k for k in s3.objects if k.endswith("manifest.json"))
                    ]
                )
                entry = manifest["tables"]["country"][week.isoformat()]
                metrics.atomic_swap_date_partition_stream(
                    stream=metrics._stream_metrics([entry], "country", week),
                    columns=metrics.COUNTRY_HISTORY_COLS,
                    pgdb=RealDB(),
                    schema=schema,
                    table="metrics",
                    partition_prefix="metrics",
                    partition_date=week,
                    date_column="week_start",
                    unique_keys=["week_start", "country_id", "store_app"],
                )
            assert conn.execute(
                sql.SQL(
                    "SELECT week_start, country_id, store_app, rating FROM {}"
                ).format(parent)
            ).fetchall() == [(week, 840, 4, None)]
        finally:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def test_publish_failure_retains_staging():
    s3 = MemoryS3()
    run_id = "a" * 32
    cutoff, end = datetime.date(2026, 6, 10), datetime.date(2026, 7, 10)
    with (
        patch.object(metrics, "get_s3_client", return_value=s3),
        patch("adscrawler.process.storage.get_s3_client", return_value=s3),
        patch.object(metrics, "METRICS_BUCKETS", ("00",)),
    ):
        metrics.stage_app_metrics(
            country_frame(), pd.DataFrame(), 1, "00", run_id, cutoff, end
        )
        metrics.stage_app_metrics(
            pd.DataFrame(), pd.DataFrame(), 2, "00", run_id, cutoff, end
        )
        original = dict(s3.objects)
        with (
            patch.object(
                metrics,
                "atomic_swap_date_partition_stream",
                side_effect=RuntimeError("swap failed"),
            ),
            pytest.raises(RuntimeError, match="swap failed"),
        ):
            metrics.publish_app_metrics_history(FakeDB(), run_id, end)
    assert s3.objects == original


def test_cleanup_reports_s3_partial_delete_failure():
    s3 = MemoryS3()
    run_id = "a" * 32
    cutoff, end = datetime.date(2026, 6, 10), datetime.date(2026, 7, 10)
    with (
        patch.object(metrics, "get_s3_client", return_value=s3),
        patch("adscrawler.process.storage.get_s3_client", return_value=s3),
        patch.object(metrics, "METRICS_BUCKETS", ("00",)),
    ):
        metrics.stage_app_metrics(
            country_frame(), pd.DataFrame(), 1, "00", run_id, cutoff, end
        )
        metrics.stage_app_metrics(
            pd.DataFrame(), pd.DataFrame(), 2, "00", run_id, cutoff, end
        )
        with (
            patch.object(
                s3,
                "delete_objects",
                return_value={"Errors": [{"Code": "AccessDenied"}]},
            ),
            patch.object(metrics, "atomic_swap_date_partition_stream") as swap,
            pytest.raises(RuntimeError, match="Failed to delete"),
        ):
            metrics.publish_app_metrics_history(FakeDB(), run_id, end)
        assert swap.call_count == 2
    assert s3.objects


def test_stage_manifest_last_and_pipeline_does_not_publish_after_failure():
    s3 = MemoryS3()
    run_id = "a" * 32
    cutoff, end = datetime.date(2026, 6, 10), datetime.date(2026, 7, 10)
    with patch.object(metrics, "get_s3_client", return_value=s3):
        metrics.stage_app_metrics(
            country_frame(), pd.DataFrame(), 1, "00", run_id, cutoff, end
        )
    assert list(s3.objects)[-1].endswith("manifest.json")
    assert json.loads(list(s3.objects.values())[-1])["tables"]["global"] == {}
    with (
        patch.object(
            metrics, "delete_and_aggregate_s3_agg", side_effect=ValueError("failed")
        ),
        patch.object(metrics, "publish_app_metrics_history") as publish,
    ):
        with pytest.raises(ValueError, match="failed"):
            metrics.run_app_metrics_pipeline(FakeDB())
        publish.assert_not_called()
