import datetime
from contextlib import contextmanager
from io import BytesIO
from unittest.mock import Mock

import duckdb
import pandas as pd

from adscrawler.process import keywords


def test_raw_keywords_to_s3_uploads_partitioned_parquet(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    uploads = []

    class Client:
        def upload_fileobj(self, buffer, bucket, key):
            uploads.append((bucket, key, buffer.read()))

    monkeypatch.setattr(keywords, "get_s3_client", Client)
    df = pd.DataFrame(
        {
            "store": [1],
            "store_id": [123],
            "country": ["US"],
            "crawled_date": [datetime.date(2026, 7, 10)],
        }
    )
    keywords.raw_keywords_to_s3(df)

    assert len(uploads) == 1
    bucket, key, payload = uploads[0]
    assert bucket == "bucket"
    assert key.startswith(
        "raw-data/keywords/store=1/crawled_date=2026-07-10/country=US/"
    )
    assert pd.read_parquet(BytesIO(payload))["store_id"].tolist() == ["123"]


def test_query_keywords_from_s3_selects_latest_rank(tmp_path, monkeypatch):
    @contextmanager
    def connection(_):
        with duckdb.connect() as con:
            yield con

    monkeypatch.setattr(keywords, "get_duckdb_connection", connection)
    path = tmp_path / "keywords.parquet"
    pd.DataFrame(
        [
            {
                "crawled_date": "2026-07-10",
                "country": "US",
                "store": 1,
                "keyword_id": 5,
                "rank": 1,
                "store_id": "old",
                "crawled_at": "2026-07-10T10:00:00",
            },
            {
                "crawled_date": "2026-07-10",
                "country": "US",
                "store": 1,
                "keyword_id": 5,
                "rank": 1,
                "store_id": "new",
                "crawled_at": "2026-07-10T11:00:00",
            },
        ]
    ).to_parquet(path)
    result = keywords.query_keywords_from_s3([str(path)], "s3")
    assert result[["store_id", "app_rank"]].to_dict("records") == [
        {"store_id": "new", "app_rank": 1}
    ]


def test_process_keywords_resolves_missing_apps(monkeypatch):
    db = object()
    monkeypatch.setattr(
        keywords,
        "query_countries",
        lambda _: pd.DataFrame({"alpha2": ["US"], "id": [4]}),
    )
    monkeypatch.setattr(
        keywords,
        "query_keywords_from_s3",
        lambda *_: pd.DataFrame(
            {
                "store_id": ["known", "new"],
                "country": ["US", "US"],
                "keyword_id": [5, 5],
                "app_rank": [1, 2],
                "crawled_date": ["2026-07-10"] * 2,
            }
        ),
    )
    monkeypatch.setattr(
        keywords,
        "query_store_id_map_cached",
        lambda *_: pd.DataFrame({"store_id": ["known"], "id": [10]}),
    )
    monkeypatch.setattr(
        keywords,
        "query_store_id_map",
        lambda *_: pd.DataFrame({"store_id": ["known", "new"], "id": [10, 11]}),
    )
    insert_apps = Mock()
    monkeypatch.setattr(keywords, "check_and_insert_new_apps", insert_apps)

    result = keywords.process_keywords(["file.parquet"], "s3", db, 2)

    assert insert_apps.call_args.kwargs["crawl_source"] == "keywords"
    assert insert_apps.call_args.kwargs["store"] == 2
    assert result[["store_app", "country", "store"]].to_dict("records") == [
        {"store_app": 10, "country": 4, "store": 2},
        {"store_app": 11, "country": 4, "store": 2},
    ]


def keyword_frame(store, date="2026-07-10", country=4):
    return pd.DataFrame(
        [
            {
                "crawled_date": date,
                "store": store,
                "country": country,
                "keyword_id": 5,
                "store_app": store + 10,
                "app_rank": 1,
            }
        ]
    )


def test_import_keywords_rebuilds_all_stores_when_any_file_unprocessed(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords,
        "get_parquet_paths_by_prefix",
        lambda bucket, prefix: [prefix + "old", prefix + "new"],
    )
    process = Mock(side_effect=lambda paths, config, db, store: keyword_frame(store))
    swap = Mock()
    record = Mock()
    unprocessed = Mock(side_effect=lambda paths, **_: [paths[-1]])
    monkeypatch.setattr(keywords, "filter_unprocessed_s3_files", unprocessed)
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    db = object()
    date = datetime.date(2026, 7, 10)

    keywords.import_keywords_from_s3(date, date, db)

    assert [call.args[3] for call in process.call_args_list] == [1, 2]
    assert all(len(call.args[0]) == 2 for call in process.call_args_list)
    assert len(unprocessed.call_args.args[0]) == 4
    assert unprocessed.call_args.kwargs == {
        "pipeline_name": "import_keywords_from_s3_v2",
        "pgdb": db,
    }
    assert swap.call_count == 1
    assert set(swap.call_args.args[0]["store"]) == {1, 2}
    assert swap.call_args.kwargs == {
        "schema": "frontend",
        "table": "app_keyword_ranks_daily",
        "partition_prefix": "app_keyword_ranks_daily",
        "partition_date": date,
    }
    assert record.call_args.kwargs["status"] == "completed"
    assert len(record.call_args.kwargs["file_path"]) == 4


def test_import_keywords_skips_dates_with_only_completed_files(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: [prefix]
    )
    unprocessed = Mock(return_value=[])
    process = Mock()
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "filter_unprocessed_s3_files", unprocessed)
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    start = datetime.date(2026, 7, 10)
    keywords.import_keywords_from_s3(
        start, start + datetime.timedelta(days=4), object()
    )
    assert unprocessed.call_count == 5
    process.assert_not_called()
    swap.assert_not_called()
    record.assert_not_called()


def test_import_keywords_missing_store_uses_available_files(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords,
        "get_parquet_paths_by_prefix",
        lambda bucket, prefix: [prefix] if "store=1/" in prefix else [],
    )
    monkeypatch.setattr(
        keywords, "filter_unprocessed_s3_files", lambda paths, **_: paths
    )
    process = Mock(return_value=keyword_frame(1))
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    date = datetime.date(2026, 7, 10)
    keywords.import_keywords_from_s3(date, date, object())
    process.assert_called_once()
    swap.assert_called_once()
    assert swap.call_args.args[0]["store"].tolist() == [1]
    assert record.call_args.kwargs["status"] == "completed"


def test_import_keywords_failure_never_swaps_partial_date(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: [prefix]
    )
    monkeypatch.setattr(
        keywords, "filter_unprocessed_s3_files", lambda paths, **_: paths
    )
    process = Mock(side_effect=[keyword_frame(1), ValueError("bad parquet")])
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    date = datetime.date(2026, 7, 10)
    keywords.import_keywords_from_s3(date, date, object())
    swap.assert_not_called()
    assert record.call_args.kwargs["status"] == "failed"
    assert record.call_args.kwargs["error_message"] == "bad parquet"
    assert len(record.call_args.kwargs["file_path"]) == 2


def test_import_keywords_retries_failed_date_and_skips_after_success(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: [prefix]
    )
    statuses = {}

    def filter_files(paths, **kwargs):
        return [path for path in paths if statuses.get(path) != "completed"]

    def record_status(**kwargs):
        statuses.update({path: kwargs["status"] for path in kwargs["file_path"]})

    process = Mock(
        side_effect=[
            keyword_frame(1),
            ValueError("bad parquet"),
            keyword_frame(1),
            keyword_frame(2),
        ]
    )
    swap = Mock()
    monkeypatch.setattr(keywords, "filter_unprocessed_s3_files", filter_files)
    monkeypatch.setattr(keywords, "record_s3_file_status", record_status)
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    date = datetime.date(2026, 7, 10)
    db = object()

    keywords.import_keywords_from_s3(date, date, db)
    assert set(statuses.values()) == {"failed"}
    swap.assert_not_called()
    keywords.import_keywords_from_s3(date, date, db)
    assert set(statuses.values()) == {"completed"}
    swap.assert_called_once()
    keywords.import_keywords_from_s3(date, date, db)
    assert process.call_count == 4
    swap.assert_called_once()


def test_import_keywords_empty_store_does_not_swap_other_store(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: [prefix]
    )
    monkeypatch.setattr(
        keywords, "filter_unprocessed_s3_files", lambda paths, **_: paths
    )
    process = Mock(side_effect=[keyword_frame(1), keyword_frame(2).iloc[0:0]])
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    date = datetime.date(2026, 7, 10)
    keywords.import_keywords_from_s3(date, date, object())
    swap.assert_not_called()
    assert record.call_args.kwargs["status"] == "failed"


def test_import_keywords_invalid_or_empty_data_does_not_swap(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords,
        "get_parquet_paths_by_prefix",
        lambda bucket, prefix: [prefix] if "store=1/" in prefix else [],
    )
    monkeypatch.setattr(
        keywords, "filter_unprocessed_s3_files", lambda paths, **_: paths
    )
    process = Mock()
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    date = datetime.date(2026, 7, 10)
    for frame in [
        keyword_frame(1, country=None),
        keyword_frame(1, date="2026-07-11"),
        keyword_frame(1).iloc[0:0],
    ]:
        process.return_value = frame
        keywords.import_keywords_from_s3(date, date, object())
    swap.assert_not_called()
    assert [call.kwargs["status"] for call in record.call_args_list] == ["failed"] * 3


def test_import_keywords_no_files_does_not_swap(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(
        keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: []
    )
    swap = Mock()
    record = Mock()
    monkeypatch.setattr(keywords, "atomic_swap_date_partition", swap)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    date = datetime.date(2026, 7, 10)
    keywords.import_keywords_from_s3(date, date, object())
    swap.assert_not_called()
    record.assert_not_called()
