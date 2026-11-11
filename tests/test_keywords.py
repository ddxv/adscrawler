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
    df = pd.DataFrame({"store": [1], "store_id": [123], "country": ["US"],
                       "crawled_date": [datetime.date(2026, 7, 10)]})
    keywords.raw_keywords_to_s3(df)

    assert len(uploads) == 1
    bucket, key, payload = uploads[0]
    assert bucket == "bucket"
    assert key.startswith("raw-data/keywords/store=1/crawled_date=2026-07-10/country=US/")
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
            {"crawled_date": "2026-07-10", "country": "US", "store": 1,
             "keyword_id": 5, "rank": 1, "store_id": "old", "crawled_at": "2026-07-10T10:00:00"},
            {"crawled_date": "2026-07-10", "country": "US", "store": 1,
             "keyword_id": 5, "rank": 1, "store_id": "new", "crawled_at": "2026-07-10T11:00:00"},
        ]
    ).to_parquet(path)
    result = keywords.query_keywords_from_s3([str(path)], "s3")
    assert result[["store_id", "app_rank"]].to_dict("records") == [
        {"store_id": "new", "app_rank": 1}
    ]


def test_process_keywords_resolves_missing_apps(monkeypatch):
    db = object()
    monkeypatch.setattr(keywords, "query_countries", lambda _: pd.DataFrame({"alpha2": ["US"], "id": [4]}))
    monkeypatch.setattr(keywords, "query_keywords_from_s3", lambda *_: pd.DataFrame({
        "store_id": ["known", "new"], "country": ["US", "US"],
        "keyword_id": [5, 5], "app_rank": [1, 2], "crawled_date": ["2026-07-10"] * 2,
    }))
    monkeypatch.setattr(keywords, "query_store_id_map_cached", lambda *_: pd.DataFrame({"store_id": ["known"], "id": [10]}))
    monkeypatch.setattr(keywords, "query_store_id_map", lambda *_: pd.DataFrame({"store_id": ["known", "new"], "id": [10, 11]}))
    insert_apps = Mock()
    insert_ranks = Mock()
    monkeypatch.setattr(keywords, "check_and_insert_new_apps", insert_apps)
    monkeypatch.setattr(keywords, "delete_and_insert", insert_ranks)

    keywords.process_keywords(["file.parquet"], "s3", db, 2)

    assert insert_apps.call_args.kwargs["crawl_source"] == "keywords"
    assert insert_apps.call_args.kwargs["store"] == 2
    args = insert_ranks.call_args.kwargs
    assert args["table_name"] == "app_keyword_ranks_daily"
    assert args["delete_by_keys"] == ["crawled_date", "store"]
    assert args["df"][["store_app", "country", "store"]].to_dict("records") == [
        {"store_app": 10, "country": 4, "store": 2},
        {"store_app": 11, "country": 4, "store": 2},
    ]


def test_import_keywords_records_success_and_failure(monkeypatch):
    monkeypatch.setattr(keywords, "CONFIG", {"s3": {"bucket": "bucket"}})
    monkeypatch.setattr(keywords, "get_parquet_paths_by_prefix", lambda bucket, prefix: [prefix])
    monkeypatch.setattr(keywords, "filter_unprocessed_s3_files", lambda paths, **_: paths)
    process = Mock(side_effect=[None, ValueError("bad parquet")])
    record = Mock()
    monkeypatch.setattr(keywords, "process_keywords", process)
    monkeypatch.setattr(keywords, "record_s3_file_status", record)
    db = object()

    keywords.import_keywords_from_s3(datetime.date(2026, 7, 10), datetime.date(2026, 7, 10), db)

    assert [call.kwargs["store"] for call in process.call_args_list] == [1, 2]
    assert [call.kwargs["status"] for call in record.call_args_list] == ["completed", "failed"]
    assert record.call_args_list[1].kwargs["error_message"] == "bad parquet"
