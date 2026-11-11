import datetime
from contextlib import contextmanager
from unittest.mock import Mock

import duckdb
import pandas as pd
import pytest

from adscrawler.process import app_rankings


@contextmanager
def duckdb_connection(_):
    with duckdb.connect() as con:
        yield con


def test_query_ranks_supports_legacy_and_timestamped_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(app_rankings, "get_duckdb_connection", duckdb_connection)
    base = {"country": "US", "collection": "top", "category": "games", "rank": 1}
    legacy = tmp_path / "legacy.parquet"
    newer = tmp_path / "newer.parquet"
    pd.DataFrame(
        [{**base, "crawled_date": datetime.date(2025, 1, 6), "store_id": "old"}]
    ).to_parquet(legacy)
    pd.DataFrame(
        [
            {
                **base,
                "crawled_date": datetime.date(2025, 1, 8),
                "store_id": "a",
                "crawled_at": "2025-01-08T10:00:00Z",
            },
            {
                **base,
                "crawled_date": datetime.date(2025, 1, 8),
                "store_id": "b",
                "crawled_at": "2025-01-08T11:00:00Z",
            },
        ]
    ).to_parquet(newer)
    result = app_rankings.query_store_collection_ranks(
        [str(legacy), str(newer)], "week", "s3"
    )
    assert set(result.store_id) == {"a", "b"}
    assert set(pd.to_datetime(result.crawled_date).dt.date) == {
        datetime.date(2025, 1, 6)
    }
    assert (
        result.set_index("store_id").loc["b", "source_crawled_at"]
        > result.set_index("store_id").loc["a", "source_crawled_at"]
    )


def test_import_swaps_once_per_date_after_combining_stores(monkeypatch):
    monkeypatch.setattr(
        app_rankings, "CONFIG", {"s3": {"bucket": "test", "region_name": "test"}}
    )

    def row(store_app, source_date, source_time, rank):
        return {
            "crawled_date": datetime.date(2025, 1, 6),
            "source_crawled_date": datetime.date.fromisoformat(source_date),
            "source_crawled_at": pd.Timestamp(source_time),
            "country": 1,
            "store_collection": 1,
            "store_category": 1,
            "rank": rank,
            "best_rank": rank,
            "store_app": store_app,
        }

    frames = {
        1: pd.DataFrame(
            [
                row(1, "2025-01-07", "2025-01-07T10:00Z", 1),
                row(2, "2025-01-08", "2025-01-08T09:00Z", 1),
            ]
        ),
        2: pd.DataFrame(
            [
                row(3, "2025-01-08", "2025-01-08T11:00Z", 1),
                row(4, "2025-01-08", "2025-01-08T11:00Z", 2),
            ]
        ),
    }
    monkeypatch.setattr(
        app_rankings, "process_ranks_from_s3", lambda store, **kwargs: frames[store]
    )
    swap = Mock()
    monkeypatch.setattr(app_rankings, "atomic_swap_date_partition", swap)
    cleanup = Mock()
    monkeypatch.setattr(app_rankings, "clean_app_ranks_weekly_table", cleanup)
    db = object()
    app_rankings.import_ranks_from_s3(
        datetime.date(2025, 1, 6), datetime.date(2025, 1, 6), db, "week"
    )
    swap.assert_called_once()
    cleanup.assert_called_once_with(db)
    df = swap.call_args.args[0]
    assert set(df.store_app) == {3, 4}
    assert "source_crawled_at" not in df.columns
    assert swap.call_args.kwargs["partition_date"] == datetime.date(2025, 1, 6)


def test_import_cleans_without_new_data(monkeypatch):
    monkeypatch.setattr(
        app_rankings, "CONFIG", {"s3": {"bucket": "test", "region_name": "test"}}
    )
    monkeypatch.setattr(
        app_rankings, "process_ranks_from_s3", lambda **kwargs: pd.DataFrame()
    )
    cleanup = Mock()
    monkeypatch.setattr(app_rankings, "clean_app_ranks_weekly_table", cleanup)
    db = object()
    app_rankings.import_ranks_from_s3(
        datetime.date(2025, 1, 6), datetime.date(2025, 1, 6), db, "day"
    )
    cleanup.assert_called_once_with(db)


def test_missing_required_values_do_not_swap(monkeypatch):
    monkeypatch.setattr(
        app_rankings, "CONFIG", {"s3": {"bucket": "test", "region_name": "test"}}
    )
    row = {
        "crawled_date": datetime.date(2025, 1, 6),
        "source_crawled_date": datetime.date(2025, 1, 6),
        "source_crawled_at": pd.NaT,
        "country": None,
        "store_collection": 1,
        "store_category": 1,
        "rank": 1,
        "best_rank": 1,
        "store_app": 1,
    }
    monkeypatch.setattr(
        app_rankings, "process_ranks_from_s3", lambda **kwargs: pd.DataFrame([row])
    )
    swap = Mock()
    monkeypatch.setattr(app_rankings, "atomic_swap_date_partition", swap)
    with pytest.raises(ValueError, match="missing required values"):
        app_rankings.import_ranks_from_s3(
            datetime.date(2025, 1, 6), datetime.date(2025, 1, 6), object(), "day"
        )
    swap.assert_not_called()


def test_daily_reads_only_one_date(monkeypatch):
    paths = Mock(return_value=[])
    monkeypatch.setattr(app_rankings, "get_s3_rank_parquet_paths", paths)
    result = app_rankings.process_ranks_from_s3(
        1, datetime.date(2025, 1, 6), object(), "day", "s3"
    )
    assert result.empty
    assert paths.call_args.args[2] == 0
