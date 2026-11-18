import datetime
from unittest.mock import Mock

import duckdb

from adscrawler.process import app_domain_history as history


def test_alltime_publish_streams_adapter_flag(tmp_path, monkeypatch):
    con = duckdb.connect()
    parquet_path = tmp_path / "history.parquet"
    con.execute(f"""COPY (
            SELECT 123 AS domain_id, 456 AS store_app,
                true AS sdk, true AS sdk_adapter, false AS api_call,
                false AS publisher, false AS app_ads_direct,
                false AS app_ads_reseller
        ) TO '{parquet_path}' (FORMAT PARQUET)""")
    con.execute(
        "CREATE TABLE company_domain_mapping_cache (domain_id INTEGER, company_id INTEGER)"
    )
    con.execute("INSERT INTO company_domain_mapping_cache VALUES (123, 789)")
    calls = []

    def stream(query, setup_sql):
        calls.append(setup_sql)
        return con.execute(
            query.replace(
                f"s3://test-bucket/{history.AGG_COMBINED_DOMAIN_HISTORY_ALLTIME}/*.parquet",
                str(parquet_path),
            )
        ).fetchall()

    swap = Mock()
    monkeypatch.setattr(history, "CONFIG", {"s3": {"bucket": "test-bucket"}})
    monkeypatch.setattr(history, "pg_db_uri", lambda: "unused")
    monkeypatch.setattr(history, "stream_duckdb_tsv", stream)
    monkeypatch.setattr(history, "atomic_swap_partition_stream", swap)

    history.combined_domain_latest_to_db(Mock(), datetime.date(2025, 4, 1))

    kwargs = swap.call_args.kwargs
    assert kwargs["columns"] == [
        "domain_id",
        "store_app",
        "company_id",
        "sdk",
        "sdk_adapter",
        "api_call",
        "publisher",
        "app_ads_direct",
        "app_ads_reseller",
        "batch_date",
    ]
    assert kwargs["stream"] == [
        (
            123,
            456,
            789,
            True,
            True,
            False,
            False,
            False,
            False,
            datetime.date(2025, 4, 1),
        )
    ]
    assert len(calls) == 1
    con.close()
