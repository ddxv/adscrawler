from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from adscrawler.app_stores import scrape_stores
from adscrawler.dbcon import queries


def test_query_developers_reports_full_backlog_and_hides_internal_column(monkeypatch):
    result = pd.DataFrame(
        {
            "id": [1],
            "developer_id": ["dev-1"],
            "total_queue_depth": [12],
        }
    )
    log_backlog = Mock()
    monkeypatch.setattr(queries.pd, "read_sql", lambda *args, **kwargs: result)
    monkeypatch.setattr(queries, "log_developers_query", log_backlog)

    developers = queries.query_developers(
        pgdb=SimpleNamespace(engine=object()),
        store=1,
        limit=1,
    )

    log_backlog.assert_called_once_with(store=1, total_backlog=12)
    assert "total_queue_depth" not in developers.columns


def test_query_developers_reports_zero_backlog_when_empty(monkeypatch):
    result = pd.DataFrame(columns=["id", "developer_id", "total_queue_depth"])
    log_backlog = Mock()
    monkeypatch.setattr(queries.pd, "read_sql", lambda *args, **kwargs: result)
    monkeypatch.setattr(queries, "log_developers_query", log_backlog)

    developers = queries.query_developers(
        pgdb=SimpleNamespace(engine=object()),
        store=2,
    )

    log_backlog.assert_called_once_with(store=2, total_backlog=0)
    assert developers.empty


def test_crawl_developers_counts_successfully_logged_developers(monkeypatch):
    developers = pd.DataFrame({"id": [1], "developer_id": ["dev-1"]})
    results_counter = Mock()
    monkeypatch.setattr(scrape_stores, "query_store_ids", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        scrape_stores, "query_developers", lambda *args, **kwargs: developers
    )
    monkeypatch.setattr(
        scrape_stores,
        "crawl_google_developers",
        lambda *args, **kwargs: pd.DataFrame(columns=["store", "store_id"]),
    )
    monkeypatch.setattr(scrape_stores, "upsert_df", Mock())
    monkeypatch.setattr(
        scrape_stores, "CRAWL_DEVELOPERS_RESULTS_COUNTER", results_counter
    )

    scrape_stores.crawl_developers_for_new_store_ids(pgdb=object(), store=1)

    results_counter.add.assert_called_once_with(1, attributes={"store": "1"})
