from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from adscrawler.app_stores import scrape_stores
from adscrawler.dbcon import queries
from main import ProcessManager


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


def test_scrape_developer_name_uses_name_only_when_canonical_id_matches(monkeypatch):
    monkeypatch.setattr(
        scrape_stores,
        "scrape_app_gp",
        lambda *args, **kwargs: {
            "developerId": "pub-123",
            "developer": "New Name",
        },
    )

    name = scrape_stores._scrape_developer_name(
        store=1,
        developer_id="pub-123",
        store_id="com.example.app",
    )

    assert name == "New Name"


def test_scrape_developer_name_rejects_reassigned_app(monkeypatch):
    monkeypatch.setattr(
        scrape_stores,
        "scrape_app_gp",
        lambda *args, **kwargs: {
            "developerId": "pub-new-owner",
            "developer": "Wrong Owner",
        },
    )

    with pytest.raises(ValueError, match="belongs to developer"):
        scrape_stores._scrape_developer_name(
            store=1,
            developer_id="pub-old-owner",
            store_id="com.example.app",
        )


def test_refresh_developer_names_updates_in_batches_by_store_and_developer_id(
    monkeypatch,
):
    developers = pd.DataFrame(
        [
            {
                "id": 4,
                "store": 2,
                "developer_id": "artist-42",
                "name": "Old Name",
                "store_id": "101",
            }
        ]
    )
    query = Mock(side_effect=[developers, developers.iloc[0:0]])
    update = Mock()
    monkeypatch.setattr(scrape_stores, "query_developer_names_to_refresh", query)
    monkeypatch.setattr(
        scrape_stores, "_scrape_developer_name", lambda **kwargs: "New Name"
    )
    monkeypatch.setattr(scrape_stores, "update_from_df", update)

    checked = scrape_stores.refresh_developer_names(
        pgdb=object(),
        store=2,
        batch_size=25,
    )

    assert checked == 1
    update.assert_called_once()
    updated_df = update.call_args.kwargs["df"]
    assert updated_df.to_dict(orient="records") == [
        {"store": 2, "developer_id": "artist-42", "name": "New Name"}
    ]
    assert update.call_args.kwargs["key_columns"] == ["store", "developer_id"]
    assert update.call_args.kwargs["update_columns"] == ["name"]
    assert [call.kwargs["after_id"] for call in query.call_args_list] == [0, 4]


def test_refresh_developer_names_preserves_name_when_store_omits_it(monkeypatch):
    developers = pd.DataFrame(
        [
            {
                "id": 4,
                "store": 2,
                "developer_id": "artist-42",
                "name": "Existing Name",
                "store_id": "101",
            }
        ]
    )
    query = Mock(side_effect=[developers, developers.iloc[0:0]])
    update = Mock()
    monkeypatch.setattr(scrape_stores, "query_developer_names_to_refresh", query)
    monkeypatch.setattr(scrape_stores, "_scrape_developer_name", lambda **kwargs: None)
    monkeypatch.setattr(scrape_stores, "update_from_df", update)

    scrape_stores.refresh_developer_names(pgdb=object(), store=2)

    updated_df = update.call_args.kwargs["df"]
    assert updated_df.loc[0, "name"] == "Existing Name"


def test_new_apps_check_devs_runs_name_refresh_for_each_store(monkeypatch):
    manager = ProcessManager.__new__(ProcessManager)
    manager.pgcon = object()
    crawl = Mock()
    refresh = Mock(return_value=1)
    init_metrics = Mock()
    monkeypatch.setattr("main.crawl_developers_for_new_store_ids", crawl)
    monkeypatch.setattr("main.refresh_developer_names", refresh)
    monkeypatch.setattr("main.init_metrics", init_metrics)

    manager.scrape_new_apps_devs()

    init_metrics.assert_called_once_with(job_name="crawl_developers")
    assert [call.kwargs["store"] for call in crawl.call_args_list] == [1, 2]
    assert [call.kwargs["store"] for call in refresh.call_args_list] == [1, 2]
