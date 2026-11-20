from threading import current_thread, main_thread
from types import SimpleNamespace

import pandas as pd

import adscrawler.scrape as scrape


def test_crawl_app_ads_uses_thread_workers_and_shared_database_engine(monkeypatch):
    pgdb = object()
    observed = {}

    monkeypatch.setattr(
        scrape,
        "query_pub_domains_to_crawl_ads_txt",
        lambda **kwargs: pd.DataFrame([{"url": "https://example.com", "id": 1}]),
    )

    def fake_scrape_app_ads_url(url, domain_id, pgdb):
        observed["thread"] = current_thread()
        observed["pgdb"] = pgdb
        return {"crawl_result": 1}

    monkeypatch.setattr(scrape, "scrape_app_ads_url", fake_scrape_app_ads_url)
    monkeypatch.setattr(
        scrape,
        "ADS_TXT_RESULTS_COUNTER",
        SimpleNamespace(add=lambda *args, **kwargs: None),
    )

    scrape.crawl_app_ads(pgdb=pgdb, workers=1)

    assert observed["thread"] is not main_thread()
    assert observed["pgdb"] is pgdb
