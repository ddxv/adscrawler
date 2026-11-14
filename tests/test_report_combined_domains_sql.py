from pathlib import Path

import duckdb


QUERY_PATH = (
    Path(__file__).resolve().parents[1]
    / "adscrawler/dbcon/sql/query_report_combined_domains.sql"
)


def test_combined_domain_sources_keep_domain_and_app_ids_in_order():
    db = duckdb.connect()
    db.execute("CREATE SCHEMA adtech")
    for statement in (
        "CREATE TABLE version_codes (id INTEGER, store_app INTEGER, version_code VARCHAR, updated_at TIMESTAMP, crawl_result INTEGER, created_at TIMESTAMP)",
        "CREATE TABLE version_code_sdk_scan_results (version_code_id INTEGER, scan_result INTEGER)",
        "CREATE TABLE api_calls (store_app INTEGER, tld_url VARCHAR, called_at TIMESTAMP)",
        "CREATE TABLE domains (id INTEGER, domain_name VARCHAR)",
        "CREATE TABLE adtech.app_sdks (store_app INTEGER, version_code_id INTEGER, sdk_id INTEGER)",
        "CREATE TABLE adtech.sdks (id INTEGER, company_id INTEGER)",
        "CREATE TABLE adtech.companies (id INTEGER, domain_id INTEGER)",
        "CREATE TABLE app_ads_entrys (id INTEGER, ad_domain INTEGER, relationship VARCHAR, updated_at TIMESTAMP, created_at TIMESTAMP)",
        "CREATE TABLE app_ads_map (app_ads_entry INTEGER, pub_domain INTEGER, updated_at TIMESTAMP)",
        "CREATE TABLE adstxt_crawl_results (domain_id INTEGER, crawled_at TIMESTAMP, created_at TIMESTAMP)",
        "CREATE TABLE app_urls_map (store_app INTEGER, pub_domain INTEGER, updated_at TIMESTAMP, created_at TIMESTAMP)",
        "CREATE TABLE adtech.company_developers (developer_id INTEGER, company_id INTEGER)",
        "CREATE TABLE store_apps (id INTEGER, developer INTEGER)",
        "CREATE TABLE developer_store_apps (store_app INTEGER, domain_id INTEGER)",
        "CREATE TABLE domains_third_party (domain_id INTEGER)",
    ):
        db.execute(statement)

    for statement in (
        "INSERT INTO domains VALUES (201, 'shared'), (202, 'developer'), (301, 'publisher'), (101, 'reverse')",
        "INSERT INTO developer_store_apps VALUES (101, 201)",
        "INSERT INTO store_apps VALUES (102, 12)",
        "INSERT INTO adtech.company_developers VALUES (12, 2)",
        "INSERT INTO adtech.companies VALUES (1, 201), (2, 202)",
        "INSERT INTO api_calls VALUES (101, 'shared', '2025-02-01'), (201, 'reverse', '2025-02-01')",
        "INSERT INTO version_codes VALUES (1, 101, '1', '2025-02-01', 1, '2025-02-01')",
        "INSERT INTO version_code_sdk_scan_results VALUES (1, 1)",
        "INSERT INTO adtech.app_sdks VALUES (101, 1, 1)",
        "INSERT INTO adtech.sdks VALUES (1, 1)",
        "INSERT INTO app_ads_entrys VALUES (1, 201, 'DIRECT', '2025-02-01', '2025-02-01')",
        "INSERT INTO app_ads_map VALUES (1, 301, '2025-02-01')",
        "INSERT INTO adstxt_crawl_results VALUES (301, '2025-02-01', '2025-02-01')",
        "INSERT INTO app_urls_map VALUES (101, 301, '2025-02-01', '2025-02-01')",
    ):
        db.execute(statement)

    query = QUERY_PATH.read_text()
    for parameter, value in (
        (":start_of_next_period", "'2025-04-01'"),
        (":start_of_period", "'2025-01-01'"),
        (":oldest_allowed_app_ads_crawl", "'2025-01-01'"),
    ):
        query = query.replace(parameter, value)

    rows = set(db.execute(query).fetchall())
    assert rows == {
        (201, 101, True, True, True, True, False),
        (202, 102, False, False, True, False, False),
        (101, 201, False, True, False, False, False),
    }
