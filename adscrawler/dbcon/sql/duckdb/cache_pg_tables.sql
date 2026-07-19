INSTALL postgres;
LOAD postgres;
ATTACH '{db_uri}' AS pg (TYPE postgres);

CREATE TEMP TABLE domains_cache AS SELECT id, domain_name FROM pg.domains;

CREATE TEMP TABLE companies_cache AS SELECT id, domain_id FROM pg.adtech.companies;

CREATE TEMP TABLE company_domain_mapping_cache AS SELECT company_id, domain_id FROM pg.adtech.company_domain_mapping;

CREATE TEMP TABLE store_app_store AS SELECT id, store, release_date FROM read_parquet($store_apps_key);

PRAGMA temp_directory='/tmp/duckdb_temp/';
PRAGMA max_memory='8GB';

