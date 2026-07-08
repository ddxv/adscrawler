"""Processing modules that read/write app-store data to/from S3 parquet files.

S3 prefix constants shared across the ``process`` subpackage.
"""

# ---------------------------------------------------------------------------
# Raw-data prefixes – written by scrape stores and read by downstream agg
# ---------------------------------------------------------------------------
RAW_DATA_KEYWORDS = "raw-data/keywords"
RAW_DATA_APP_DETAILS_INCOMING = "raw-data/_incoming/app_details"
RAW_DATA_APP_DETAILS = "raw-data/app_details"
RAW_DATA_APP_RANKINGS = "raw-data/app_rankings"

# ---------------------------------------------------------------------------
# Aggregated app metrics (app_metrics_history.py)
# ---------------------------------------------------------------------------
AGG_APP_HASH_BUCKETS_DAILY = "agg-data/app-hash-daily"
AGG_APP_HASH_BUCKETS_WEEKLY = "agg-data/app-hash-weekly"
AGG_APP_HASH_BUCKETS_FILLED = "agg-data/app-hash-weekly-filled"

# ---------------------------------------------------------------------------
# Domain-app history & change detection (app_domain_history.py)
# ---------------------------------------------------------------------------
AGG_COMBINED_DOMAIN_HISTORY = "agg-data/combined-domain-app-history-quarter"
AGG_STORE_APPS_RELEASE_DATES = "agg-data/store-apps-release-dates"
