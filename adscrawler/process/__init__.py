"""Processing modules that read/write app-store data to/from S3 parquet files.

S3 prefix constants shared across the ``process`` subpackage.
"""

# ---------------------------------------------------------------------------
# Raw-data prefixes – written by scrape stores and read by downstream agg
# ---------------------------------------------------------------------------
RAW_DATA_KEYWORDS = "raw-data/keywords"
RAW_DATA_APP_DETAILS_INCOMING = "raw-data/_incoming/app_details"
RAW_DATA_VERSION_DETAILS_INCOMING = "raw-data/_incoming/version-details-map"
RAW_DATA_APP_DETAILS = "raw-data/app_details"
RAW_DATA_VERSION_DETAILS = "raw-data/version-details-map"
RAW_DATA_APP_RANKINGS = "raw-data/app_rankings"

RAW_DATA_VERSION_DETAILS_INITIAL = "raw-data/initial-version-details-map"

AGG_VERSION_DETAILS = "agg-data/version-details-map"
TMP_VERSION_DETAILS = "tmp/version-details-map"
AGG_PATTERN_MATCHES = "agg-data/pattern-matches"
TMP_PATTERN_MATCHES = "tmp/pattern-matches"
AGG_MATCHED_SDKS = "agg-data/matched-sdks"
TMP_MATCHED_SDKS = "tmp/matched-sdks"


LOOKUP_VERSION_STRINGS = "lookups/version-strings/version-strings.parquet"
LOOKUP_SDK_PACKAGE_PATTERNS = "lookups/adtech-sdk-packages/sdk-packages.parquet"
LOOKUP_SDK_PATH_PATTERNS = "lookups/adtech-sdk-paths/sdk-paths.parquet"
LOOKUP_SDK_MEDIATION_PATTERNS = "lookups/adtech-sdk-mediation/sdk-mediation.parquet"
LOOKUP_VERSION_CODES = "lookups/version-codes/version-codes.parquet"


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
LOOKUP_STORE_APPS_RELEASE_DATES = "lookups/store-apps-release-dates"
