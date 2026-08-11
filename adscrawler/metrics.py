from prometheus_client import CollectorRegistry, Counter, Gauge

from adscrawler.config import get_logger

logger = get_logger(__name__)

registry = CollectorRegistry()


CRAWL_RESULTS_COUNTER = Counter(
    name="app_crawl_results_total",
    documentation="Total number of app crawls processed by store and outcome",
    labelnames=["store", "crawl_result"],
)


CRAWL_BACKLOG_GAUGE = Gauge(
    "app_crawl_backlog_total",
    "Total apps meeting crawl criteria in SQL queue",
    labelnames=["store", "country_priority_group"],
    registry=registry,
)

DOWNLOAD_BACKLOG_GAUGE = Gauge(
    "download_backlog_total",
    "Total downloads pending",
    labelnames=["store"],
    registry=registry,
)

DOWNLOAD_RESULTS_COUNTER = Counter(
    "download_results_total",
    "Total downloads",
    labelnames=["store", "download_result"],
    registry=registry,
)


SDK_SCAN_BACKLOG_GAUGE = Gauge(
    "sdk_scan_backlog_total",
    "Total sdk scan pending",
    labelnames=["store"],
    registry=registry,
)

SDK_SCAN_RESULTS_COUNTER = Counter(
    "sdk_scan_results_total",
    "Total sdk scan results",
    labelnames=["store", "scan_result"],
    registry=registry,
)


WAYDROID_RUN_BACKLOG_GAUGE = Gauge(
    "waydroid_backlog_total",
    "Total waydroid runs pending",
    labelnames=["run_name"],
    registry=registry,
)

WAYDROID_RUN_RESULTS_COUNTER = Counter(
    "waydroid_results_total",
    "Total waydroid run results",
    labelnames=["run_name", "run_result"],
    registry=registry,
)
