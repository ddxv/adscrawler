import atexit
import socket

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from prometheus_client import Counter

from adscrawler.config import get_logger

logger = get_logger(__name__)

_provider: MeterProvider | None = None


def _flush_and_shutdown() -> None:
    """Flush pending metrics and close the provider on process exit."""
    global _provider
    if _provider:
        try:
            # Force flush and shut down gRPC connection
            _provider.shutdown(timeout_millis=5000)
            logger.debug("OTLP metrics successfully flushed to Alloy.")
        except Exception as e:
            logger.error("Failed to flush OTLP metrics on exit: %s", e)
        finally:
            _provider = None


def init_metrics(job_name: str) -> None:
    """Initialize OpenTelemetry metric export to local Alloy agent."""
    global _provider
    if _provider is not None:
        return  # Prevent re-initialization

    hostname = socket.gethostname()

    resource = Resource.create(
        {
            "service.name": "app_pipeline_cron",
            "job.name": job_name,
            "service.instance.id": hostname,
            "host.name": hostname,
        }
    )

    exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True)

    # Configure reader with reasonable export defaults
    reader = PeriodicExportingMetricReader(
        exporter,
        export_interval_millis=10000,  # Flush every 10s if long-running
    )

    _provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(_provider)

    # Register exit hook automatically
    atexit.register(_flush_and_shutdown)


# HANDLED BY DRAMATIQ PROMETHEUS CLIENT
CRAWL_RESULTS_COUNTER = Counter(
    name="app_crawl_results_total",
    documentation="Total number of app crawls processed by store and outcome",
    labelnames=["store", "crawl_result"],
)


meter = metrics.get_meter("app.pipeline.metrics")

# Hanlded by OTEL
CRAWL_BACKLOG_GAUGE = meter.create_gauge(
    "app_crawl_backlog_total",
    description="Total apps meeting crawl criteria in SQL queue",
)

DOWNLOAD_BACKLOG_GAUGE = meter.create_gauge(
    "download_backlog_total",
    description="Total downloads pending",
)

SDK_SCAN_BACKLOG_GAUGE = meter.create_gauge(
    "sdk_scan_backlog_total",
    description="Total sdk scan pending",
)

WAYDROID_RUN_BACKLOG_GAUGE = meter.create_gauge(
    "waydroid_backlog_total",
    description="Total waydroid runs pending",
)

# --- Counters ---
DOWNLOAD_RESULTS_COUNTER = meter.create_counter(
    "download_results_total",
    description="Total downloads",
)

SDK_SCAN_RESULTS_COUNTER = meter.create_counter(
    "sdk_scan_results_total",
    description="Total sdk scan results",
)

WAYDROID_RUN_RESULTS_COUNTER = meter.create_counter(
    "waydroid_results_total",
    description="Total waydroid run results",
)

# --- Backlog gauges: app-ads.txt / keyword pipelines ---
ADS_TXT_BACKLOG_GAUGE = meter.create_gauge(
    "app_ads_txt_backlog_total",
    description="Total pub domains queued for app-ads.txt crawl",
)

PROCESS_KEYWORDS_BACKLOG_GAUGE = meter.create_gauge(
    "process_keywords_backlog_total",
    description="Total apps queued for keyword extraction",
)

CRAWL_KEYWORDS_BACKLOG_GAUGE = meter.create_gauge(
    "crawl_keywords_backlog_total",
    description="Total keywords queued for rank crawl",
)

# --- Result counters: app-ads.txt / keyword pipelines ---
ADS_TXT_RESULTS_COUNTER = meter.create_counter(
    "app_ads_txt_results_total",
    description="Total app-ads.txt crawl results by outcome",
)

PROCESS_KEYWORDS_RESULTS_COUNTER = meter.create_counter(
    "process_keywords_results_total",
    description="Total apps processed for keyword extraction",
)

CRAWL_KEYWORDS_RESULTS_COUNTER = meter.create_counter(
    "crawl_keywords_results_total",
    description="Total keywords crawled for ranks by outcome",
)
