from prometheus_client import Counter, Histogram, Gauge, start_http_server
import logging

logger = logging.getLogger(__name__)

# Counters
records_processed = Counter(
    "cdc_records_processed_total",
    "Total CDC records processed",
    ["topic", "operation"],
)

records_valid = Counter(
    "cdc_records_valid_total",
    "Total valid CDC records",
    ["topic"],
)

records_invalid = Counter(
    "cdc_records_invalid_total",
    "Total invalid CDC records sent to DLQ",
    ["topic", "check_name"],
)

records_dlq = Counter(
    "cdc_records_dlq_total",
    "Total records sent to DLQ",
    ["topic"],
)

# Gauges
data_quality_score = Gauge(
    "cdc_data_quality_score",
    "Current data quality score (0-1)",
    ["topic"],
)

# Histograms
processing_latency = Histogram(
    "cdc_processing_latency_seconds",
    "Time to validate and route a record",
    ["topic"],
)


class QualityTracker:
    """Track data quality score per topic using a sliding window."""

    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.results: dict[str, list[bool]] = {}

    def record(self, topic: str, is_valid: bool):
        if topic not in self.results:
            self.results[topic] = []

        self.results[topic].append(is_valid)

        # Keep only last N results
        if len(self.results[topic]) > self.window_size:
            self.results[topic] = self.results[topic][-self.window_size:]

        # Update gauge
        score = self.get_score(topic)
        data_quality_score.labels(topic=topic).set(score)

    def get_score(self, topic: str) -> float:
        results = self.results.get(topic, [])
        if not results:
            return 1.0
        return sum(results) / len(results)


def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics HTTP server."""
    start_http_server(port)
    logger.info(f"Metrics server started on port {port}")
