# main.py

import logging
import sys
from config import AppConfig, KafkaConfig, TopicConfig, AlertConfig
from kafkaconsumer import CDCQualityConsumer
from metrics import start_metrics_server

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("cdc_quality.log"),
    ],
)
logger = logging.getLogger(__name__)


def main():
    # ──────────────────────────────────────────────
    # Configuration
    # ──────────────────────────────────────────────
    config = AppConfig(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            consumer_group="data-quality-consumer",
        ),
        topics=TopicConfig(
            source_topics=[
                "cdc.public.orders",
                "cdc.public.customers",
            ],
            downstream_topics={
                "cdc.public.orders": "validated.orders",
                "cdc.public.customers": "validated.customers",
            },
            dlq_topic="data-quality-dlq",
        ),
        alerts=AlertConfig(
            slack_webhook_url="",  # Add your Slack webhook URL
            alert_threshold=50,
            alert_window_seconds=300,
        ),
    )

    # ──────────────────────────────────────────────
    # Start Prometheus metrics server
    # ──────────────────────────────────────────────
    start_metrics_server(port=8000)
    print("heloo #########################################")
    logger.info("Prometheus metrics available at http://localhost:8000/metrics")

    # ──────────────────────────────────────────────
    # Start consumer
    # ──────────────────────────────────────────────
    consumer = CDCQualityConsumer(config)
    consumer.start()


if __name__ == "__main__":
    main()