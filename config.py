# config.py

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "data-quality-consumer"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False


@dataclass
class TopicConfig:
    # Debezium source topics (auto-created by Debezium as: {topic.prefix}.{schema}.{table})
    source_topics: List[str] = field(default_factory=lambda: [
        "cdc.public.orders",
        "cdc.public.customers",
    ])
    # Validated records go here
    downstream_topics: Dict[str, str] = field(default_factory=lambda: {
        "cdc.public.orders": "validated.orders",
        "cdc.public.customers": "validated.customers",
    })
    # Dead letter queue topic
    dlq_topic: str = "data-quality-dlq"


@dataclass
class AlertConfig:
    slack_webhook_url: str = ""  # Set your Slack webhook
    alert_threshold: int = 50   # Alert after N failures in window
    alert_window_seconds: int = 300  # 5-minute window


@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    topics: TopicConfig = field(default_factory=TopicConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
