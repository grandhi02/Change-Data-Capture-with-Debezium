import json
import time
import logging
import requests
from typing import Dict, Optional
from confluent_kafka import Producer

from config import AlertConfig

logger = logging.getLogger(__name__)


class DLQHandler:
    """Handles routing invalid records to the Dead Letter Queue."""

    def __init__(self, producer: Producer, dlq_topic: str, alert_config: AlertConfig):
        self.producer = producer
        self.dlq_topic = dlq_topic
        self.alert_config = alert_config

        # Alert tracking
        self.failure_count = 0
        self.window_start = time.time()
        self.alert_sent = False

    def send_to_dlq(
        self,
        original_topic: str,
        key: Optional[bytes],
        value: Dict,
        errors: list,
        warnings: list,
    ):
        """Send an invalid record to the DLQ with error metadata."""
        dlq_record = {
            "original_topic": original_topic,
            "original_key": key.decode("utf-8") if key else None,
            "original_value": value,
            "errors": errors,
            "warnings": warnings,
            "error_count": len(errors),
            "timestamp": int(time.time() * 1000),
            "dlq_reason": "data_quality_validation_failed",
        }

        self.producer.produce(
            topic=self.dlq_topic,
            key=key,
            value=json.dumps(dlq_record).encode("utf-8"),
            callback=self._delivery_callback,
        )
        self.producer.flush()

        logger.warning(
            f"Record sent to DLQ | topic={original_topic} | key={key} | errors={len(errors)}"
        )

        # Track failures for alerting
        self._track_failure(original_topic, errors)

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"DLQ delivery failed: {err}")
        else:
            logger.debug(f"DLQ delivery succeeded: {msg.topic()} [{msg.partition()}]")

    def _track_failure(self, topic: str, errors: list):
        """Track failures and fire alerts if threshold exceeded."""
        now = time.time()

        # Reset window if expired
        if now - self.window_start > self.alert_config.alert_window_seconds:
            self.failure_count = 0
            self.window_start = now
            self.alert_sent = False

        self.failure_count += 1

        if self.failure_count >= self.alert_config.alert_threshold and not self.alert_sent:
            self._fire_alert(topic, errors)
            self.alert_sent = True

    def _fire_alert(self, topic: str, sample_errors: list):
        """Send alert to Slack."""
        message = {
            "text": (
                f"🚨 *Data Quality Alert*\n"
                f"• Topic: `{topic}`\n"
                f"• Failures: {self.failure_count} in "
                f"{self.alert_config.alert_window_seconds}s window\n"
                f"• Threshold: {self.alert_config.alert_threshold}\n"
                f"• DLQ Topic: `{self.dlq_topic}`\n"
                f"• Sample Errors: ```{json.dumps(sample_errors[:3], indent=2)}```"
            )
        }

        logger.critical(
            f"ALERT: {self.failure_count} failures in {self.alert_config.alert_window_seconds}s | topic={topic}"
        )

        if self.alert_config.slack_webhook_url:
            try:
                requests.post(
                    self.alert_config.slack_webhook_url,
                    json=message,
                    timeout=5,
                )
            except Exception as e:
                logger.error(f"Failed to send Slack alert: {e}")