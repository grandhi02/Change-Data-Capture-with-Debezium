# consumer.py

import json
import time
import logging
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaError

from config import AppConfig
from validators import validate_record
from dlq_handler import DLQHandler
from metrics import (
    records_processed,
    records_valid,
    records_invalid,
    records_dlq,
    processing_latency,
    QualityTracker,
)

logger = logging.getLogger(__name__)


class CDCQualityConsumer:
    """Consumes Debezium CDC events, validates, and routes to downstream or DLQ."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.running = False

        # Kafka consumer
        self.consumer = Consumer({
            "bootstrap.servers": config.kafka.bootstrap_servers,
            "group.id": config.kafka.consumer_group,
            "auto.offset.reset": config.kafka.auto_offset_reset,
            "enable.auto.commit": config.kafka.enable_auto_commit,
        })

        # Kafka producer (for downstream + DLQ)
        self.producer = Producer({
            "bootstrap.servers": config.kafka.bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
        })

        # DLQ handler
        self.dlq_handler = DLQHandler(
            producer=self.producer,
            dlq_topic=config.topics.dlq_topic,
            alert_config=config.alerts,
        )

        # Quality tracker
        self.quality_tracker = QualityTracker(window_size=1000)

    def start(self):
        """Subscribe and start consuming."""
        self.consumer.subscribe(self.config.topics.source_topics)
        self.running = True

        logger.info(f"Subscribed to topics: {self.config.topics.source_topics}")
        logger.info(f"DLQ topic: {self.config.topics.dlq_topic}")
        logger.info("Consumer started. Waiting for messages...")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    self._handle_consumer_error(msg)
                    continue

                self._process_message(msg)

        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self.stop()

    def stop(self):
        """Graceful shutdown."""
        self.running = False
        self.consumer.close()
        self.producer.flush()
        logger.info("Consumer stopped.")

    def _process_message(self, msg):
        """Process a single Debezium CDC message."""
        topic = msg.topic()
        key = msg.key()
        start_time = time.time()

        try:
            # Deserialize
            value = json.loads(msg.value().decode("utf-8"))

            # Extract operation type for metrics
            op = value.get("op", "unknown")
            records_processed.labels(topic=topic, operation=op).inc()

            # Tombstone events (Debezium sends null value for deletes sometimes)
            if value is None:
                self._commit(msg)
                return

            # ─── VALIDATE ───
            result = validate_record(topic, value)

            # Track quality
            self.quality_tracker.record(topic, result.is_valid)

            if result.is_valid:
                # ─── SEND TO DOWNSTREAM ───
                self._send_downstream(topic, key, value)
                records_valid.labels(topic=topic).inc()

                if result.warnings:
                    logger.warning(
                        f"Valid with warnings | topic={topic} | key={result.record_key} | "
                        f"warnings={result.warnings}"
                    )
            else:
                # ─── SEND TO DLQ ───
                self.dlq_handler.send_to_dlq(
                    original_topic=topic,
                    key=key,
                    value=value,
                    errors=result.errors,
                    warnings=result.warnings,
                )
                records_dlq.labels(topic=topic).inc()

                for error in result.errors:
                    records_invalid.labels(
                        topic=topic, check_name=error["check"]
                    ).inc()

                logger.error(
                    f"Invalid record → DLQ | topic={topic} | key={result.record_key} | "
                    f"errors={result.errors}"
                )

            # Commit offset after processing
            self._commit(msg)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize message: {e}")
            # Send unparseable messages to DLQ
            self.dlq_handler.send_to_dlq(
                original_topic=topic,
                key=key,
                value={"raw": msg.value().decode("utf-8", errors="replace")},
                errors=[{"check": "deserialization", "message": str(e)}],
                warnings=[],
            )
            self._commit(msg)

        except Exception as e:
            logger.exception(f"Unexpected error processing message: {e}")

        finally:
            duration = time.time() - start_time
            processing_latency.labels(topic=topic).observe(duration)

    def _send_downstream(self, source_topic: str, key: Optional[bytes], value: dict):
        """Send validated record to downstream topic."""
        downstream_topic = self.config.topics.downstream_topics.get(source_topic)

        if not downstream_topic:
            logger.warning(f"No downstream topic configured for {source_topic}")
            return

        self.producer.produce(
            topic=downstream_topic,
            key=key,
            value=json.dumps(value).encode("utf-8"),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)  # Trigger callbacks

    def _commit(self, msg):
        """Commit offset."""
        self.consumer.commit(message=msg, asynchronous=False)

    def _handle_consumer_error(self, msg):
        """Handle Kafka consumer errors."""
        if msg.error().code() == KafkaError._PARTITION_EOF:
            logger.debug(f"End of partition: {msg.topic()} [{msg.partition()}]")
        else:
            logger.error(f"Consumer error: {msg.error()}")

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error(f"Downstream delivery failed: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
            )
