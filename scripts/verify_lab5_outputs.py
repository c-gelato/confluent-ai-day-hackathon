#!/usr/bin/env python3
"""Verify that Lab 5 governed output streams are receiving records."""

import argparse
import json
import logging
import sys
import time
from typing import Any

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    SCHEMA_REGISTRY_AVAILABLE = True
except ImportError:
    SCHEMA_REGISTRY_AVAILABLE = False

from .common.cloud_detection import auto_detect_cloud_provider, suggest_cloud_provider
from .common.logging_utils import setup_logging
from .common.terraform import extract_kafka_credentials, get_project_root


OUTPUT_TOPICS = ["brand_incident_alerts", "brand_response_actions"]


class Lab5OutputVerifier:
    def __init__(
        self,
        bootstrap_servers: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        schema_registry_url: str,
        schema_registry_api_key: str,
        schema_registry_api_secret: str,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": kafka_api_key,
            "sasl.password": kafka_api_secret,
            "group.id": f"verify-lab5-outputs-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.schema_registry_client = None
        if SCHEMA_REGISTRY_AVAILABLE:
            self.schema_registry_client = SchemaRegistryClient(
                {
                    "url": schema_registry_url,
                    "basic.auth.user.info": (
                        f"{schema_registry_api_key}:{schema_registry_api_secret}"
                    ),
                }
            )

    def _decode_value(self, topic: str, value: bytes | None) -> Any:
        if value is None:
            return None

        if self.schema_registry_client and value and value[0] == 0:
            try:
                deserializer = AvroDeserializer(self.schema_registry_client)
                return deserializer(
                    value, SerializationContext(topic, MessageField.VALUE)
                )
            except Exception:
                pass

        try:
            return json.loads(value.decode("utf-8"))
        except Exception:
            return value.decode("utf-8", errors="replace")

    def read_topic(self, topic: str, max_messages: int, timeout_seconds: int) -> list[Any]:
        consumer = Consumer(self.consumer_config)
        messages: list[Any] = []
        deadline = time.time() + timeout_seconds

        try:
            consumer.subscribe([topic])
            while len(messages) < max_messages and time.time() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                messages.append(self._decode_value(topic, msg.value()))
        finally:
            consumer.close()

        return messages


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Lab 5 output topics by consuming sample records",
    )
    parser.add_argument(
        "cloud_provider",
        nargs="?",
        choices=["aws", "azure"],
        help="Target cloud provider (auto-detected if not specified)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Maximum seconds to wait for each topic (default: 30)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=3,
        help="Maximum number of sample messages to print per topic (default: 3)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error(
            "confluent-kafka not available. Run: uv pip install confluent-kafka"
        )
        return 1

    cloud_provider = args.cloud_provider or auto_detect_cloud_provider()
    if not cloud_provider:
        cloud_provider = suggest_cloud_provider()
    if not cloud_provider:
        logger.error("Could not auto-detect cloud provider")
        return 1

    try:
        project_root = get_project_root()
        credentials = extract_kafka_credentials(cloud_provider, project_root)
    except Exception as exc:
        logger.error("Failed to extract credentials: %s", exc)
        return 1

    verifier = Lab5OutputVerifier(
        bootstrap_servers=credentials["bootstrap_servers"],
        kafka_api_key=credentials["kafka_api_key"],
        kafka_api_secret=credentials["kafka_api_secret"],
        schema_registry_url=credentials["schema_registry_url"],
        schema_registry_api_key=credentials["schema_registry_api_key"],
        schema_registry_api_secret=credentials["schema_registry_api_secret"],
    )

    exit_code = 0
    for topic in OUTPUT_TOPICS:
        messages = verifier.read_topic(topic, args.max_messages, args.timeout)
        if not messages:
            logger.error("No records observed in %s within %ss", topic, args.timeout)
            exit_code = 1
            continue

        logger.info("Observed %s sample record(s) in %s", len(messages), topic)
        for index, message in enumerate(messages, start=1):
            print(f"{topic}[{index}]={json.dumps(message, default=str)}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())