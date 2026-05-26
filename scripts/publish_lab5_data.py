#!/usr/bin/env python3
"""Publish local Lab5 multi-stream brand incident demo data with Schema Registry-backed Avro schemas."""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    from confluent_kafka import Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec
    from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .common.cloud_detection import auto_detect_cloud_provider, suggest_cloud_provider
from .common.logging_utils import setup_logging
from .common.terraform import extract_kafka_credentials, get_project_root, validate_terraform_state


TOPIC_SCHEMAS = {
    "brand_mentions": json.dumps(
        {
            "type": "record",
            "name": "brand_mentions_value",
            "namespace": "org.apache.flink.avro.generated.record",
            "fields": [
                {"name": "mention_id", "type": "string"},
                {"name": "brand", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "source_type", "type": "string"},
                {"name": "channel", "type": "string"},
                {"name": "author_handle", "type": ["null", "string"], "default": None},
                {"name": "headline", "type": ["null", "string"], "default": None},
                {"name": "body", "type": "string"},
                {"name": "url", "type": ["null", "string"], "default": None},
                {"name": "priority_hint", "type": ["null", "string"], "default": None},
                {"name": "sentiment_score", "type": "double"},
                {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ],
        }
    ),
    "support_cases": json.dumps(
        {
            "type": "record",
            "name": "support_cases_value",
            "namespace": "org.apache.flink.avro.generated.record",
            "fields": [
                {"name": "case_id", "type": "string"},
                {"name": "brand", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "customer_tier", "type": ["null", "string"], "default": None},
                {"name": "case_channel", "type": ["null", "string"], "default": None},
                {"name": "priority", "type": ["null", "string"], "default": None},
                {"name": "issue_category", "type": ["null", "string"], "default": None},
                {"name": "issue_summary", "type": ["null", "string"], "default": None},
                {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ],
        }
    ),
    "product_release_events": json.dumps(
        {
            "type": "record",
            "name": "product_release_events_value",
            "namespace": "org.apache.flink.avro.generated.record",
            "fields": [
                {"name": "release_id", "type": "string"},
                {"name": "brand", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "release_type", "type": "string"},
                {"name": "release_version", "type": ["null", "string"], "default": None},
                {"name": "rollout_percent", "type": ["null", "int"], "default": None},
                {"name": "initiated_by", "type": ["null", "string"], "default": None},
                {"name": "change_summary", "type": ["null", "string"], "default": None},
                {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ],
        }
    ),
}

TOPIC_FILES = {
    "product_release_events": "product_release_events.jsonl",
    "support_cases": "support_cases.jsonl",
    "brand_mentions": "brand_mentions.jsonl",
}

KEY_FIELDS = {
    "brand_mentions": "mention_id",
    "support_cases": "case_id",
    "product_release_events": "release_id",
}

WATERMARK_ADVANCE_MS = 6 * 60 * 1000


def parse_event_ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


class Lab5DataPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        schema_registry_url: str,
        schema_registry_api_key: str,
        schema_registry_api_secret: str,
        dry_run: bool = False,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.dry_run = dry_run
        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": kafka_api_key,
            "sasl.password": kafka_api_secret,
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
        }

        sr_client = SchemaRegistryClient(
            {
                "url": schema_registry_url,
                "basic.auth.user.info": f"{schema_registry_api_key}:{schema_registry_api_secret}",
            }
        )
        self.key_serializer = StringSerializer("utf_8")
        self.value_serializers = {
            topic: AvroSerializer(sr_client, schema)
            for topic, schema in TOPIC_SCHEMAS.items()
        }
        self.producer = None if dry_run else Producer(self.producer_config)

    def purge_topic(self, topic: str) -> None:
        admin = AdminClient(self.producer_config)
        metadata = admin.list_topics(topic=topic, timeout=10)
        if topic not in metadata.topics:
            self.logger.info("Topic '%s' not found yet; skipping purge", topic)
            return

        partitions = [TopicPartition(topic, partition_id) for partition_id in metadata.topics[topic].partitions]
        delete_offsets = {}
        offsets = admin.list_offsets({tp: OffsetSpec.latest() for tp in partitions})
        for topic_partition, future in offsets.items():
            result = future.result()
            if result.offset > 0:
                delete_offsets[topic_partition] = TopicPartition(
                    topic_partition.topic, topic_partition.partition, result.offset
                )

        if delete_offsets:
            for future in admin.delete_records(list(delete_offsets.values())).values():
                future.result()
            self.logger.info("Purged existing records from %s", topic)

    def publish_records(self, topic: str, records: List[Dict[str, Any]]) -> int:
        if self.dry_run:
            self.logger.info("Dry run: would publish %s records to %s", len(records), topic)
            return len(records)

        assert self.producer is not None
        self.purge_topic(topic)
        value_serializer = self.value_serializers[topic]
        key_field = KEY_FIELDS[topic]

        published = 0
        for record in records:
            key_bytes = self.key_serializer(
                record[key_field], SerializationContext(topic, MessageField.KEY)
            )
            value_bytes = value_serializer(
                record, SerializationContext(topic, MessageField.VALUE)
            )
            self.producer.produce(topic=topic, key=key_bytes, value=value_bytes)
            published += 1

        self.producer.flush()
        return published


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append_watermark_records(topic_records: Dict[str, List[Dict[str, Any]]], watermark_ts: int) -> None:
    topic_records["brand_mentions"].append(
        {
            "mention_id": "wm-brand-mentions",
            "brand": "WatermarkCo",
            "product": "Verifier",
            "region": "internal",
            "source_type": "system",
            "channel": "heartbeat",
            "author_handle": None,
            "headline": None,
            "body": "Watermark advancement event for Lab 5 demo windows.",
            "url": None,
            "priority_hint": "system",
            "sentiment_score": 0.0,
            "event_ts": watermark_ts,
        }
    )
    topic_records["support_cases"].append(
        {
            "case_id": "wm-support-cases",
            "brand": "WatermarkCo",
            "product": "Verifier",
            "region": "internal",
            "customer_tier": "internal",
            "case_channel": "system",
            "priority": "low",
            "issue_category": "watermark",
            "issue_summary": "Watermark advancement event for Lab 5 demo windows.",
            "event_ts": watermark_ts,
        }
    )
    topic_records["product_release_events"].append(
        {
            "release_id": "wm-product-release-events",
            "brand": "WatermarkCo",
            "product": "Verifier",
            "region": "internal",
            "release_type": "watermark-advance",
            "release_version": "1.0.0",
            "rollout_percent": 0,
            "initiated_by": "system",
            "change_summary": "Watermark advancement event for Lab 5 demo windows.",
            "event_ts": watermark_ts,
        }
    )


def load_records_by_topic(data_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    topic_records = {
        topic: _load_jsonl(data_dir / file_name)
        for topic, file_name in TOPIC_FILES.items()
    }
    max_ts = max(
        parse_event_ts(record["event_ts"])
        for records in topic_records.values()
        for record in records
    )
    aligned_now = int(time.time() * 1000)
    offset_ms = aligned_now - max_ts + 15_000
    watermark_ts = max_ts + offset_ms + WATERMARK_ADVANCE_MS

    rebased: Dict[str, List[Dict[str, Any]]] = {}
    for topic, records in topic_records.items():
        rebased_records = []
        for record in records:
            adjusted = dict(record)
            adjusted["event_ts"] = parse_event_ts(record["event_ts"]) + offset_ms
            if "sentiment_score" in adjusted:
                adjusted["sentiment_score"] = float(adjusted["sentiment_score"])
            if "rollout_percent" in adjusted and adjusted["rollout_percent"] is not None:
                adjusted["rollout_percent"] = int(adjusted["rollout_percent"])
            rebased_records.append(adjusted)
        rebased[topic] = rebased_records

    _append_watermark_records(rebased, watermark_ts)
    return rebased


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="publish_lab5_data",
        description="Publish Lab5 multi-stream brand incident data to Kafka",
    )
    parser.add_argument(
        "cloud_provider",
        nargs="?",
        choices=["aws", "azure"],
        help="Target cloud provider (auto-detected if not specified)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("assets/lab5/data"),
        help="Input directory containing Lab5 JSONL topic files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate setup without publishing records",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed logs",
    )
    return parser


def main() -> None:
    args = create_argument_parser().parse_args()
    logger = setup_logging(args.verbose)

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error("confluent-kafka is not installed. Run `uv sync` first.")
        sys.exit(1)

    project_root = get_project_root()
    cloud_provider = args.cloud_provider or auto_detect_cloud_provider()
    if not cloud_provider:
        logger.error("Could not auto-detect cloud provider")
        suggest_cloud_provider(project_root)
        sys.exit(1)

    if not validate_terraform_state(cloud_provider, project_root):
        logger.error("Terraform state validation failed for %s", cloud_provider)
        logger.error(
            "Please deploy terraform/core/ and terraform/lab5-brand-sentiment-response/ first"
        )
        sys.exit(1)

    data_dir = args.data_dir
    if not data_dir.is_absolute():
        data_dir = project_root / data_dir
    if not data_dir.exists():
        logger.error("Data directory not found: %s", data_dir)
        sys.exit(1)
    missing_files = [file_name for file_name in TOPIC_FILES.values() if not (data_dir / file_name).exists()]
    if missing_files:
        logger.error("Missing required Lab5 data files: %s", ", ".join(missing_files))
        sys.exit(1)

    credentials = extract_kafka_credentials(cloud_provider, project_root)
    records_by_topic = load_records_by_topic(data_dir)

    publisher = Lab5DataPublisher(
        bootstrap_servers=credentials["bootstrap_servers"],
        kafka_api_key=credentials["kafka_api_key"],
        kafka_api_secret=credentials["kafka_api_secret"],
        schema_registry_url=credentials["schema_registry_url"],
        schema_registry_api_key=credentials["schema_registry_api_key"],
        schema_registry_api_secret=credentials["schema_registry_api_secret"],
        dry_run=args.dry_run,
    )

    publish_order = ["product_release_events", "support_cases", "brand_mentions"]
    publish_counts = {
        topic: publisher.publish_records(topic, records_by_topic[topic])
        for topic in publish_order
    }

    latest_ts = max(
        record["event_ts"]
        for records in records_by_topic.values()
        for record in records
    )
    latest_iso = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc).isoformat()
    for topic in publish_order:
        logger.info("Published %s records to %s", publish_counts[topic], topic)
    logger.info("Latest event timestamp after rebasing: %s", latest_iso)


if __name__ == "__main__":
    main()
